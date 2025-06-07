package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"task-management-service/internal/task-worker/executors"
	"task-management-service/internal/task-worker/validation"
	pb_kafka "task-management-service/pkg/pb/kafka_messages_pb"
)

const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultTaskTopic    = "task_execution_requests"
	DefaultGroupID      = "task-worker-group"
	DefaultResultTopic  = "task_results"
)

func main() {
	log.Println("Starting Task Worker Service (Protobuf mode)...")
	// Executors are now registered via init() in the executors package.
	// No need to call RegisterExecutor here explicitly.

	kafkaBrokers := os.Getenv("KAFKA_BROKERS"); if kafkaBrokers == "" { kafkaBrokers = DefaultKafkaBrokers }
	taskTopic := os.Getenv("TASK_TOPIC"); if taskTopic == "" { taskTopic = DefaultTaskTopic }
	groupID := os.Getenv("GROUP_ID"); if groupID == "" { groupID = DefaultGroupID }
	resultTopic := os.Getenv("RESULT_TOPIC"); if resultTopic == "" { resultTopic = DefaultResultTopic }
	brokerList := strings.Split(kafkaBrokers, ",")

	readerCfg := kafka.ReaderConfig{
		Brokers: brokerList, GroupID: groupID, Topic: taskTopic,
		MinBytes: 10e3, MaxBytes: 10e6, CommitInterval: time.Second, MaxWait: 3 * time.Second,
	}
	reader := kafka.NewReader(readerCfg)
	defer reader.Close()

	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokerList, Topic: resultTopic, Balancer: &kafka.LeastBytes{},
		RequiredAcks: int(kafka.RequireOne), // Cast to int
		Async: false,
	})
	defer producer.Close()

	log.Printf("Task Worker (Protobuf) listening on topic '%s', group '%s'. Results to topic '%s'.", taskTopic, groupID, resultTopic)

	signals := make(chan os.Signal, 1); signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background());

	go func() {
		sig := <-signals
		log.Printf("Task Worker: Shutdown signal received (%s). Cancelling context...", sig)
		cancel();
	}()

	for {
		select {
		case <-ctx.Done(): log.Println("Task Worker: Context done, worker shutting down."); return
		default:
		}

		readCtx, readTimeoutCancel := context.WithTimeout(ctx, readerCfg.MaxWait + 1*time.Second)
		m, err := reader.ReadMessage(readCtx)
		readTimeoutCancel()

		if err == context.DeadlineExceeded { continue }
		if err == context.Canceled { log.Println("Task Worker: Read context cancelled during shutdown."); return }
		if err == io.EOF { log.Println("Task Worker: Kafka reader closed (EOF), shutting down worker."); return }
		if err != nil { log.Printf("Task Worker: Kafka read error: %v. Retrying...", err); time.Sleep(1 * time.Second); continue }

		log.Printf("Received message: Topic %s, Partition %d, Offset %d, Key %s (expecting Protobuf)", m.Topic, m.Partition, m.Offset, string(m.Key))

		var dispatchPb pb_kafka.KafkaTaskDispatch
		if err_unmarshal := proto.Unmarshal(m.Value, &dispatchPb); err_unmarshal != nil {
			log.Printf("Error unmarshalling KafkaTaskDispatch (Protobuf): %v. Value: %x", err_unmarshal, m.Value)
			continue
		}

		log.Printf("Processing task (Protobuf): ID %d, Name: %s, Type: %s", dispatchPb.TaskId, dispatchPb.Name, dispatchPb.ExecutorType)

		taskPayloadForExecutor := executors.TaskPayload{
			TaskID:         dispatchPb.TaskId,
			TaskTemplateID: dispatchPb.TaskTemplateId,
			Name:           dispatchPb.Name,
			Params:         dispatchPb.Params,
			ExecutorType:   dispatchPb.ExecutorType,
			ParamSchema:    dispatchPb.ParamSchema,
		}

		if taskPayloadForExecutor.ParamSchema != "" {
			log.Printf("Task Worker: Validating params for task ID %d (Protobuf flow)", taskPayloadForExecutor.TaskID)
			if err_val := validation.ValidateJSONWithSchema(taskPayloadForExecutor.ParamSchema, taskPayloadForExecutor.Params); err_val != nil {
				log.Printf("Task Worker: Task params validation failed for task ID %d: %v", taskPayloadForExecutor.TaskID, err_val)
				sendTaskCompletion(context.Background(), producer, dispatchPb.TaskId, "FAILED", "", fmt.Sprintf("Parameter validation failed in worker: %s", err_val.Error()))
				continue
			}
			log.Printf("Task Worker: Params validated for task ID %d (Protobuf flow)", taskPayloadForExecutor.TaskID)
		}

		executor, err_exec := executors.GetExecutor(taskPayloadForExecutor.ExecutorType)
		if err_exec != nil {
			log.Printf("Error getting executor for type '%s': %v. Task ID: %d", taskPayloadForExecutor.ExecutorType, err_exec, taskPayloadForExecutor.TaskID)
			sendTaskCompletion(context.Background(), producer, taskPayloadForExecutor.TaskID, "FAILED", "", fmt.Sprintf("Executor type '%s' not found", taskPayloadForExecutor.ExecutorType))
			continue
		}

		go func(payload executors.TaskPayload) {
			log.Printf("Executing task %d with %s (Protobuf flow)", payload.TaskID, payload.ExecutorType)
			resultStr, execErr := executor.Execute(payload)

			if execErr != nil {
				log.Printf("Error executing task ID %d: %v", payload.TaskID, execErr)
				sendTaskCompletion(context.Background(), producer, payload.TaskID, "FAILED", "", execErr.Error())
			} else {
				log.Printf("Task ID %d completed successfully. Result: %s", payload.TaskID, resultStr)
				sendTaskCompletion(context.Background(), producer, payload.TaskID, "COMPLETED", resultStr, "")
			}
		}(taskPayloadForExecutor)
	}
}

func sendTaskCompletion(ctx context.Context, producer *kafka.Writer, taskID uint32, status string, result string, errorMessage string) {
	completionPb := &pb_kafka.KafkaTaskCompletion{
		TaskId:       taskID,
		Status:       status,
		Result:       result,
		ErrorMessage: errorMessage,
	}

	payloadBytes, err := proto.Marshal(completionPb)
	if err != nil {
		log.Printf("Error marshalling KafkaTaskCompletion (Protobuf) for task ID %d: %v", taskID, err)
		return
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", taskID)),
		Value: payloadBytes,
	}

	writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.WriteMessages(writeCtx, msg)
	if err != nil {
		if writeCtx.Err() != nil {
			log.Printf("Context error sending Protobuf result for task ID %d to Kafka: %v", taskID, writeCtx.Err())
		} else {
			log.Printf("Error sending Protobuf result for task ID %d to Kafka: %v", taskID, err)
		}
	} else {
		log.Printf("Successfully sent Protobuf result for task ID %d to Kafka topic %s", taskID, producer.Stats().Topic)
	}
}
