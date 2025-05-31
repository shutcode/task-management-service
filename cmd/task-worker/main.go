package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"task-management-service/internal/task-worker/executors"
	"task-management-service/internal/task-worker/validation"
)

const (
	DefaultKafkaBrokers = "localhost:9092"
	DefaultTaskTopic    = "task_execution_requests"
	DefaultGroupID      = "task-worker-group"
	DefaultResultTopic  = "task_results"
)
type TaskResultPayload struct {
	TaskID uint   `json:"task_id"`
	Status string `json:"status"`
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}
func main() {
	log.Println("Starting Task Worker Service...")
	executors.RegisterExecutor("echo-executor", &executors.EchoExecutor{})
	kafkaBrokers := os.Getenv("KAFKA_BROKERS"); if kafkaBrokers == "" { kafkaBrokers = DefaultKafkaBrokers }
	taskTopic := os.Getenv("TASK_TOPIC"); if taskTopic == "" { taskTopic = DefaultTaskTopic }
	groupID := os.Getenv("GROUP_ID"); if groupID == "" { groupID = DefaultGroupID }
	resultTopic := os.Getenv("RESULT_TOPIC"); if resultTopic == "" { resultTopic = DefaultResultTopic }
	brokerList := strings.Split(kafkaBrokers, ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokerList, GroupID: groupID, Topic: taskTopic,
		MinBytes: 10e3, MaxBytes: 10e6, CommitInterval: time.Second, MaxWait: 3 * time.Second,
	})
	defer reader.Close()
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokerList, Topic: resultTopic, Balancer: &kafka.LeastBytes{},
		RequiredAcks: int(kafka.RequireOne), // Cast to int as last resort
		Async: false,
	})
	defer producer.Close()
	log.Printf("Task Worker Kafka consumer configured for brokers: %s, topic: %s, groupID: %s", kafkaBrokers, taskTopic, groupID)
    log.Printf("Task Worker Kafka producer configured for results topic: %s", resultTopic)
	signals := make(chan os.Signal, 1); signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background());
	go func() { sig := <-signals; log.Printf("Task Worker: Shutdown signal received (%s). Cancelling context...", sig); cancel() }()
	log.Println("Task Worker listening for messages...")
	for {
		select {
		case <-ctx.Done(): log.Println("Task Worker: Context cancelled. Exiting message loop."); return
		default:
			readCtx, readLoopCancel := context.WithTimeout(ctx, 1*time.Second)
			m, err := reader.ReadMessage(readCtx); readLoopCancel()
			if err == context.DeadlineExceeded { continue }
			if err == context.Canceled { log.Println("Task Worker: Read context cancelled, likely due to shutdown."); continue }
			if err == io.EOF { log.Println("Task Worker: Kafka reader closed (EOF). Exiting."); return }
            if err != nil { log.Printf("Task Worker: Kafka read error: %v. Retrying...", err); time.Sleep(1 * time.Second); continue }
			log.Printf("Task Worker: Received message: Topic %s, Partition %d, Offset %d", m.Topic, m.Partition, m.Offset)
			var taskPayload executors.TaskPayload
			if err := json.Unmarshal(m.Value, &taskPayload); err != nil {
				log.Printf("Task Worker: Unmarshal error for task payload: %v. Value: %s", err, string(m.Value)); continue
			}
			if taskPayload.ParamSchema != "" {
				err_val := validation.ValidateJSONWithSchema(taskPayload.ParamSchema, taskPayload.Params)
				if err_val != nil {
					log.Printf("Task Worker: Task params validation failed for task ID %d: %v. Params: %s",
						taskPayload.TaskID, err_val, taskPayload.Params)
					sendTaskResult(ctx, producer, TaskResultPayload{ TaskID: taskPayload.TaskID, Status: "FAILED", Error:  fmt.Sprintf("Parameter validation failed in worker: %s", err_val.Error()), }); continue
				}
				log.Printf("Task Worker: Params validated successfully for task ID %d.", taskPayload.TaskID)
			} else {
				log.Printf("Task Worker: No ParamSchema provided for task ID %d. Skipping params validation.", taskPayload.TaskID)
			}
			executor, err_exec := executors.GetExecutor(taskPayload.ExecutorType)
			if err_exec != nil {
				log.Printf("Task Worker: Error getting executor for type '%s': %v. Task ID: %d", taskPayload.ExecutorType, err_exec, taskPayload.TaskID)
				sendTaskResult(ctx, producer, TaskResultPayload{ TaskID: taskPayload.TaskID, Status: "FAILED", Error: fmt.Sprintf("Executor type '%s' not found", taskPayload.ExecutorType), }); continue
			}
			go func(p executors.TaskPayload) {
				res, execErr := executor.Execute(p)
				trp := TaskResultPayload{TaskID: p.TaskID}
				if execErr != nil {
					log.Printf("Task Worker: Error executing task ID %d: %v", p.TaskID, execErr)
					trp.Status = "FAILED"; trp.Error = execErr.Error()
				} else {
					log.Printf("Task Worker: Task ID %d completed successfully. Result: %s", p.TaskID, res)
					trp.Status = "COMPLETED"; trp.Result = res
				}
				sendTaskResult(context.Background(), producer, trp)
			}(taskPayload)
		}
	}
}

func sendTaskResult(ctx context.Context, producer *kafka.Writer, resultPayload TaskResultPayload) {
	payloadBytes, err := json.Marshal(resultPayload)
	if err != nil {
		log.Printf("Task Worker: Error marshalling result payload for task ID %d: %v", resultPayload.TaskID, err)
		return
	}
	msg := kafka.Message{ Key: []byte(fmt.Sprintf("%d", resultPayload.TaskID)), Value: payloadBytes }

	writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := producer.WriteMessages(writeCtx, msg); err != nil {
		if writeCtx.Err() == context.Canceled {
             log.Printf("Task Worker: Write context cancelled sending result for task ID %d: %v", resultPayload.TaskID, err)
        } else if ctx.Err() == context.Canceled {
             log.Printf("Task Worker: App context cancelled, could not send result for task ID %d.", resultPayload.TaskID)
        } else {
             log.Printf("Task Worker: Error sending result for task ID %d to Kafka: %v", resultPayload.TaskID, err)
        }
	} else {
		log.Printf("Task Worker: Sent result for task ID %d to Kafka topic %s", resultPayload.TaskID, producer.Stats().Topic)
	}
}
