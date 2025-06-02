package services

import (
	"context"
	// "encoding/json" // Replaced by proto unmarshal
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto" // Added for protobuf
	"gorm.io/gorm"

	"task-management-service/internal/task-manager/db"
	// "task-management-service/internal/task-manager/events" // Old JSON events
	pb_kafka "task-management-service/pkg/pb/kafka_messages_pb" // New Kafka Protobuf messages
	"task-management-service/pkg/validation"
)

const (
	DefaultKafkaBrokers    = "localhost:9092"
	DefaultResultTopic     = "task_results"
	DefaultResultGroupID   = "task-manager-results-group"
)
type ResultService struct { DB *gorm.DB; Reader *kafka.Reader; }
func NewResultService(gormDB *gorm.DB) *ResultService {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS"); if kafkaBrokers == "" { kafkaBrokers = DefaultKafkaBrokers }
	resultTopic := os.Getenv("RESULT_TOPIC"); if resultTopic == "" { resultTopic = DefaultResultTopic }
	groupID := os.Getenv("RESULT_GROUP_ID"); if groupID == "" { groupID = DefaultResultGroupID }
	brokerList := strings.Split(kafkaBrokers, ",");
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:brokerList, GroupID:groupID, Topic:resultTopic,
		MinBytes:10e3, MaxBytes:10e6, CommitInterval:time.Second, MaxWait:3*time.Second,
	})
	log.Printf("Task Manager Kafka consumer for results configured for topic: %s, groupID: %s", resultTopic, groupID)
	return &ResultService{DB:gormDB, Reader:reader}
}


func (s *ResultService) StartConsuming(ctx context.Context) {
	log.Println("ResultService starting to consume task completion events (Protobuf)...")
	go func() {
		for {
			select {
			case <-ctx.Done(): log.Println("ResultService: ctx cancelled, stopping."); return // Removed s.Reader.Close() here, handled by main defer
			default:
			}

			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := s.Reader.ReadMessage(readCtx)
			cancel()

			if err == context.DeadlineExceeded { continue }
			if err == context.Canceled { log.Println("ResultService: Read context cancelled during ReadMessage."); return }
			if err == io.EOF { log.Println("ResultService: Kafka reader closed (EOF), stopping consumption."); return }
			if err != nil {
				log.Printf("ResultService: Kafka read error: %v", err); time.Sleep(1 * time.Second); continue
			}

			log.Printf("ResultService: received message on topic %s (expecting Protobuf)", msg.Topic)

			var payload pb_kafka.KafkaTaskCompletion // Use new Protobuf type
			if err_unmarshal := proto.Unmarshal(msg.Value, &payload); err_unmarshal != nil { // Use proto.Unmarshal
				log.Printf("ResultService: error unmarshalling KafkaTaskCompletion (Protobuf): %v. Value: %x", err_unmarshal, msg.Value)
				continue
			}

			var taskToUpdate db.Task
			if res := s.DB.First(&taskToUpdate, payload.TaskId); res.Error != nil { // Use payload.TaskId
				log.Printf("ResultService: task ID %d not found: %v", payload.TaskId, res.Error)
				continue
			}

			var template db.TaskTemplate
			if res := s.DB.First(&template, taskToUpdate.TaskTemplateID); res.Error != nil {
				log.Printf("ResultService: template ID %d not found for task %d: %v", taskToUpdate.TaskTemplateID, payload.TaskId, res.Error)
			}

			finalStatus := payload.Status
			finalResult := payload.Result
			validationErrorStr := ""

			if template.ID != 0 && template.ResultSchema != "" && payload.Status == "COMPLETED" {
				if err_val := validation.ValidateJSONWithSchema(template.ResultSchema, payload.Result); err_val != nil {
					log.Printf("ResultService: Task result (Protobuf payload, JSON result field) validation failed for task ID %d: %v", payload.TaskId, err_val)
					finalStatus = "RESULT_VALIDATION_FAILED"
					validationErrorStr = fmt.Sprintf("Result schema validation failed: %s", err_val.Error())
					// Keep original result from worker, append validation error to it.
					finalResult = fmt.Sprintf("Original Result: %s. Validation Error: %s", payload.Result, validationErrorStr)
				} else {
					log.Printf("ResultService: Task result (Protobuf payload, JSON result field) validated for task ID %d.", payload.TaskId)
				}
			}

			updateData := map[string]interface{}{"status": finalStatus, "result": finalResult}
			// If worker reported FAILED, use ErrorMessage from proto payload
			if payload.Status == "FAILED" && validationErrorStr == "" { // Only if no new validation error occurred
				updateData["result"] = fmt.Sprintf("Worker Error: %s. Original Result Data: %s", payload.ErrorMessage, payload.Result)
			}

			if res := s.DB.Model(&taskToUpdate).Updates(updateData); res.Error != nil {
				log.Printf("ResultService: failed to update task ID %d: %v", payload.TaskId, res.Error)
				continue
			}
			log.Printf("ResultService: updated task ID %d with status %s (Protobuf flow)", payload.TaskId, finalStatus)
		}
	}()
}
func (s *ResultService) Close() { if s.Reader != nil { log.Println("ResultService: Closing Kafka reader."); s.Reader.Close() } }
