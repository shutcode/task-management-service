package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io" // Import io for io.EOF
	"log"
	"os"
	"strings"
	"task-management-service/internal/task-manager/db"
	"task-management-service/internal/task-manager/events"
	"task-management-service/pkg/validation"
	"time"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

const (
	DefaultKafkaBrokers    = "localhost:9092"
	DefaultResultTopic     = "task_results"
	DefaultResultGroupID   = "task-manager-results-group"
)

type ResultService struct {
	DB     *gorm.DB
	Reader *kafka.Reader
}

func NewResultService(gormDB *gorm.DB) *ResultService {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS"); if kafkaBrokers == "" { kafkaBrokers = DefaultKafkaBrokers }
	resultTopic := os.Getenv("RESULT_TOPIC"); if resultTopic == "" { resultTopic = DefaultResultTopic }
	groupID := os.Getenv("RESULT_GROUP_ID"); if groupID == "" { groupID = DefaultResultGroupID }
	brokerList := strings.Split(kafkaBrokers, ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokerList, GroupID: groupID, Topic: resultTopic,
		MinBytes: 10e3, MaxBytes: 10e6, CommitInterval: time.Second, MaxWait: 3 * time.Second,
	})
	log.Printf("Task Manager Kafka consumer for results configured for topic: %s, groupID: %s", resultTopic, groupID)
	return &ResultService{DB: gormDB, Reader: reader}
}

func (s *ResultService) StartConsuming(ctx context.Context) {
	log.Println("ResultService starting to consume task completion events...")
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("ResultService: context cancelled, stopping consumer.")
				return
			default:
				readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				msg, err := s.Reader.ReadMessage(readCtx)
				cancel()

				if err == context.DeadlineExceeded { continue }
				if err == context.Canceled { log.Println("ResultService: Read context cancelled."); return }
				if err == io.EOF { log.Println("ResultService: Kafka reader closed (EOF), stopping consumption."); return }
				if err != nil {
					log.Printf("ResultService: error reading message: %v", err)
					time.Sleep(1 * time.Second); continue
				}
				log.Printf("ResultService: received message on topic %s, partition %d, offset %d: %s", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
				var payload events.TaskCompletionPayload
				if err := json.Unmarshal(msg.Value, &payload); err != nil {
					log.Printf("ResultService: error unmarshalling task completion payload: %v. Value: %s", err, string(msg.Value))
					continue
				}
				var taskToUpdate db.Task
				if res := s.DB.First(&taskToUpdate, payload.TaskID); res.Error != nil {
					log.Printf("ResultService: task with ID %d not found for update: %v", payload.TaskID, res.Error); continue
				}
				var template db.TaskTemplate
				if res := s.DB.First(&template, taskToUpdate.TaskTemplateID); res.Error != nil {
					log.Printf("ResultService: TaskTemplate with ID %d not found for task ID %d: %v. Result validation may be skipped.",
						taskToUpdate.TaskTemplateID, payload.TaskID, res.Error)
				}
				finalStatus := payload.Status; finalResult := payload.Result; originalWorkerError := payload.Error
				if template.ID != 0 && template.ResultSchema != "" && payload.Status == "COMPLETED" {
					err_val := validation.ValidateJSONWithSchema(template.ResultSchema, payload.Result)
					if err_val != nil {
						finalStatus = "RESULT_VALIDATION_FAILED"
						finalResult = fmt.Sprintf("Original Result: '%s'. Validation Error: %s", payload.Result, err_val.Error())
						originalWorkerError = ""
					}
				} else if payload.Status == "FAILED" {
					finalResult = fmt.Sprintf("Worker Error: '%s'. Original Result: '%s'", originalWorkerError, payload.Result)
				}
				updateData := map[string]interface{}{ "status": finalStatus, "result": finalResult }
				if res := s.DB.Model(&taskToUpdate).Updates(updateData); res.Error != nil {
					log.Printf("ResultService: failed to update task ID %d: %v", payload.TaskID, res.Error); continue
				}
				log.Printf("ResultService: successfully updated task ID %d with status %s", payload.TaskID, finalStatus)
			}
		}
	}()
}
func (s *ResultService) Close() { if s.Reader != nil { log.Println("ResultService: Closing Kafka reader."); s.Reader.Close() } }
