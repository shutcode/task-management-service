package generated_handler

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	gorm_models "task-management-service/internal/task-manager/db"
	pb_kafka "task-management-service/pkg/pb/kafka_messages_pb"
	pb_task "task-management-service/pkg/pb/task_pb"
	"task-management-service/pkg/validation"
)

type KafkaProducerInterface interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
	Stats() kafka.WriterStats
}

type TaskServiceImpl struct {
	DB       *gorm.DB
	Producer KafkaProducerInterface
}

func NewTaskService(db *gorm.DB, producer KafkaProducerInterface) *TaskServiceImpl {
	return &TaskServiceImpl{DB: db, Producer: producer}
}

func toProtoTask(dbTask *gorm_models.Task) *pb_task.Task {
	if dbTask == nil { return nil }
	return &pb_task.Task{
		Id: uint32(dbTask.ID), TaskTemplateId: uint32(dbTask.TaskTemplateID), Name: dbTask.Name,
		Description: dbTask.Description, Params: dbTask.Params, Status: dbTask.Status,
		Result: dbTask.Result, CreatedAt: timestamppb.New(dbTask.CreatedAt), UpdatedAt: timestamppb.New(dbTask.UpdatedAt),
	}
}

func (s *TaskServiceImpl) CreateTask(ctx context.Context, c *app.RequestContext, req *pb_task.CreateTaskRequest) (*pb_task.CreateTaskResponse, error) {
	log.Printf("TaskServiceImpl: CreateTask called with Name: %s, TemplateID: %d", req.Name, req.TaskTemplateId)
	var template gorm_models.TaskTemplate
	if err := s.DB.First(&template, req.TaskTemplateId).Error; err != nil {
		return nil, fmt.Errorf("task template ID %d not found: %w", req.TaskTemplateId, err)
	}
	if template.ParamSchema != "" {
		if err := validation.ValidateJSONWithSchema(template.ParamSchema, req.Params); err != nil {
			return nil, fmt.Errorf("task parameters do not match template schema: %w", err)
		}
	}
	dbTask := gorm_models.Task{
		TaskTemplateID: uint(req.TaskTemplateId), Name: req.Name, Description: req.Description,
		Params: req.Params, Status: "PENDING",
	}

	tx := s.DB.Begin() // Start a transaction
	if tx.Error != nil {
		log.Printf("TaskServiceImpl: Error starting transaction: %v", tx.Error)
		return nil, tx.Error
	}

	if result := tx.Create(&dbTask); result.Error != nil {
		tx.Rollback()
		log.Printf("TaskServiceImpl: Error creating task in DB: %v", result.Error)
		return nil, result.Error
	}

	dispatchPayloadProto := &pb_kafka.KafkaTaskDispatch{
		TaskId:         uint32(dbTask.ID), TaskTemplateId: uint32(dbTask.TaskTemplateID), Name: dbTask.Name,
		Params: dbTask.Params, ExecutorType: template.ExecutorType, ParamSchema: template.ParamSchema,
	}
	payloadBytes, err := proto.Marshal(dispatchPayloadProto)
	if err != nil {
		tx.Rollback()
		log.Printf("TaskServiceImpl: Error marshalling KafkaTaskDispatch (Protobuf) for task ID %d: %v", dbTask.ID, err)
		return nil, fmt.Errorf("failed to marshal kafka payload: %w", err)
	}

	kafkaMsg := kafka.Message{ Key: []byte(strconv.FormatUint(uint64(dbTask.ID), 10)), Value: payloadBytes }
	kafkaWriteCtx, kafkaCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer kafkaCancel()

	if err := s.Producer.WriteMessages(kafkaWriteCtx, kafkaMsg); err != nil {
		tx.Rollback()
		log.Printf("TaskServiceImpl: Error sending Protobuf task ID %d to Kafka: %v", dbTask.ID, err)
		// It's important to decide if this error should also fail the HTTP request.
		// For now, we'll make it fail the request.
		return nil, fmt.Errorf("failed to dispatch task to kafka: %w", err)
	}

	if err := tx.Commit().Error; err != nil {
		log.Printf("TaskServiceImpl: Error committing transaction: %v", err)
		// Kafka message was sent, but DB commit failed. This is a problematic state (distributed transaction).
		// Needs a compensation strategy (e.g., log for manual intervention, or try to retract Kafka message if possible).
		return nil, fmt.Errorf("failed to commit task creation: %w", err)
	}

	log.Printf("TaskServiceImpl: Protobuf Task ID %d dispatched to Kafka topic %s", dbTask.ID, s.Producer.Stats().Topic)
	return &pb_task.CreateTaskResponse{Task: toProtoTask(&dbTask)}, nil
}

// ... (GetTask, ListTasks, UpdateTask, DeleteTask as before) ...
func (s *TaskServiceImpl) GetTask(ctx context.Context, c *app.RequestContext, req *pb_task.GetTaskRequest) (*pb_task.GetTaskResponse, error) {
	idStr := c.Param("id"); id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil { return nil, fmt.Errorf("invalid ID format in path: %w", err) }
	log.Printf("TaskServiceImpl: GetTask called with ID %d", id)
	var dbTask gorm_models.Task
	if dbErr := s.DB.First(&dbTask, uint(id)).Error; dbErr != nil { return nil, dbErr }
	return &pb_task.GetTaskResponse{Task: toProtoTask(&dbTask)}, nil
}

func (s *TaskServiceImpl) ListTasks(ctx context.Context, c *app.RequestContext, req *pb_task.ListTasksRequest) (*pb_task.ListTasksResponse, error) {
	log.Printf("TaskServiceImpl: ListTasks called with filters: Status='%s', TemplateID=%d", req.Status, req.TemplateId)
	var dbTasks []gorm_models.Task; query := s.DB.Model(&gorm_models.Task{})
	if req.Status != "" { query = query.Where("status = ?", req.Status) }
	if req.TemplateId > 0 { query = query.Where("task_template_id = ?", req.TemplateId) }
	if err := query.Find(&dbTasks).Error; err != nil { return nil, err }
	respTasks := make([]*pb_task.Task, len(dbTasks))
	for i, dbT := range dbTasks { respTasks[i] = toProtoTask(&dbT) }
	return &pb_task.ListTasksResponse{Tasks: respTasks}, nil
}

func (s *TaskServiceImpl) UpdateTask(ctx context.Context, c *app.RequestContext, req *pb_task.UpdateTaskRequest) (*pb_task.UpdateTaskResponse, error) {
	idStr := c.Param("id"); id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil { return nil, fmt.Errorf("invalid ID format in path: %w", err) }
	if req.Id != 0 && req.Id != uint32(id) { return nil, fmt.Errorf("path ID %d mismatches body ID %d", id, req.Id) }
	req.Id = uint32(id); log.Printf("TaskServiceImpl: UpdateTask called for ID %d", req.Id)
	var dbTask gorm_models.Task
	if dbErr := s.DB.First(&dbTask, req.Id).Error; dbErr != nil { return nil, dbErr }
	updateData := make(map[string]interface{})
	if req.Status != nil { updateData["status"] = req.GetStatus() }
	if req.Result != nil { updateData["result"] = req.GetResult() }
	if len(updateData) == 0 { return &pb_task.UpdateTaskResponse{Task: toProtoTask(&dbTask)}, nil }
	if result := s.DB.Model(&dbTask).Updates(updateData); result.Error != nil { return nil, result.Error }
	s.DB.First(&dbTask, req.Id); return &pb_task.UpdateTaskResponse{Task: toProtoTask(&dbTask)}, nil
}

func (s *TaskServiceImpl) DeleteTask(ctx context.Context, c *app.RequestContext, req *pb_task.DeleteTaskRequest) (*emptypb.Empty, error) {
	idStr := c.Param("id"); id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil { return nil, fmt.Errorf("invalid ID format in path: %w", err) }
	log.Printf("TaskServiceImpl: DeleteTask called for ID %d", id)
	deleteOpResult := s.DB.Delete(&gorm_models.Task{}, uint(id))
	if deleteOpResult.Error != nil { return nil, deleteOpResult.Error }
	if deleteOpResult.RowsAffected == 0 { return nil, gorm.ErrRecordNotFound }
	return &emptypb.Empty{}, nil
}
