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

// SchedulerInterface defines the methods our HTTP handler needs from the SchedulerService.
// This allows for decoupling and easier testing.
type SchedulerInterface interface {
	ScheduleOrUpdateTask(task *gorm_models.Task) error
}

type TaskServiceImpl struct {
	DB        *gorm.DB
	Producer  KafkaProducerInterface
	Scheduler SchedulerInterface // Use an interface for the scheduler
}

func NewTaskService(db *gorm.DB, producer KafkaProducerInterface, scheduler SchedulerInterface) *TaskServiceImpl {
	return &TaskServiceImpl{DB: db, Producer: producer, Scheduler: scheduler}
}

// Helper function to convert *timestamppb.Timestamp to *time.Time
func protoTimestampToTimePtr(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil || !ts.IsValid() {
		return nil
	}
	t := ts.AsTime()
	return &t
}

// Helper function to convert *time.Time to *timestamppb.Timestamp
func timePtrToProtoTimestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func toProtoTask(dbTask *gorm_models.Task) *pb_task.Task {
	if dbTask == nil {
		return nil
	}
	return &pb_task.Task{
		Id:             uint32(dbTask.ID),
		TaskTemplateId: uint32(dbTask.TaskTemplateID),
		Name:           dbTask.Name,
		Description:    dbTask.Description,
		Params:         dbTask.Params,
		Status:         dbTask.Status,
		Result:         dbTask.Result,
		CreatedAt:      timestamppb.New(dbTask.CreatedAt),
		UpdatedAt:      timestamppb.New(dbTask.UpdatedAt),
		RunAt:          timePtrToProtoTimestamp(dbTask.RunAt),
	}
}

func (s *TaskServiceImpl) CreateTask(ctx context.Context, c *app.RequestContext, req *pb_task.CreateTaskRequest) (*pb_task.CreateTaskResponse, error) {
	log.Printf("TaskServiceImpl: CreateTask called with Name: %s, TemplateID: %d, RunAt: %v", req.Name, req.TaskTemplateId, req.RunAt)
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
		TaskTemplateID: uint(req.TaskTemplateId),
		Name:           req.Name,
		Description:    req.Description,
		Params:         req.Params,
		// Status and RunAt will be set below
	}

	isScheduledOnce := false
	if req.RunAt != nil && req.RunAt.IsValid() && req.RunAt.AsTime().After(time.Now()) {
		dbTask.Status = "SCHEDULED_ONCE"
		dbTask.RunAt = protoTimestampToTimePtr(req.RunAt)
		isScheduledOnce = true
		log.Printf("TaskServiceImpl: Task %s for template ID %d will be scheduled for %v", req.Name, req.TaskTemplateId, dbTask.RunAt)
	} else {
		dbTask.Status = "PENDING"
		if req.RunAt != nil {
			log.Printf("TaskServiceImpl: Task %s for template ID %d RunAt is nil, invalid, or in the past (%v). Scheduling for immediate dispatch.", req.Name, req.TaskTemplateId, req.RunAt)
		}
	}

	tx := s.DB.Begin() // Start a transaction
	if tx.Error != nil {
		log.Printf("TaskServiceImpl: Error starting transaction: %v", tx.Error)
		return nil, fmt.Errorf("failed to start transaction: %w", tx.Error)
	}

	// Save task to DB
	if result := tx.Create(&dbTask); result.Error != nil {
		tx.Rollback()
		log.Printf("TaskServiceImpl: Error creating task in DB: %v", result.Error)
		return nil, fmt.Errorf("failed to create task in db: %w", result.Error)
	}

	if isScheduledOnce {
		// Task is scheduled for later execution.
		if err := s.Scheduler.ScheduleOrUpdateTask(&dbTask); err != nil {
			tx.Rollback() // Rollback DB transaction if scheduling fails
			log.Printf("TaskServiceImpl: Error scheduling one-time task ID %d: %v", dbTask.ID, err)
			return nil, fmt.Errorf("failed to schedule task for later execution: %w", err)
		}
		log.Printf("TaskServiceImpl: Task ID %d (%s) successfully scheduled with status 'SCHEDULED_ONCE' for RunAt %v. Kafka dispatch deferred.", dbTask.ID, dbTask.Name, dbTask.RunAt)
	} else {
		// Task is for immediate dispatch.
		dispatchPayloadProto := &pb_kafka.KafkaTaskDispatch{
			TaskId:         uint32(dbTask.ID),
			TaskTemplateId: uint32(dbTask.TaskTemplateID),
			Name:           dbTask.Name,
			Params:         dbTask.Params,
			ExecutorType:   template.ExecutorType,
			ParamSchema:    template.ParamSchema,
		}
		payloadBytes, err := proto.Marshal(dispatchPayloadProto)
		if err != nil {
			tx.Rollback()
			log.Printf("TaskServiceImpl: Error marshalling KafkaTaskDispatch (Protobuf) for task ID %d: %v", dbTask.ID, err)
			return nil, fmt.Errorf("failed to marshal kafka payload: %w", err)
		}

		kafkaMsg := kafka.Message{Key: []byte(strconv.FormatUint(uint64(dbTask.ID), 10)), Value: payloadBytes}
		kafkaWriteCtx, kafkaCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer kafkaCancel()

		if err := s.Producer.WriteMessages(kafkaWriteCtx, kafkaMsg); err != nil {
			tx.Rollback()
			log.Printf("TaskServiceImpl: Error sending Protobuf task ID %d to Kafka: %v", dbTask.ID, err)
			return nil, fmt.Errorf("failed to dispatch task to kafka: %w", err)
		}
		log.Printf("TaskServiceImpl: Protobuf Task ID %d dispatched to Kafka topic %s", dbTask.ID, s.Producer.Stats().Topic)
	}

	if err := tx.Commit().Error; err != nil {
		log.Printf("TaskServiceImpl: Error committing transaction: %v", err)
		// If Kafka message was sent (for non-scheduled tasks), but DB commit failed, this is a problematic state.
		// Needs a compensation strategy. For scheduled tasks, it's less critical if Kafka wasn't involved yet.
		return nil, fmt.Errorf("failed to commit task creation: %w", err)
	}

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
