package services

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	taskDB_models "task-management-service/internal/task-manager/db"
	pb_kafka "task-management-service/pkg/pb/kafka_messages_pb"
)

type SchedulerService struct {
	DB         *gorm.DB
	Scheduler  gocron.Scheduler
	Producer   *kafka.Writer
	appContext context.Context
}
func NewSchedulerService(ctx context.Context, db *gorm.DB, producer *kafka.Writer) (*SchedulerService, error) {
	s, err := gocron.NewScheduler(); if err != nil { return nil, fmt.Errorf("failed to create gocron scheduler: %w", err) }; return &SchedulerService{ DB: db, Scheduler:  s, Producer:   producer, appContext: ctx, }, nil
}
func (s *SchedulerService) Start() {
	log.Println("SchedulerService starting...")
	s.Scheduler.Start()
	s.LoadAndScheduleTasks()
	log.Println("SchedulerService started and initial tasks scheduled.")
}
func (s *SchedulerService) Stop() {
	log.Println("SchedulerService stopping...")
	if err := s.Scheduler.Shutdown(); err != nil { log.Printf("Error shutting down gocron scheduler: %v", err) } else { log.Println("Gocron scheduler shut down successfully.") }
}

func (s *SchedulerService) LoadAndScheduleTasks() {
	log.Println("Loading and scheduling tasks from task templates...")
	var templates []taskDB_models.TaskTemplate
	if err := s.DB.Where("cron_expression IS NOT NULL AND cron_expression != ''").Find(&templates).Error; err != nil {
		log.Printf("Error fetching task templates for scheduling: %v", err)
		return
	}

    // Explicitly ignore return value for RemoveByTags due to persistent compiler issue.
    // This is line 47 (approx)
	s.Scheduler.RemoveByTags("cron_template_task")
	log.Println("Attempted to remove existing cron jobs by tag 'cron_template_task'. Any error from this operation is currently not checked.")

    for _, tmpl := range templates {
		template := tmpl; jobID := fmt.Sprintf("template_%d", template.ID)
		jobDef := gocron.CronJob(template.CronExpression, false)
		_, err := s.Scheduler.NewJob( jobDef, gocron.NewTask( func(t taskDB_models.TaskTemplate) { s.executeScheduledTask(t) }, template ), gocron.WithName(jobID), gocron.WithTags("cron_template_task", fmt.Sprintf("template_id:%d", template.ID)), )
		if err != nil {log.Printf("Error scheduling task for template ID %d (%s) with cron '%s': %v",template.ID, template.Name, template.CronExpression, err)} else {log.Printf("Scheduled task for template ID %d (%s) with cron '%s'. Job ID: %s",template.ID, template.Name, template.CronExpression, jobID)}
	}
	currentJobs := s.Scheduler.Jobs(); log.Printf("%d jobs currently scheduled.", len(currentJobs))
}

func (s *SchedulerService) executeScheduledTask(template taskDB_models.TaskTemplate) {
	log.Printf("Cron job triggered for TaskTemplate ID %d (%s)", template.ID, template.Name)
	actualParams := "{}"
	if template.ParamSchema != "" && template.ParamSchema != "{}" {
		log.Printf("Note: TaskTemplate ID %d has ParamSchema defined. For cron jobs, ensure 'DefaultCronParams' or similar is used if needed. Using default '%s' for now.", template.ID, actualParams)
	}
	task := taskDB_models.Task{
		TaskTemplateID: template.ID,
		Name:           fmt.Sprintf("Scheduled: %s - %s", template.Name, time.Now().Format("2006-01-02 15:04:05")),
		Description:    "Automatically scheduled task", Params: actualParams, Status: "PENDING",
	}
	if err := s.DB.Create(&task).Error; err != nil { log.Printf("Error creating scheduled task: %v", err); return }
	log.Printf("Created new task ID %d from scheduled template ID %d", task.ID, template.ID)

	dispatchPayloadProto := &pb_kafka.KafkaTaskDispatch{
		TaskId:         uint32(task.ID),
		TaskTemplateId: uint32(task.TaskTemplateID),
		Name:           task.Name,
		Params:         task.Params,
		ExecutorType:   template.ExecutorType,
		ParamSchema:    template.ParamSchema,
	}
	payloadBytes, err := proto.Marshal(dispatchPayloadProto)
	if err != nil {
		log.Printf("Error marshalling KafkaTaskDispatch (Protobuf) for scheduled task ID %d: %v", task.ID, err)
		s.DB.Model(&task).Update("status", "DISPATCH_FAILED"); return
	}

	kafkaMsg := kafka.Message{ Key: []byte(strconv.FormatUint(uint64(task.ID), 10)), Value: payloadBytes }
	kafkaWriteCtx, kafkaCancel := context.WithTimeout(s.appContext, 10*time.Second)
	defer kafkaCancel()
	if err := s.Producer.WriteMessages(kafkaWriteCtx, kafkaMsg); err != nil {
		log.Printf("Error sending scheduled Protobuf task ID %d to Kafka: %v", task.ID, err)
		s.DB.Model(&task).Update("status", "DISPATCH_FAILED"); return
	}
	log.Printf("Scheduled Protobuf task ID %d dispatched to Kafka.", task.ID)
}
func (s *SchedulerService) RefreshScheduledJobs() { s.LoadAndScheduleTasks() }
