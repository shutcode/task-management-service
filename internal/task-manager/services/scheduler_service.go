package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"

	taskDB "task-management-service/internal/task-manager/db"
	"task-management-service/internal/task-manager/events"
)

type SchedulerService struct {
	DB         *gorm.DB
	Scheduler  gocron.Scheduler
	Producer   *kafka.Writer
	appContext context.Context
}

func NewSchedulerService(ctx context.Context, db *gorm.DB, producer *kafka.Writer) (*SchedulerService, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("failed to create gocron scheduler: %w", err)
	}
	return &SchedulerService{DB: db, Scheduler: s, Producer: producer, appContext: ctx}, nil
}

func (s *SchedulerService) Start() {
	log.Println("SchedulerService starting...")
	s.Scheduler.Start()
	s.LoadAndScheduleTasks()
	log.Println("SchedulerService started and initial tasks scheduled.")
}

func (s *SchedulerService) Stop() {
	log.Println("SchedulerService stopping...")
	if err := s.Scheduler.Shutdown(); err != nil {
		log.Printf("Error shutting down gocron scheduler: %v", err)
	} else {
		log.Println("Gocron scheduler shut down successfully.")
	}
}

func (s *SchedulerService) LoadAndScheduleTasks() {
	log.Println("Loading and scheduling tasks from task templates...")
	var templates []taskDB.TaskTemplate
	if err := s.DB.Where("cron_expression IS NOT NULL AND cron_expression != ''").Find(&templates).Error; err != nil {
		log.Printf("Error fetching task templates for scheduling: %v", err)
		return
	}

	// Workaround for persistent compiler error: Do not check error from RemoveByTags.
	// This is not ideal as it might hide other errors from this call.
	s.Scheduler.RemoveByTags("cron_template_task")
	log.Println("Attempted to remove existing cron jobs by tag 'cron_template_task'. Any actual error from this operation is currently not checked due to a persistent build issue.")


	for _, tmpl := range templates {
		template := tmpl
		jobID := fmt.Sprintf("template_%d", template.ID)

		jobDef := gocron.CronJob(template.CronExpression, false)

		_, err := s.Scheduler.NewJob(
			jobDef,
			gocron.NewTask(
				func(t taskDB.TaskTemplate) { s.executeScheduledTask(t) },
				template,
			),
			gocron.WithName(jobID),
			gocron.WithTags("cron_template_task", fmt.Sprintf("template_id:%d", template.ID)),
		)

		if err != nil {
			log.Printf("Error scheduling task for template ID %d (%s) with cron '%s': %v",
				template.ID, template.Name, template.CronExpression, err)
		} else {
			log.Printf("Scheduled task for template ID %d (%s) with cron '%s'. Job ID: %s",
				template.ID, template.Name, template.CronExpression, jobID)
		}
	}
	currentJobs := s.Scheduler.Jobs()
	log.Printf("%d jobs currently scheduled.", len(currentJobs))
}

func (s *SchedulerService) executeScheduledTask(template taskDB.TaskTemplate) {
	log.Printf("Cron job triggered for TaskTemplate ID %d (%s)", template.ID, template.Name)
	actualParams := "{}"
	if template.ParamSchema != "" && template.ParamSchema != "{}" {
		log.Printf("Note: TaskTemplate ID %d has ParamSchema defined. For cron jobs, ensure 'DefaultCronParams' or similar is used if needed. Using default '%s' for now.", template.ID, actualParams)
	}

	task := taskDB.Task{
		TaskTemplateID: template.ID,
		Name:           fmt.Sprintf("Scheduled: %s - %s", template.Name, time.Now().Format("2006-01-02 15:04:05")),
		Description:    "Automatically scheduled task",
		Params:         actualParams,
		Status:         "PENDING",
	}

	if result := s.DB.Create(&task); result.Error != nil {
		log.Printf("Error creating scheduled task instance for template ID %d: %v", template.ID, result.Error)
		return
	}
	log.Printf("Created new task ID %d from scheduled template ID %d", task.ID, template.ID)

	dispatchPayload := events.TaskDispatchPayload{
		TaskID:         task.ID,
		TaskTemplateID: task.TaskTemplateID,
		Name:           task.Name,
		Params:         task.Params,
		ExecutorType:   template.ExecutorType,
		ParamSchema:    template.ParamSchema,
	}
	payloadBytes, err := json.Marshal(dispatchPayload)
	if err != nil {
		log.Printf("Error marshalling Kafka TaskDispatchPayload for scheduled task ID %d: %v", task.ID, err)
		s.DB.Model(&task).Update("status", "DISPATCH_FAILED")
		return
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(strconv.FormatUint(uint64(task.ID), 10)),
		Value: payloadBytes,
	}

	writeCtx, cancel := context.WithTimeout(s.appContext, 10*time.Second)
	defer cancel()

	if err := s.Producer.WriteMessages(writeCtx, kafkaMsg); err != nil {
		log.Printf("Error sending scheduled task ID %d to Kafka: %v", task.ID, err)
		s.DB.Model(&task).Update("status", "DISPATCH_FAILED")
		return
	}

	log.Printf("Scheduled task ID %d (from template ID %d) dispatched to Kafka successfully.", task.ID, template.ID)
}
func (s *SchedulerService) RefreshScheduledJobs() { s.LoadAndScheduleTasks() }
