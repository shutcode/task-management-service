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

	// Attempt to remove existing cron jobs by tag.
	log.Println("Attempting to remove existing cron jobs by tag 'cron_template_task'...")
	s.Scheduler.RemoveByTags("cron_template_task") // Assuming no error return or ignoring it as per env behavior
	log.Println("Completed removal attempt for cron jobs by tag 'cron_template_task'.")

	for _, tmpl := range templates {
		template := tmpl
		jobID := fmt.Sprintf("template_%d", template.ID) // This is for gocron's WithName, not the primary ID for logging.
		jobDef := gocron.CronJob(template.CronExpression, false)
		job, err := s.Scheduler.NewJob(
			jobDef,
			gocron.NewTask(
				func(t taskDB_models.TaskTemplate) { s.executeScheduledTask(t) },
				template,
			),
			gocron.WithName(jobID),
			gocron.WithTags("cron_template_task", fmt.Sprintf("template_id:%d", template.ID)),
		)
		if err != nil {
			log.Printf("Error scheduling task for template ID %d (%s) with cron '%s': %v", template.ID, template.Name, template.CronExpression, err)
		} else {
			nextRunTime, errNextRun := job.NextRun()
			logMessage := fmt.Sprintf("Scheduled task for template ID %d (%s) with cron '%s'. gocron Job ID: %s, Tags: %v",
				template.ID, template.Name, template.CronExpression, job.ID(), job.Tags())
			if errNextRun != nil {
				logMessage += fmt.Sprintf(", Next Run: (error: %v)", errNextRun)
			} else {
				logMessage += fmt.Sprintf(", Next Run: %s", nextRunTime.Format(time.RFC3339))
			}
			log.Println(logMessage)
		}
	}
	currentJobs := s.Scheduler.Jobs()
	log.Printf("%d jobs currently scheduled.", len(currentJobs))
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

// ExecuteOneTimeTask is called by gocron when a one-time scheduled task is due.
// It's exported for testing purposes.
func (s *SchedulerService) ExecuteOneTimeTask(taskID uint) {
	log.Printf("Executing one-time task ID: %d", taskID)

	var task taskDB_models.Task
	if err := s.DB.First(&task, taskID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			log.Printf("Error executing one-time task: Task ID %d not found.", taskID)
		} else {
			log.Printf("Error fetching task ID %d for one-time execution: %v", taskID, err)
		}
		return
	}

	if task.Status != "SCHEDULED_ONCE" {
		log.Printf("Warning: One-time task ID %d was expected to have status 'SCHEDULED_ONCE' but found '%s'. Aborting execution.", taskID, task.Status)
		return
	}

	// Update status to DISPATCHING
	// Using a map for updates to ensure UpdatedAt is also set correctly by GORM or manually if needed.
	// GORM's default behavior for Updates with a struct only updates non-zero fields.
	updateData := map[string]interface{}{"status": "DISPATCHING", "updated_at": time.Now()}
	if err := s.DB.Model(&task).Updates(updateData).Error; err != nil {
		log.Printf("Error updating task ID %d status to DISPATCHING: %v. Task will not be dispatched.", taskID, err)
		return
	}
	log.Printf("Task ID %d status updated to DISPATCHING.", taskID)

	var template taskDB_models.TaskTemplate
	if err := s.DB.First(&template, task.TaskTemplateID).Error; err != nil {
		log.Printf("Error fetching template ID %d for task ID %d: %v", task.TaskTemplateID, taskID, err)
		s.DB.Model(&task).Updates(map[string]interface{}{
			"status": "DISPATCH_FAILED",
			"result": fmt.Sprintf("Failed to fetch template: %v", err),
			"updated_at": time.Now(),
		})
		return
	}

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
		log.Printf("Error marshalling KafkaTaskDispatch for one-time task ID %d: %v", task.ID, err)
		s.DB.Model(&task).Updates(map[string]interface{}{
			"status": "DISPATCH_FAILED",
			"result": fmt.Sprintf("Failed to marshal Kafka payload: %v", err),
			"updated_at": time.Now(),
		})
		return
	}

	kafkaMsg := kafka.Message{Key: []byte(strconv.FormatUint(uint64(task.ID), 10)), Value: payloadBytes}
	kafkaWriteCtx, kafkaCancel := context.WithTimeout(s.appContext, 10*time.Second)
	defer kafkaCancel()

	if err := s.Producer.WriteMessages(kafkaWriteCtx, kafkaMsg); err != nil {
		log.Printf("Error sending one-time Protobuf task ID %d to Kafka: %v", task.ID, err)
		s.DB.Model(&task).Updates(map[string]interface{}{
			"status": "DISPATCH_FAILED",
			"result": fmt.Sprintf("Failed to dispatch to Kafka: %v", err),
			"updated_at": time.Now(),
		})
		return
	}

	log.Printf("One-time Protobuf task ID %d dispatched to Kafka.", task.ID)
	// Update status to PENDING, as it's now in the worker queue
	if err := s.DB.Model(&task).Updates(map[string]interface{}{"status": "PENDING", "updated_at": time.Now()}).Error; err != nil {
		log.Printf("Error updating task ID %d status to PENDING after Kafka dispatch: %v", taskID, err)
		// This is not ideal, as the task is in Kafka but DB status is DISPATCHING. Requires monitoring.
	} else {
		log.Printf("Task ID %d status updated to PENDING after successful Kafka dispatch.", taskID)
	}
}

// ScheduleOrUpdateTask schedules a task that has already been created in the DB
// (typically with Status: "SCHEDULED_ONCE" and a future RunAt time).
func (s *SchedulerService) ScheduleOrUpdateTask(task *taskDB_models.Task) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}
	if task.RunAt == nil {
		return fmt.Errorf("task ID %d has no RunAt time, cannot schedule", task.ID)
	}
	if task.RunAt.Before(time.Now()) {
		return fmt.Errorf("task ID %d RunAt time %v is in the past, cannot schedule", task.ID, task.RunAt)
	}

	taskTag := fmt.Sprintf("task_id:%d", task.ID)
	jobName := fmt.Sprintf("onetime_task_%d", task.ID)

	// Attempt to remove any existing job for this task_id to handle updates
	log.Printf("Attempting to remove existing job with tag '%s' for task ID %d...", taskTag, task.ID)
	s.Scheduler.RemoveByTags(taskTag) // Assuming no error return or ignoring it as per env behavior
	log.Printf("Completed removal attempt for job with tag '%s' for task ID %d.", taskTag, task.ID)

	// Using the specific API call as instructed for one-time job definition
	jobDef := gocron.OneTimeJob(gocron.OneTimeJobStartDateTime(task.RunAt.UTC()))
	log.Printf("Defined OneTimeJob for task ID %d with RunAt: %v", task.ID, task.RunAt.UTC())

	gocronJob, err := s.Scheduler.NewJob(
		jobDef,
		gocron.NewTask(s.ExecuteOneTimeTask, task.ID), // Pass task.ID to the execution function
		gocron.WithName(jobName),
		gocron.WithTags("onetime_task", taskTag),
	)

	if err != nil {
		log.Printf("Error scheduling one-time task for ID %d at %v: %v", task.ID, task.RunAt, err)
		return fmt.Errorf("failed to schedule one-time task ID %d: %w", task.ID, err)
	}

	nextRunTime, errNextRun := gocronJob.NextRun()
	logMessage := fmt.Sprintf("Successfully scheduled one-time task for ID %d (%s) at %v. gocron Job ID: %s, Tags: %v",
		task.ID, task.Name, task.RunAt, gocronJob.ID(), gocronJob.Tags())
	if errNextRun != nil {
		logMessage += fmt.Sprintf(", Next Run: (error: %v)", errNextRun)
	} else {
		logMessage += fmt.Sprintf(", Next Run: %s", nextRunTime.Format(time.RFC3339))
	}
	log.Println(logMessage)

	return nil
}

func (s *SchedulerService) RefreshScheduledJobs() { s.LoadAndScheduleTasks() }
