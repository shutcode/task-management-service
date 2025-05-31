package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"task-management-service/internal/task-manager/events"
	"task-management-service/pkg/validation"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"

	taskDB "task-management-service/internal/task-manager/db"
)

type TaskHandler struct {
	DB       *gorm.DB
	Producer *kafka.Writer
}

func NewTaskHandler(db *gorm.DB, producer *kafka.Writer) *TaskHandler {
	return &TaskHandler{DB: db, Producer: producer}
}

type CreateTaskRequest struct {
	TaskTemplateID uint   `json:"task_template_id" validate:"required"`
	Name           string `json:"name" validate:"required"`
	Description    string `json:"description"`
	Params         string `json:"params" validate:"required"`
}
type UpdateTaskRequest struct { // Assuming no validation needed for partial updates, or add specific rules
	Status *string `json:"status"`
	Result *string `json:"result"`
}

func (h *TaskHandler) CreateTask(ctx context.Context, c *app.RequestContext) {
	var req CreateTaskRequest
	if err := c.BindAndValidate(&req); err != nil {
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request payload: " + err.Error()})
		return
	}

	var template taskDB.TaskTemplate
	if err := h.DB.First(&template, req.TaskTemplateID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusBadRequest, utils.H{"error": "Task template not found"})
		} else {
			c.JSON(http.StatusInternalServerError, utils.H{"error": "Error verifying task template: " + err.Error()})
		}
		return
	}

	if template.ParamSchema != "" {
		err := validation.ValidateJSONWithSchema(template.ParamSchema, req.Params)
		if err != nil {
			log.Printf("Task params validation failed for template ID %d: %v. Params: %s, Schema: %s",
				template.ID, err, req.Params, template.ParamSchema)
			c.JSON(http.StatusBadRequest, utils.H{
				"error":             "Task parameters do not match the template schema.",
				"validation_errors": err.Error(),
			})
			return
		}
		log.Printf("Task params validated successfully against schema for template ID %d.", template.ID)
	} else {
		log.Printf("No ParamSchema defined for template ID %d. Skipping params validation.", template.ID)
	}

	task := taskDB.Task{
		TaskTemplateID: req.TaskTemplateID,
		Name:           req.Name,
		Description:    req.Description,
		Params:         req.Params,
		Status:         "PENDING",
	}

	if result := h.DB.Create(&task); result.Error != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to create task: " + result.Error.Error()})
		return
	}

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
		log.Printf("Error marshalling Kafka TaskDispatchPayload for task ID %d: %v", task.ID, err)
		c.JSON(http.StatusCreated, utils.H{"task": task, "dispatch_warning": "failed to marshal kafka payload"})
		return
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(strconv.FormatUint(uint64(task.ID), 10)),
		Value: payloadBytes,
	}

	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := h.Producer.WriteMessages(writeCtx, kafkaMsg); err != nil {
		log.Printf("Error sending task ID %d to Kafka: %v", task.ID, err)
		c.JSON(http.StatusCreated, utils.H{"task": task, "dispatch_warning": "failed to send message to kafka: " + err.Error()})
		return
	}

	log.Printf("Task ID %d dispatched to Kafka topic %s", task.ID, h.Producer.Stats().Topic)
	c.JSON(http.StatusCreated, task)
}

func (h *TaskHandler) GetTasks(ctx context.Context, c *app.RequestContext) {
	var tasks []taskDB.Task
	query := h.DB.Model(&taskDB.Task{})
	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if templateIDStr := c.Query("template_id"); templateIDStr != "" {
		templateID, err := strconv.ParseUint(templateIDStr, 10, 32)
		if err == nil {
			query = query.Where("task_template_id = ?", uint(templateID))
		} else {
			log.Printf("Invalid template_id query parameter: %s", templateIDStr)
		}
	}
	if result := query.Find(&tasks); result.Error != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to fetch tasks: " + result.Error.Error()})
		return
	}
	c.JSON(http.StatusOK, tasks)
}

func (h *TaskHandler) GetTaskByID(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid ID format"})
		return
	}
	var task taskDB.Task
	if result := h.DB.First(&task, uint(id)); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, utils.H{"error": "Task not found"})
		} else {
			c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to fetch task: " + result.Error.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, task)
}

func (h *TaskHandler) UpdateTask(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid ID format"})
		return
	}
	var req UpdateTaskRequest
	if err := c.BindAndValidate(&req); err != nil { // Note: UpdateTaskRequest has no validate tags currently
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request payload: " + err.Error()})
		return
	}
	var task taskDB.Task
	if result := h.DB.First(&task, uint(id)); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, utils.H{"error": "Task not found"})
		} else {
			c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to find task: " + result.Error.Error()})
		}
		return
	}
	updateData := make(map[string]interface{})
	if req.Status != nil {
		updateData["status"] = *req.Status
	}
	if req.Result != nil {
		updateData["result"] = *req.Result
	}
	if len(updateData) == 0 {
		c.JSON(http.StatusBadRequest, utils.H{"error": "No update fields provided"})
		return
	}
	if result := h.DB.Model(&task).Updates(updateData); result.Error != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to update task: " + result.Error.Error()})
		return
	}
	c.JSON(http.StatusOK, task)
}
