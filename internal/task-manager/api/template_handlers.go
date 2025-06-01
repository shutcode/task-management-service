package api

import (
	"context"
	"log"
	"net/http"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"gorm.io/gorm"

	taskDB "task-management-service/internal/task-manager/db"
)

type TaskTemplateHandler struct {
	DB *gorm.DB
}

func NewTaskTemplateHandler(db *gorm.DB) *TaskTemplateHandler {
	return &TaskTemplateHandler{DB: db}
}

type CreateTaskTemplateRequest struct {
	Name           string `json:"name" validate:"required,gt=0"` // Changed to validate and added gt=0
	Description    string `json:"description"`
	TaskType       string `json:"task_type" validate:"required,gt=0"` // Changed to validate and added gt=0
	ParamSchema    string `json:"param_schema" validate:"required,gt=0"` // Changed to validate and added gt=0; schemas are JSON strings.
	ResultSchema   string `json:"result_schema" validate:"required,gt=0"` // Changed to validate and added gt=0
	ExecutorType   string `json:"executor_type" validate:"required,gt=0"` // Changed to validate and added gt=0
	CronExpression string `json:"cron_expression,omitempty"`
}

func (h *TaskTemplateHandler) CreateTaskTemplate(ctx context.Context, c *app.RequestContext) {
	var req CreateTaskTemplateRequest

	if err := c.Bind(&req); err != nil {
		log.Printf("CreateTaskTemplate: Bind failed: %v", err)
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request format: " + err.Error()})
		return
	}
	if err := c.Validate(&req); err != nil {
		log.Printf("Validation error for CreateTaskTemplateRequest: %v, Request: %+v", err, req)
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request payload: " + err.Error()})
		return
	}

	tmpl := taskDB.TaskTemplate{
		Name:           req.Name,
		Description:    req.Description,
		TaskType:       req.TaskType,
		ParamSchema:    req.ParamSchema,
		ResultSchema:   req.ResultSchema,
		ExecutorType:   req.ExecutorType,
		CronExpression: req.CronExpression,
	}
	if result := h.DB.Create(&tmpl); result.Error != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to create task template: " + result.Error.Error()})
		return
	}

	if tmpl.CronExpression != "" {
		log.Printf("TaskTemplate ID %d created with CronExpression '%s'. Scheduler refresh might be needed.", tmpl.ID, tmpl.CronExpression)
	}
	c.JSON(http.StatusCreated, tmpl)
}

func (h *TaskTemplateHandler) GetTaskTemplates(ctx context.Context, c *app.RequestContext) {
	var templates []taskDB.TaskTemplate
	if result := h.DB.Find(&templates); result.Error != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to fetch task templates: " + result.Error.Error()})
		return
	}
	c.JSON(http.StatusOK, templates)
}

func (h *TaskTemplateHandler) GetTaskTemplateByID(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid ID format"})
		return
	}
	var tmpl taskDB.TaskTemplate
	if result := h.DB.First(&tmpl, uint(id)); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, utils.H{"error": "Task template not found"})
		} else {
			c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to fetch task template: " + result.Error.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, tmpl)
}

func (h *TaskTemplateHandler) DeleteTaskTemplate(ctx context.Context, c *app.RequestContext) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid ID format"})
		return
	}
	var tmpl taskDB.TaskTemplate
	if findResult := h.DB.First(&tmpl, uint(id)); findResult.Error != nil {
		if findResult.Error == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, utils.H{"error": "Task template not found to delete"})
		} else {
			c.JSON(http.StatusInternalServerError, utils.H{"error": "Error finding template to delete: " + findResult.Error.Error()})
		}
		return
	}

	if result := h.DB.Delete(&taskDB.TaskTemplate{}, uint(id)); result.Error != nil {
		c.JSON(http.StatusInternalServerError, utils.H{"error": "Failed to delete task template: " + result.Error.Error()})
		return
	}
	log.Printf("TaskTemplate ID %d deleted. Associated cron job should be unscheduled. Scheduler refresh might be needed.", id)
	c.JSON(http.StatusOK, utils.H{"message": "Task template deleted successfully. Manual scheduler refresh may be needed."})
}
