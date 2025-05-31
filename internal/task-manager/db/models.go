package db

import (
	// "time" // No longer directly used here due to gorm.Model
	"gorm.io/gorm"
)

// Task represents a task in the system.
type Task struct {
	gorm.Model     // Includes ID, CreatedAt, UpdatedAt, DeletedAt
	TaskTemplateID uint   `json:"task_template_id"` // Foreign key to TaskTemplate
	Name           string `json:"name" gorm:"index"`
	Description    string `json:"description"`
	Params         string `json:"params" gorm:"type:json"` // Store JSON string
	Status         string `json:"status" gorm:"index"`   // e.g., PENDING, RUNNING, COMPLETED, FAILED
	Result         string `json:"result" gorm:"type:json"` // Store JSON string for task result
}

// TaskTemplate represents a template for creating tasks.
type TaskTemplate struct {
	gorm.Model
	Name           string `json:"name" gorm:"uniqueIndex"`
	Description    string `json:"description"`
	TaskType       string `json:"task_type" gorm:"index"` // User-defined type of the task
	ParamSchema    string `json:"param_schema" gorm:"type:json"`   // JSON schema for params
	ResultSchema   string `json:"result_schema" gorm:"type:json"`  // JSON schema for results
	ExecutorType   string `json:"executor_type" gorm:"index"` // Specifies which worker/plugin handles this task type
	CronExpression string `json:"cron_expression,omitempty" gorm:"index;comment:Standard cron expression for scheduling"` // New field
	Tasks          []Task `json:"-"` // Has many relationship: A template can have many tasks
}
