package db

import (
	"time" // Ensure time package is imported

	"gorm.io/gorm"
)

// Task represents a task in the system.
type Task struct {
	gorm.Model            // Includes ID, CreatedAt, UpdatedAt, DeletedAt
	TaskTemplateID uint   `json:"task_template_id"`      // Foreign key to TaskTemplate
	Name           string `json:"name" gorm:"index"`     // Name of the task instance
	Description    string `json:"description"`           // Description of the task instance
	Params         string `json:"params" gorm:"type:json"` // Store JSON string, specific parameters for this task run
	Status         string `json:"status" gorm:"index"`   // e.g., PENDING, SCHEDULED_ONCE, RUNNING, COMPLETED, FAILED
	Result         string `json:"result" gorm:"type:json"` // Store JSON string for task result
	RunAt          *time.Time `gorm:"index"`             // Specific time for one-time scheduled execution
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
