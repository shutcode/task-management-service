package executors

import (
	"fmt"
	"log"
)

// TaskPayload is the expected structure of a message from Kafka
type TaskPayload struct {
	TaskID         uint   `json:"task_id"`
	TaskTemplateID uint   `json:"task_template_id"`
	Name           string `json:"name"`
	Params         string `json:"params"`
	ExecutorType   string `json:"executor_type"`
	ParamSchema    string `json:"param_schema,omitempty"` // New field for the schema itself
}

// Executor defines the interface for a task executor.
type Executor interface {
	Execute(taskPayload TaskPayload) (result string, err error)
}

// Registry holds registered executors.
var Registry = make(map[string]Executor)

// RegisterExecutor adds an executor to the registry.
func RegisterExecutor(executorType string, executor Executor) {
	log.Printf("Registering executor for type: %s", executorType)
	Registry[executorType] = executor
}

// GetExecutor retrieves an executor from the registry.
func GetExecutor(executorType string) (Executor, error) {
	executor, exists := Registry[executorType]
	if !exists {
		return nil, fmt.Errorf("no executor registered for type: %s", executorType)
	}
	return executor, nil
}
