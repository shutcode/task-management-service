package executors

import (
	"fmt"
	"log"
)

// TaskPayload is the structure passed to an executor.
// It mirrors pb_kafka.KafkaTaskDispatch.
type TaskPayload struct {
	TaskID         uint32 // Changed from uint to match pb_kafka.KafkaTaskDispatch
	TaskTemplateID uint32 // Changed from uint to match pb_kafka.KafkaTaskDispatch
	Name           string
	Params         string
	ExecutorType   string
	ParamSchema    string
}

type Executor interface {
	Execute(taskPayload TaskPayload) (result string, err error)
}

var Registry = make(map[string]Executor)

func RegisterExecutor(executorType string, executor Executor) {
	log.Printf("Registering executor for type: %s", executorType)
	Registry[executorType] = executor
}

func GetExecutor(executorType string) (Executor, error) {
	executor, exists := Registry[executorType]
	if !exists {
		return nil, fmt.Errorf("no executor registered for type: %s", executorType)
	}
	return executor, nil
}
