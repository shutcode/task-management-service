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

// ExecutorType constants
const (
	ExecutorTypeEcho   = "echo-executor"
	ExecutorTypePython = "python-executor"
	// Add other executor types here
)

type Executor interface {
	Execute(taskPayload TaskPayload) (result string, err error)
}

var Registry = make(map[string]Executor)

// init function to register all known executors
func init() {
	// Register EchoExecutor (assuming it's defined in this package)
	// If EchoExecutor is simple and has no state, an instance can be directly created.
	// If it's in a different file but same package, it's directly accessible.
	RegisterExecutor(ExecutorTypeEcho, &EchoExecutor{})

	// Register PythonExecutor
	RegisterExecutor(ExecutorTypePython, &PythonExecutor{})

	// Register other executors here
	log.Println("Executor registry initialized with known executors.")
}

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
