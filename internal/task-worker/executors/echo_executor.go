package executors

import (
	"fmt"
	"log"
	"time"
)

// EchoExecutor is a simple executor that logs params and returns them.
type EchoExecutor struct{}

// Execute implements the Executor interface.
func (e *EchoExecutor) Execute(taskPayload TaskPayload) (result string, err error) {
	log.Printf("EchoExecutor: Executing task ID %d (%s) with type '%s'", taskPayload.TaskID, taskPayload.Name, taskPayload.ExecutorType)
	log.Printf("EchoExecutor: Params: %s", taskPayload.Params)

	// Simulate work
	time.Sleep(2 * time.Second)

	result = fmt.Sprintf("EchoExecutor processed params: %s", taskPayload.Params)
	log.Printf("EchoExecutor: Task ID %d completed. Result: %s", taskPayload.TaskID, result)
	return result, nil
}
