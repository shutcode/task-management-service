package executors

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestEchoExecutor_Execute(t *testing.T) {
	executor := EchoExecutor{}
	payload := TaskPayload{
		TaskID:         1,
		TaskTemplateID: 1,
		Name:           "Test Echo Task",
		Params:         `{"message": "hello"}`,
		ExecutorType:   "echo-executor",
		ParamSchema:    "", // Not used by echo executor directly for validation
	}

	result, err := executor.Execute(payload)

	assert.NoError(t, err, "EchoExecutor should not return an error")
	expectedResult := `EchoExecutor processed params: {"message": "hello"}`
	assert.Equal(t, expectedResult, result, "EchoExecutor result mismatch")
}
