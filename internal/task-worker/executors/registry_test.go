package executors

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMain can be used if specific setup for the package is needed,
// but for registry tests, the init() function in registry.go should suffice.

func TestGetExecutor_RegisteredTypes(t *testing.T) {
	// The init() function in registry.go should have registered these.
	testCases := []struct {
		name         string
		executorType string
		expectedType interface{}
		expectError  bool
	}{
		{
			name:         "EchoExecutor",
			executorType: ExecutorTypeEcho, // Assuming this constant is "echo-executor"
			expectedType: &EchoExecutor{},
			expectError:  false,
		},
		{
			name:         "PythonExecutor",
			executorType: ExecutorTypePython, // Assuming this constant is "python-executor"
			expectedType: &PythonExecutor{},
			expectError:  false,
		},
		{
			name:         "UnknownExecutor",
			executorType: "unknown-type-for-testing",
			expectedType: nil,
			expectError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			executor, err := GetExecutor(tc.executorType)

			if tc.expectError {
				assert.Error(t, err, fmt.Sprintf("Expected an error for executor type '%s'", tc.executorType))
				assert.Nil(t, executor, "Executor should be nil on error")
				expectedErrMsg := fmt.Sprintf("no executor registered for type: %s", tc.executorType)
				assert.EqualError(t, err, expectedErrMsg, "Error message mismatch")
			} else {
				assert.NoError(t, err, fmt.Sprintf("Did not expect an error for executor type '%s'", tc.executorType))
				assert.NotNil(t, executor, "Expected a non-nil executor")
				assert.IsType(t, tc.expectedType, executor, fmt.Sprintf("Executor type mismatch for '%s'", tc.executorType))
			}
		})
	}
}

func TestExecutorRegistry_InitialState(t *testing.T) {
	// This test verifies that the global Registry variable is initialized by the init() function.
	// It checks if the known executor types are present.
	assert.NotNil(t, Registry, "Registry should be initialized")

	_, echoExists := Registry[ExecutorTypeEcho]
	assert.True(t, echoExists, fmt.Sprintf("Executor type '%s' should be registered", ExecutorTypeEcho))

	_, pythonExists := Registry[ExecutorTypePython]
	assert.True(t, pythonExists, fmt.Sprintf("Executor type '%s' should be registered", ExecutorTypePython))

	// Check types of registered instances
	if echoExists {
		assert.IsType(t, &EchoExecutor{}, Registry[ExecutorTypeEcho], "Registered EchoExecutor instance type mismatch")
	}
	if pythonExists {
		assert.IsType(t, &PythonExecutor{}, Registry[ExecutorTypePython], "Registered PythonExecutor instance type mismatch")
	}
}
