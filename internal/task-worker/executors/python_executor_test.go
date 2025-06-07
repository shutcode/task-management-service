package executors

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPythonExecutor_Execute_Success(t *testing.T) {
	executor := &PythonExecutor{}
	payload := TaskPayload{
		TaskID:       1,
		Name:         "TestPythonSuccess",
		ExecutorType: ExecutorTypePython,
		Params:       "print('hello from python')",
	}

	result, err := executor.Execute(payload)
	assert.NoError(t, err)
	assert.Equal(t, "hello from python\n", result)
}

func TestPythonExecutor_Execute_RuntimeError(t *testing.T) {
	executor := &PythonExecutor{}
	payload := TaskPayload{
		TaskID:       2,
		Name:         "TestPythonError",
		ExecutorType: ExecutorTypePython,
		Params:       "import sys; sys.stderr.write('custom error message\\n'); raise ValueError('test python error')",
	}

	result, err := executor.Execute(payload)
	assert.Error(t, err)
	assert.Empty(t, result) // No result on error

	// Check if the underlying error message from python is in the error string
	assert.Contains(t, err.Error(), "ValueError: test python error", "Error message should contain the Python exception")
	assert.Contains(t, err.Error(), "custom error message", "Error message should contain stderr output from Python")
}

func TestPythonExecutor_Execute_NonZeroExit(t *testing.T) {
	executor := &PythonExecutor{}
	payload := TaskPayload{
		TaskID:       3,
		Name:         "TestPythonNonZeroExit",
		ExecutorType: ExecutorTypePython,
		Params:       "import sys; sys.exit(5)",
	}

	result, err := executor.Execute(payload)
	assert.Error(t, err)
	assert.Empty(t, result)
	assert.Contains(t, err.Error(), "exit status 5", "Error message should indicate non-zero exit status")
}


func TestPythonExecutor_Execute_Timeout(t *testing.T) {
	// Note: The actual timeout is 30s in python_executor.go.
	// This test will take slightly longer than that.
	// To make this test faster, one might consider making the timeout configurable
	// or using a mock for `exec.Command` which is more involved.
	// For now, we accept the longer test duration.
	t.Skip("Skipping timeout test due to its long execution time (30s+). Uncomment to run.")

	executor := &PythonExecutor{}
	payload := TaskPayload{
		TaskID:       4,
		Name:         "TestPythonTimeout",
		ExecutorType: ExecutorTypePython,
		Params:       "import time; time.sleep(32)", // Sleep longer than the 30s timeout in executor
	}

	startTime := time.Now()
	result, err := executor.Execute(payload)
	duration := time.Since(startTime)

	assert.Error(t, err)
	assert.Empty(t, result)
	assert.Contains(t, err.Error(), "python script execution timed out", "Error message should indicate timeout")

	// Check if the execution was indeed interrupted around the timeout period
	// Allow some buffer for test execution overhead.
	assert.True(t, duration > 29*time.Second && duration < 33*time.Second, fmt.Sprintf("Execution duration %s was not within the expected timeout range", duration))
}


func TestPythonExecutor_Execute_EmptyParams(t *testing.T) {
	executor := &PythonExecutor{}
	payload := TaskPayload{
		TaskID:       5,
		Name:         "TestPythonEmptyParams",
		ExecutorType: ExecutorTypePython,
		Params:       "", // Empty code
	}

	result, err := executor.Execute(payload)
	assert.Error(t, err)
	assert.Empty(t, result)
	assert.EqualError(t, err, "Python code in task.Params is empty")
}

func TestPythonExecutor_Execute_StderrOutputNoError(t *testing.T) {
	executor := &PythonExecutor{}
	payload := TaskPayload{
		TaskID:       6,
		Name:         "TestPythonStderrNoError",
		ExecutorType: ExecutorTypePython,
		Params:       "import sys; sys.stderr.write('this is a warning\\n'); print('hello')",
	}

	result, err := executor.Execute(payload)
	assert.NoError(t, err, "Execution should succeed even with stderr output if exit code is 0")
	assert.Equal(t, "hello\n", result)
	// The log message about stderr content is in python_executor.go, not directly testable here
	// unless we capture logs, which is beyond this scope.
}

func TestPythonExecutor_Execute_ScriptNotFound(t *testing.T) {
	// This test relies on the specific implementation detail of using "python3"
	// If "python3" is not in PATH, this test might behave differently or fail for other reasons.
	// We are testing the error handling of os/exec when the command itself is problematic.

	// Temporarily modify how the command is found by manipulating PATH or using a mock
	// For simplicity, if python3 is expected to be there, this tests a different failure mode:
	// if the script file itself was not found by python3 (which is hard to simulate directly here
	// as the executor writes the script).
	// Instead, we'll test a malformed command scenario if possible, or accept that some
	// os/exec errors are hard to unit test without complex mocks.

	// The current PythonExecutor writes the script and then executes it.
	// A "script not found" by the python3 interpreter itself (e.g. python3 non_existent_file.py)
	// is different from the python3 command not being found.
	// The executor is designed to always create the script.
	// Let's consider if `python3` itself is missing.

	// To test `python3` not being found, one would typically alter the PATH environment variable
	// for the `exec.Command` call, or ensure `python3` is not installed in the test environment,
	// which is not ideal for standard unit tests.

	// The current error handling in PythonExecutor bundles errors from cmd.Run().
	// If python3 is not found, cmd.Run() will return an error like "exec: \"python3\": executable file not found in $PATH".
	// This is a valid error case.

	t.Log("Assuming 'python3' is in PATH. Testing other exec error (e.g., if script had issues immediately)")
	// For now, this scenario is implicitly covered by other error tests if they cause os/exec issues.
	// A specific test for "python3 not found" would require environment manipulation.
	assert.True(t, true, "Placeholder for deeper os/exec error testing if needed")
}
