package executors

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"
	"log"
)

// PythonExecutor executes Python scripts.
type PythonExecutor struct{}

// Execute runs the Python script provided in taskPayload.Params.
// It conforms to the Executor interface defined in registry.go.
func (pe *PythonExecutor) Execute(taskPayload TaskPayload) (result string, err error) {
	pythonCode := taskPayload.Params
	if pythonCode == "" {
		return "", fmt.Errorf("Python code in task.Params is empty")
	}

	log.Printf("PythonExecutor: Executing task ID %d (%s)", taskPayload.TaskID, taskPayload.Name)
	log.Printf("PythonExecutor: Received code snippet:\n%s", pythonCode)

	// Create a temporary directory for the script
	tempDir, err := ioutil.TempDir("", "python_executor_scripts_")
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir) // Clean up the directory afterwards

	// Create the temporary Python script file
	scriptPath := filepath.Join(tempDir, "script.py")
	err = ioutil.WriteFile(scriptPath, []byte(pythonCode), 0755) // Give execute permissions
	if err != nil {
		return "", fmt.Errorf("failed to write python script to temp file: %w", err)
	}

	log.Printf("PythonExecutor: Script written to %s", scriptPath)

	// Prepare the command
	cmd := exec.Command("python3", scriptPath)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Execute the command with a timeout
	// Using a simple timeout here, could be configurable
	timeout := 30 * time.Second
	done := make(chan error, 1)
	go func() {
		done <- cmd.Run()
	}()

	select {
	case <-time.After(timeout):
		// Command timed out
		if cmd.Process != nil {
			cmd.Process.Kill() // Attempt to kill the process
		}
		log.Printf("PythonExecutor: Command timed out after %s for task ID %d", timeout, taskPayload.TaskID)
		return "", fmt.Errorf("python script execution timed out after %s. Stderr: %s", timeout, stderr.String())
	case err = <-done:
		// Command completed or failed
		if err != nil {
			log.Printf("PythonExecutor: Command failed for task ID %d. Error: %v. Stderr: %s", taskPayload.TaskID, err, stderr.String())
			return "", fmt.Errorf("python script execution failed: %w. Stderr: %s", err, stderr.String())
		}
	}

	resultStr := stdout.String()
	log.Printf("PythonExecutor: Task ID %d completed. Stdout: %s", taskPayload.TaskID, resultStr)

	// If stderr has content, it might indicate warnings or non-fatal errors,
	// which could be useful to log or include in the result depending on requirements.
	// For now, we only return an error if cmd.Run() itself returned an error.
	if stderr.Len() > 0 {
		log.Printf("PythonExecutor: Task ID %d has Stderr content:\n%s", taskPayload.TaskID, stderr.String())
	}

	return resultStr, nil
}

// Note: The Executor interface in registry.go does not define a Validate method.
// If validation is required for PythonExecutor, it would be a separate method
// not part of the common Executor interface.
//
// func (pe *PythonExecutor) Validate(taskPayload TaskPayload) error {
// 	if taskPayload.Params == "" {
// 		return fmt.Errorf("python code (task.Params) cannot be empty")
// 	}
// 	// Potentially add more validation, e.g., basic syntax check (complex)
// 	return nil
// }

// Ensure PythonExecutor satisfies the Executor interface from registry.go.
// This is a compile-time check.
var _ Executor = (*PythonExecutor)(nil)
