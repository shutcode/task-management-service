package models

// Task represents a task to be executed.
// This is a placeholder based on the needs of EchoExecutor.
// It should be updated with the actual fields required.
type Task struct {
	ID     string
	Name   string
	Type   string // Corresponds to ExecutorType in the old TaskPayload
	Params string // Parameters for the task execution
	// Add other fields as necessary, e.g.:
	// Status string
	// CreatedAt time.Time
	// UpdatedAt time.Time
	// RetryCount int
}
