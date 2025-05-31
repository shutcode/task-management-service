package events

// TaskDispatchPayload is sent by TaskManager to Kafka for TaskWorker
type TaskDispatchPayload struct {
	TaskID         uint   `json:"task_id"`
	TaskTemplateID uint   `json:"task_template_id"`
	Name           string `json:"name"`
	Params         string `json:"params"`
	ExecutorType   string `json:"executor_type"`
	ParamSchema    string `json:"param_schema,omitempty"` // Added ParamSchema
}

// TaskCompletionPayload is received by TaskManager from Kafka (sent by TaskWorker)
type TaskCompletionPayload struct {
	TaskID uint   `json:"task_id"`
	Status string `json:"status"`
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}
