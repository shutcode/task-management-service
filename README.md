# Task Management Service

A distributed, general-purpose task management service.

## Overview

The system consists of two main services:
-   **Task Manager (`task-manager`)**: Provides RESTful APIs for managing tasks and task templates. It handles task creation, scheduling (cron-based and real-time), and updates task statuses based on results from workers. It also validates task parameters and results against JSON schemas defined in task templates.
-   **Task Worker (`task-worker`)**: Listens for tasks on a Kafka topic, executes them using registered executors, and sends results back to another Kafka topic. It can re-validate task parameters against a JSON schema if provided in the task payload.

Communication between these services is primarily via Kafka.

## Features Implemented

-   **Task & Task Template Management**: CRUD APIs for tasks and their templates.
-   **Dynamic Parameters**: Task parameters and results are defined by JSON schemas in templates.
-   **Scheduling**:
    -   Real-time: Tasks created via API are immediately dispatched for execution.
    -   Cron-based: Tasks can be scheduled via cron expressions in task templates.
-   **Executor Model**: Task workers use a registry to load and run different task executors (currently, a sample `echo-executor` is implemented).
-   **Kafka Integration**: For task dispatching and result reporting.
-   **JSON Schema Validation**:
    -   Task Manager: Validates incoming task parameters and outgoing task results.
    -   Task Worker: Can re-validate task parameters before execution.
-   **Basic Unit Tests**: For core components and utilities.

## Technology Stack

-   **Go**: Language for both services.
-   **Hertz**: HTTP framework for `task-manager` APIs.
-   **GORM**: ORM for database interaction (TiDB/MySQL compatible, uses SQLite for local dev).
-   **Kafka**: Message broker (via `segmentio/kafka-go`).
-   **JSON Schema**: For defining and validating task parameters/results (via `santhosh-tekuri/jsonschema/v5`).
-   **gocron**: For cron-based scheduling in `task-manager`.

## Getting Started

### Prerequisites

-   Go (version 1.23.0 or higher, due to gocron dependency)
-   Kafka instance running.
-   (Optional) MySQL/TiDB instance if not using the default SQLite for Task Manager.

### Environment Variables

Both services use environment variables for configuration. Key variables include:

**For Task Manager (`task-manager`):**
-   `SERVER_ADDR`: Address for the HTTP server (e.g., `:8080`).
-   `DB_TYPE`: Database type. Set to `mysql` to use MySQL/TiDB. Defaults to `sqlite` (creates `gorm.db` file).
-   `DB_DSN`: Database DSN. For MySQL/TiDB, e.g., `user:pass@tcp(host:port)/dbname?charset=utf8mb4&parseTime=True&loc=Local`.
-   `KAFKA_BROKERS`: Comma-separated list of Kafka brokers (e.g., `localhost:9092`).
-   `TASK_DISPATCH_TOPIC`: Kafka topic for sending tasks to workers (defaults to `task_execution_requests`).
-   `RESULT_TOPIC`: Kafka topic for receiving results from workers (defaults to `task_results`).
-   `RESULT_GROUP_ID`: Kafka consumer group ID for reading results (defaults to `task-manager-results-group`).

**For Task Worker (`task-worker`):**
-   `KAFKA_BROKERS`: Comma-separated list of Kafka brokers (e.g., `localhost:9092`).
-   `TASK_TOPIC`: Kafka topic for receiving tasks (defaults to `task_execution_requests`).
-   `GROUP_ID`: Kafka consumer group ID for reading tasks (defaults to `task-worker-group`).
-   `RESULT_TOPIC`: Kafka topic for sending results back to manager (defaults to `task_results`).

### Building and Running

1.  **Clone the repository.**
2.  **Tidy dependencies:**
    ```bash
    go mod tidy
    ```
3.  **Run Task Manager:**
    ```bash
    go run cmd/task-manager/main.go
    # Or build: go build -o task-manager cmd/task-manager/main.go && ./task-manager
    ```
    The Task Manager will start its HTTP server (default `:8080`) and connect to Kafka and the database.

4.  **Run Task Worker:**
    ```bash
    go run cmd/task-worker/main.go
    # Or build: go build -o task-worker cmd/task-worker/main.go && ./task-worker
    ```
    The Task Worker will connect to Kafka and start listening for tasks.

## API Endpoints (Task Manager)

Base URL: `http://<server_addr>` (e.g., `http://localhost:8080`)

### Task Templates

-   **`POST /templates`**: Create a new task template.
    -   Payload: JSON object representing the template (name, description, task_type, param_schema, result_schema, executor_type, cron_expression).
    -   `param_schema` and `result_schema` are JSON strings representing valid JSON schemas.
-   **`GET /templates`**: List all task templates.
-   **`GET /templates/{id}`**: Get a specific task template by ID.
-   **`DELETE /templates/{id}`**: Delete a task template by ID.
    *(Note: Updating templates via PUT is not yet implemented; delete and recreate if changes are needed for cron expression or schema.)*

### Tasks

-   **`POST /tasks`**: Create a new task.
    -   Payload: JSON object (name, description, task_template_id, params).
    -   `params` is a JSON string that must validate against the `param_schema` of the linked template.
-   **`GET /tasks`**: List all tasks. Supports query parameters:
    -   `status`: Filter by task status (e.g., PENDING, COMPLETED, FAILED).
    -   `template_id`: Filter by task template ID.
-   **`GET /tasks/{id}`**: Get a specific task by ID.
-   **`PUT /tasks/{id}`**: Update a task's status or result. (Typically used internally or by trusted systems, not the primary way workers update status, which is via Kafka).

### Admin (Example)
-   **`POST /admin/scheduler/refresh`**: Manually trigger a refresh of cron jobs from templates in the database. (This was added during development and might require auth in a real system).

## Development & Testing

-   **Unit Tests**: Run with `go test ./...`. Some API validation tests related to Hertz's test environment are currently skipped.
-   **Dependencies**: Managed by Go Modules (`go.mod`, `go.sum`).

## Future Considerations (Not Implemented)

-   Go Plugins for truly dynamic executor loading in Task Worker.
-   More sophisticated error handling and retry mechanisms for Kafka messages and task execution.
-   Authentication and Authorization for APIs.
-   Distributed tracing and monitoring.
-   More comprehensive integration and end-to-end tests.
-   Updating Task Templates (especially cron expressions) and having the scheduler update dynamically without full reload or with more granular control.

EOF
