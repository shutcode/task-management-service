package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"task-management-service/internal/task-manager/db"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	// "github.com/cloudwego/hertz/pkg/app/validator" // Removed this import again
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupTestAppWithRoutes(t *testing.T) (*route.Engine, *gorm.DB) {
	testDBFile := "test_api_handler.db"
	_ = os.Remove(testDBFile)

	gormDB, err := gorm.Open(sqlite.Open(testDBFile), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}
	gormDB.AutoMigrate(&db.TaskTemplate{}, &db.Task{})

	h := server.New(
		server.WithHostPorts("127.0.0.1:0"),
		server.WithExitWaitTime(1*time.Millisecond),
		// No explicit validator here, relying on Hertz's default for server.New()
	)

	templateHandler := NewTaskTemplateHandler(gormDB)
	templateGroup := h.Group("/templates")
	{
		templateGroup.POST("", templateHandler.CreateTaskTemplate)
		templateGroup.GET("/:id", templateHandler.GetTaskTemplateByID)
	}

	return h.Engine, gormDB
}

func teardownTestDBFromRouter(gormDB *gorm.DB, t *testing.T) {
	if gormDB != nil {
		sqlDB, err := gormDB.DB()
		if err == nil && sqlDB != nil {
			err = sqlDB.Close()
			if err != nil { t.Logf("Warning: could not close test API DB: %v", err) }
		}
	}
	err := os.Remove("test_api_handler.db")
	if err != nil && !os.IsNotExist(err) {
		t.Logf("Warning: could not remove test API DB file: %v", err)
	}
}


func TestCreateTaskTemplateAPI(t *testing.T) {
	router, gormDB := setupTestAppWithRoutes(t)
	defer teardownTestDBFromRouter(gormDB, t)

	templatePayload := CreateTaskTemplateRequest{
		Name:           "APITestTemplate",
		Description:    "API Test Desc",
		TaskType:       "api_test",
		ParamSchema:    `{"type":"object"}`,
		ResultSchema:   `{"type":"object"}`,
		ExecutorType:   "api-executor",
		CronExpression: "0 0 * * *",
	}
	payloadBytes, _ := json.Marshal(templatePayload)

	w := ut.PerformRequest(router, "POST", "/templates", &ut.Body{Body: bytes.NewReader(payloadBytes), Len: len(payloadBytes)},
		ut.Header{Key: "Content-Type", Value: "application/json"})

	resp := w.Result()
	assert.Equal(t, http.StatusCreated, resp.StatusCode())

	var createdTemplate db.TaskTemplate
	err := json.Unmarshal(resp.Body(), &createdTemplate)
	assert.NoError(t, err)
	assert.Equal(t, templatePayload.Name, createdTemplate.Name)
	assert.NotZero(t, createdTemplate.ID)
	assert.Equal(t, templatePayload.CronExpression, createdTemplate.CronExpression)

	var dbTemplate db.TaskTemplate
	gormDB.First(&dbTemplate, createdTemplate.ID)
	assert.Equal(t, templatePayload.Name, dbTemplate.Name)
}


func TestGetTaskTemplateByIDAPI(t *testing.T) {
	router, gormDB := setupTestAppWithRoutes(t)
	defer teardownTestDBFromRouter(gormDB, t)

	prePopulated := db.TaskTemplate{
		Name: "PrePop", ExecutorType: "exec", ParamSchema: "{}", ResultSchema: "{}", TaskType: "tt",
	}
	gormDB.Create(&prePopulated)
	assert.NotZero(t, prePopulated.ID)

	url := "/templates/" + strconv.FormatUint(uint64(prePopulated.ID), 10)
	w := ut.PerformRequest(router, "GET", url, nil)
	resp := w.Result()

	assert.Equal(t, http.StatusOK, resp.StatusCode())
	var fetchedTemplate db.TaskTemplate
	json.Unmarshal(resp.Body(), &fetchedTemplate)
	assert.Equal(t, prePopulated.Name, fetchedTemplate.Name)
	assert.Equal(t, prePopulated.ID, fetchedTemplate.ID)
}

func TestCreateTaskTemplateAPI_InvalidPayload(t *testing.T) {
    router, gormDB := setupTestAppWithRoutes(t)
    defer teardownTestDBFromRouter(gormDB, t)

    invalidPayload := `{"Description":"Test", "TaskType":"type", "ParamSchema":"{}", "ResultSchema":"{}", "ExecutorType":"exec"}`

    w := ut.PerformRequest(router, "POST", "/templates", &ut.Body{Body: bytes.NewReader([]byte(invalidPayload)), Len: len(invalidPayload)},
        ut.Header{Key: "Content-Type", Value: "application/json"})

    resp := w.Result()
    // This test is currently expected to fail validation (expect 400, get 201)
    // until the root cause of validator not firing in test is resolved.
    // For now, we keep the assertion as is.
    assert.Equal(t, http.StatusBadRequest, resp.StatusCode(), "Expected 400 Bad Request for invalid payload")

    var errorResponse map[string]interface{}
    err := json.Unmarshal(resp.Body(), &errorResponse)
    if err == nil { // Only check error content if unmarshal succeeded
        errorVal, ok := errorResponse["error"].(string)
        if ok {
            assert.Contains(t, errorVal, "Key: 'CreateTaskTemplateRequest.Name' Error:Field validation for 'Name' failed on the 'required' tag",
                "Error message should specifically mention the 'Name' field validation failure")
        } else {
            // If "error" field is not a string, or not present, fail the assertion for error content.
            assert.Fail(t, "Error response did not contain a string 'error' field as expected.")
        }
    } else if resp.StatusCode() == http.StatusBadRequest {
        // If we expected a 400 and got it, but couldn't unmarshal the error response,
        // it implies the error response format is not what we expected.
         assert.NoError(t, err, "Should be able to unmarshal error response if status is 400")
    }
    // If status code is not 400, the primary assertion (assert.Equal for status code) would have already failed.
}
