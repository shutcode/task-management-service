package db

import (
	"os"
	"testing"
	// "task-management-service/pkg/db" // Not used directly, using gorm.Open

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *gorm.DB {
	testDBFile := "test_gorm.db"
	// Attempt to remove before test to ensure clean state, ignore error if not exists
	_ = os.Remove(testDBFile)

	gormDB, err := gorm.Open(sqlite.Open(testDBFile), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	err = gormDB.AutoMigrate(&TaskTemplate{}, &Task{})
	if err != nil {
		t.Fatalf("Failed to migrate test database: %v", err)
	}
	return gormDB
}

func teardownTestDB(gormDB *gorm.DB, t *testing.T) {
	sqlDB, err := gormDB.DB()
	if err == nil { // only close if getting DB didn't error
		err = sqlDB.Close()
		if err != nil {
			t.Logf("Warning: could not close test DB: %v", err)
		}
	}
	err = os.Remove("test_gorm.db")
	if err != nil && !os.IsNotExist(err) { // only log if error is not "file does not exist"
		t.Logf("Warning: could not remove test DB file: %v", err)
	}
}

func TestTaskTemplateCRUD(t *testing.T) {
	gormDB := setupTestDB(t)
	defer teardownTestDB(gormDB, t)

	// Create
	template := TaskTemplate{
		Name:           "TestTemplateForCRUD",
		Description:    "A test template",
		TaskType:       "test_type",
		ParamSchema:    `{"type":"object"}`,
		ResultSchema:   `{"type":"object"}`,
		ExecutorType:   "test_executor",
		CronExpression: "* * * * *",
	}
	result := gormDB.Create(&template)
	assert.NoError(t, result.Error)
	assert.NotZero(t, template.ID)

	// Read
	var fetchedTemplate TaskTemplate
	result = gormDB.First(&fetchedTemplate, template.ID)
	assert.NoError(t, result.Error)
	assert.Equal(t, template.Name, fetchedTemplate.Name)
	assert.Equal(t, template.CronExpression, fetchedTemplate.CronExpression)

	// Update
	fetchedTemplate.Description = "Updated description"
	result = gormDB.Save(&fetchedTemplate)
	assert.NoError(t, result.Error)

	var updatedTemplate TaskTemplate
	gormDB.First(&updatedTemplate, fetchedTemplate.ID)
	assert.Equal(t, "Updated description", updatedTemplate.Description)

	// Delete
	result = gormDB.Delete(&updatedTemplate)
	assert.NoError(t, result.Error)

	var deletedTemplate TaskTemplate
	result = gormDB.First(&deletedTemplate, template.ID)
	assert.Error(t, result.Error)
	assert.Equal(t, gorm.ErrRecordNotFound, result.Error)
}
