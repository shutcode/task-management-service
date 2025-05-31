package db

import (
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// NewGormDB initializes and returns a GORM DB instance.
// It uses environment variables for configuration.
// DB_DSN for the database connection string.
// DB_TYPE for "mysql" or "sqlite" (defaults to sqlite for dev).
func NewGormDB() (*gorm.DB, error) {
	dbType := os.Getenv("DB_TYPE")
	dsn := os.Getenv("DB_DSN")

	var dialector gorm.Dialector

	if dbType == "mysql" {
		if dsn == "" {
			// Default DSN for local MySQL/TiDB if not provided
			// Replace with your actual TiDB/MySQL connection string
			dsn = "root:@tcp(127.0.0.1:3306)/task_management?charset=utf8mb4&parseTime=True&loc=Local"
			log.Println("Using default MySQL DSN: ", dsn)
		}
		dialector = mysql.Open(dsn)
	} else {
		// Default to SQLite for ease of local development
		if dsn == "" {
			dsn = "gorm.db"
			log.Println("Using default SQLite DSN: ", dsn)
		}
		dialector = sqlite.Open(dsn)
	}

	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  logger.Info, // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			Colorful:                  true,        // Disable color - corrected from 'Disable color' to true
		},
	)

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: newLogger,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Println("Database connection established successfully.")
	return db, nil
}

// AutoMigrate performs auto-migration for the given GORM models.
func AutoMigrate(db *gorm.DB, models ...interface{}) error {
	err := db.AutoMigrate(models...)
	if err != nil {
		return fmt.Errorf("failed to auto-migrate database: %w", err)
	}
	log.Println("Database migration completed successfully for provided models.")
	return nil
}
