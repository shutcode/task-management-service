package main

import (
	"context"
	stdlog "log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/hertz/pkg/common/utils"

	"task-management-service/internal/task-manager/api"
	taskDB "task-management-service/internal/task-manager/db"
	tmKafka "task-management-service/internal/task-manager/kafka"
	"task-management-service/internal/task-manager/services"
	gorm_db "task-management-service/pkg/db"
)

func main() {
	stdlog.Println("Task Manager Service starting...")

	appCtx, appCancel := context.WithCancel(context.Background())
	// defer appCancel() // Called explicitly in graceful shutdown

	gormDB, err := gorm_db.NewGormDB()
	if err != nil {
		stdlog.Fatalf("Failed to initialize database: %v", err)
	}
	stdlog.Println("Database initialized successfully.")

	stdlog.Println("Running database migrations...")
	err = gorm_db.AutoMigrate(gormDB, &taskDB.Task{}, &taskDB.TaskTemplate{})
	if err != nil {
		stdlog.Fatalf("Failed to migrate database: %v", err)
	}
	stdlog.Println("Database migration successful.")

	kafkaProducer := tmKafka.NewKafkaProducer()

	resultService := services.NewResultService(gormDB)
	resultService.StartConsuming(appCtx)

	schedulerService, err := services.NewSchedulerService(appCtx, gormDB, kafkaProducer)
	if err != nil {
		stdlog.Fatalf("Failed to create scheduler service: %v", err)
	}
	schedulerService.Start()

	serverAddr := os.Getenv("SERVER_ADDR")
	if serverAddr == "" {
		serverAddr = ":8080"
	}

	hlog.SetOutput(os.Stdout)
	hlog.SetLevel(hlog.LevelInfo)

	h := server.Default(server.WithHostPorts(serverAddr), server.WithExitWaitTime(5*time.Second))

	templateHandler := api.NewTaskTemplateHandler(gormDB)
	taskHandler := api.NewTaskHandler(gormDB, kafkaProducer)

	templateGroup := h.Group("/templates")
	{
		templateGroup.POST("", templateHandler.CreateTaskTemplate)
		templateGroup.GET("", templateHandler.GetTaskTemplates)
		templateGroup.GET("/:id", templateHandler.GetTaskTemplateByID)
        templateGroup.DELETE("/:id", templateHandler.DeleteTaskTemplate)
	}
	taskGroup := h.Group("/tasks")
	{
		taskGroup.POST("", taskHandler.CreateTask)
		taskGroup.GET("", taskHandler.GetTasks)
		taskGroup.GET("/:id", taskHandler.GetTaskByID)
		taskGroup.PUT("/:id", taskHandler.UpdateTask)
	}
	adminGroup := h.Group("/admin")
	adminGroup.POST("/scheduler/refresh", func(c context.Context, ctxReq *app.RequestContext){
		schedulerService.RefreshScheduledJobs()
		ctxReq.JSON(http.StatusOK, utils.H{"message": "Scheduler refresh triggered"})
	})

	h.GET("/ping", func(c context.Context, ctxReq *app.RequestContext) {
		ctxReq.JSON(http.StatusOK, utils.H{"message": "pong"})
	})

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		sig := <-signals
		hlog.Infof("Received signal: %s. Initiating graceful shutdown...", sig)

		appCancel()

		shutdownCtx, httpShutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer httpShutdownCancel()
		if err := h.Shutdown(shutdownCtx); err != nil {
			hlog.Errorf("Hertz server shutdown error: %v", err)
		} else {
			hlog.Info("Hertz server gracefully stopped.")
		}

		schedulerService.Stop()

		resultService.Close()
		hlog.Info("Result service consumer closed.")

		if err := kafkaProducer.Close(); err != nil {
			hlog.Errorf("Kafka producer close error: %v", err)
		} else {
			hlog.Info("Kafka producer closed.")
		}
		hlog.Info("Task Manager gracefully shut down.")
	}()

	hlog.Infof("Task Manager Service fully initialized and starting Hertz server on %s...", serverAddr)
	// Corrected: h.Spin() does not return an error to be checked with if err := ...
	// It's a blocking call. Fatal errors during startup are handled by Hertz.
	h.Spin()

	stdlog.Println("Task Manager Service has been shut down.")
}
