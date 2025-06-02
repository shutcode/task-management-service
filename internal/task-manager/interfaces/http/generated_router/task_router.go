package generated_router

import (
	"context"
	"log"
	"net/http"
	// "strconv" // Removed: Path params are parsed in handler now

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"

	"task-management-service/internal/task-manager/interfaces/http/generated_handler"
	pb_task "task-management-service/pkg/pb/task_pb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// RegisterTaskService registers HTTP routes for the TaskService.
func RegisterTaskService(
	r *server.Hertz,
	service *generated_handler.TaskServiceImpl,
) {
	tasksGroup := r.Group("/tasks")
	{
		// CreateTask
		tasksGroup.POST("/", func(ctx context.Context, c *app.RequestContext) {
			var req pb_task.CreateTaskRequest
			if err := c.BindAndValidate(&req); err != nil {
				log.Printf("Router: BindAndValidate CreateTask failed: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request payload: " + err.Error()})
				return
			}
			resp, err := service.CreateTask(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.CreateTask failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusCreated, resp)
		})

		// GetTask
		tasksGroup.GET("/:id", func(ctx context.Context, c *app.RequestContext) {
			var req pb_task.GetTaskRequest
			// Handler `service.GetTask` will now use c.Param("id")
			resp, err := service.GetTask(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.GetTask failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		// ListTasks
		tasksGroup.GET("/", func(ctx context.Context, c *app.RequestContext) {
			var req pb_task.ListTasksRequest
			if err := c.BindAndValidate(&req); err != nil {
				log.Printf("Router: BindAndValidate ListTasks failed for query params: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid query parameters: " + err.Error()})
				return
			}
			resp, err := service.ListTasks(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.ListTasks failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		// UpdateTask
		tasksGroup.PUT("/:id", func(ctx context.Context, c *app.RequestContext) {
			var req pb_task.UpdateTaskRequest
			if err := c.BindAndValidate(&req); err != nil {
				log.Printf("Router: BindAndValidate UpdateTask failed: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request payload: " + err.Error()})
				return
			}
			// Handler `service.UpdateTask` will use c.Param("id") and req for body.
			resp, err := service.UpdateTask(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.UpdateTask failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		// DeleteTask
		tasksGroup.DELETE("/:id", func(ctx context.Context, c *app.RequestContext) {
			var req pb_task.DeleteTaskRequest
			var respProto *emptypb.Empty
			// Handler `service.DeleteTask` will use c.Param("id")
			respProto, err := service.DeleteTask(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.DeleteTask failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			if respProto == nil {
				respProto = &emptypb.Empty{}
			}
			c.JSON(http.StatusOK, respProto)
		})
	}
}
