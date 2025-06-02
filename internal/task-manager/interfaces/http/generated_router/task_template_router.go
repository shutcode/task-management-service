package generated_router

import (
	"context"
	// "fmt" // Removed: Not directly used
	"log"
	"net/http"
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"

	"task-management-service/internal/task-manager/interfaces/http/generated_handler"
	pb "task-management-service/pkg/pb/task_template_pb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// RegisterTaskTemplateService registers HTTP routes for the TaskTemplateService.
func RegisterTaskTemplateService(
	r *server.Hertz,
	service *generated_handler.TaskTemplateServiceImpl,
) {
	templatesGroup := r.Group("/templates")
	{
		templatesGroup.POST("/", func(ctx context.Context, c *app.RequestContext) {
			var req pb.CreateTaskTemplateRequest
			if err := c.BindAndValidate(&req); err != nil {
				log.Printf("Router: BindAndValidate CreateTaskTemplate failed: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request payload: " + err.Error()})
				return
			}
			resp, err := service.CreateTaskTemplate(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.CreateTaskTemplate failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusCreated, resp)
		})

		templatesGroup.GET("/:id", func(ctx context.Context, c *app.RequestContext) {
			idStr := c.Param("id")
			id, err := strconv.ParseUint(idStr, 10, 32)
			if err != nil {
				log.Printf("Router: Invalid ID format for GetTaskTemplate: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid ID format"})
				return
			}
			req := pb.GetTaskTemplateRequest{Id: uint32(id)}

			resp, err := service.GetTaskTemplate(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.GetTaskTemplate failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		templatesGroup.GET("/", func(ctx context.Context, c *app.RequestContext) {
			var req pb.ListTaskTemplatesRequest
			resp, err := service.ListTaskTemplates(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.ListTaskTemplates failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		templatesGroup.PUT("/:id", func(ctx context.Context, c *app.RequestContext) {
			idStr := c.Param("id")
			id, err := strconv.ParseUint(idStr, 10, 32)
			if err != nil {
				log.Printf("Router: Invalid ID format for UpdateTaskTemplate: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid ID format"})
				return
			}

			var req pb.UpdateTaskTemplateRequest
			if err := c.BindAndValidate(&req); err != nil {
				log.Printf("Router: BindAndValidate UpdateTaskTemplate failed: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid request payload: " + err.Error()})
				return
			}
			req.Id = uint32(id)

			resp, err := service.UpdateTaskTemplate(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.UpdateTaskTemplate failed: %v", err)
				c.JSON(http.StatusInternalServerError, utils.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, resp)
		})

		templatesGroup.DELETE("/:id", func(ctx context.Context, c *app.RequestContext) {
			idStr := c.Param("id")
			id, err := strconv.ParseUint(idStr, 10, 32)
			if err != nil {
				log.Printf("Router: Invalid ID format for DeleteTaskTemplate: %v", err)
				c.JSON(http.StatusBadRequest, utils.H{"error": "Invalid ID format"})
				return
			}
			req := pb.DeleteTaskTemplateRequest{Id: uint32(id)}

			var respProto *emptypb.Empty
			respProto, err = service.DeleteTaskTemplate(ctx, c, &req)
			if err != nil {
				log.Printf("Router: service.DeleteTaskTemplate failed: %v", err)
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
