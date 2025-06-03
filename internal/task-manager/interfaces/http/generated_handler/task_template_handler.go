package generated_handler

import (
	"context"
	"fmt" // For error formatting
	"log"
	// "net/http" // Removed: Not directly used for response codes
	"strconv"  // For parsing path parameters

	"github.com/cloudwego/hertz/pkg/app"
	// "github.com/cloudwego/hertz/pkg/common/utils" // Removed: Not directly used for utils.H

	gorm_models "task-management-service/internal/task-manager/db"
	pb "task-management-service/pkg/pb/task_template_pb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"gorm.io/gorm"
)

type TaskTemplateServiceImpl struct {
	DB *gorm.DB
}

func NewTaskTemplateService(db *gorm.DB) *TaskTemplateServiceImpl {
	return &TaskTemplateServiceImpl{DB: db}
}

func (s *TaskTemplateServiceImpl) CreateTaskTemplate(ctx context.Context, c *app.RequestContext, req *pb.CreateTaskTemplateRequest) (*pb.CreateTaskTemplateResponse, error) {
	log.Printf("Generated Handler: CreateTaskTemplate called with Name: %s, Type: %s", req.Name, req.TaskType)

	dbTemplate := gorm_models.TaskTemplate{
		Name:           req.Name,
		Description:    req.Description,
		TaskType:       req.TaskType,
		ParamSchema:    req.ParamSchema,
		ResultSchema:   req.ResultSchema,
		ExecutorType:   req.ExecutorType,
		CronExpression: req.CronExpression,
	}

	if result := s.DB.Create(&dbTemplate); result.Error != nil {
		log.Printf("Error creating task template in DB: %v", result.Error)
		return nil, result.Error
	}

	respTemplate := &pb.TaskTemplate{
		Id:             uint32(dbTemplate.ID), Name: dbTemplate.Name, Description: dbTemplate.Description,
		TaskType: dbTemplate.TaskType, ParamSchema: dbTemplate.ParamSchema, ResultSchema: dbTemplate.ResultSchema,
		ExecutorType: dbTemplate.ExecutorType, CronExpression: dbTemplate.CronExpression,
		CreatedAt: timestamppb.New(dbTemplate.CreatedAt), UpdatedAt: timestamppb.New(dbTemplate.UpdatedAt),
	}
	return &pb.CreateTaskTemplateResponse{Template: respTemplate}, nil
}

func (s *TaskTemplateServiceImpl) GetTaskTemplate(ctx context.Context, c *app.RequestContext, req *pb.GetTaskTemplateRequest) (*pb.GetTaskTemplateResponse, error) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		log.Printf("Error parsing 'id' path parameter '%s': %v", idStr, err)
		return nil, fmt.Errorf("invalid ID format: %w", err)
	}

	log.Printf("Generated Handler: GetTaskTemplate called with ID %d", id)
	var dbTemplate gorm_models.TaskTemplate
	if dbErr := s.DB.First(&dbTemplate, uint32(id)).Error; dbErr != nil {
		log.Printf("Error fetching task template ID %d from DB: %v", id, dbErr)
		return nil, dbErr
	}

	respTemplate := &pb.TaskTemplate{
		Id:             uint32(dbTemplate.ID), Name: dbTemplate.Name, Description: dbTemplate.Description,
		TaskType: dbTemplate.TaskType, ParamSchema: dbTemplate.ParamSchema, ResultSchema: dbTemplate.ResultSchema,
		ExecutorType: dbTemplate.ExecutorType, CronExpression: dbTemplate.CronExpression,
		CreatedAt: timestamppb.New(dbTemplate.CreatedAt), UpdatedAt: timestamppb.New(dbTemplate.UpdatedAt),
	}
	return &pb.GetTaskTemplateResponse{Template: respTemplate}, nil
}

func (s *TaskTemplateServiceImpl) ListTaskTemplates(ctx context.Context, c *app.RequestContext, req *pb.ListTaskTemplatesRequest) (*pb.ListTaskTemplatesResponse, error) {
	log.Printf("Generated Handler: ListTaskTemplates called")
	var dbTemplates []gorm_models.TaskTemplate
	if err := s.DB.Find(&dbTemplates).Error; err != nil {
		log.Printf("Error listing task templates from DB: %v", err)
		return nil, err
	}

	respTemplates := make([]*pb.TaskTemplate, len(dbTemplates))
	for i, dbTmpl := range dbTemplates {
		respTemplates[i] = &pb.TaskTemplate{
			Id: uint32(dbTmpl.ID), Name: dbTmpl.Name, Description: dbTmpl.Description, TaskType: dbTmpl.TaskType,
			ParamSchema: dbTmpl.ParamSchema, ResultSchema: dbTmpl.ResultSchema, ExecutorType: dbTmpl.ExecutorType,
			CronExpression: dbTmpl.CronExpression, CreatedAt: timestamppb.New(dbTmpl.CreatedAt), UpdatedAt: timestamppb.New(dbTmpl.UpdatedAt),
		}
	}
	return &pb.ListTaskTemplatesResponse{Templates: respTemplates}, nil
}

func (s *TaskTemplateServiceImpl) UpdateTaskTemplate(ctx context.Context, c *app.RequestContext, req *pb.UpdateTaskTemplateRequest) (*pb.UpdateTaskTemplateResponse, error) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		log.Printf("Error parsing 'id' path parameter '%s': %v", idStr, err)
		return nil, fmt.Errorf("invalid ID format in path: %w", err)
	}

	if req.Id != 0 && req.Id != uint32(id) {
		log.Printf("Path ID %d mismatches request body ID %d", id, req.Id)
		return nil, fmt.Errorf("path ID mismatch with request body ID")
	}
	log.Printf("Generated Handler: UpdateTaskTemplate called for ID %d with %+v", id, req)

	var dbTemplate gorm_models.TaskTemplate
	if dbErr := s.DB.First(&dbTemplate, uint32(id)).Error; dbErr != nil {
		log.Printf("Error fetching task template ID %d for update: %v", id, dbErr)
		return nil, dbErr
	}

	dbTemplate.Name = req.Name
	dbTemplate.Description = req.Description
	dbTemplate.TaskType = req.TaskType
	dbTemplate.ParamSchema = req.ParamSchema
	dbTemplate.ResultSchema = req.ResultSchema
	dbTemplate.ExecutorType = req.ExecutorType
	dbTemplate.CronExpression = req.CronExpression

	if result := s.DB.Save(&dbTemplate); result.Error != nil {
		log.Printf("Error updating task template ID %d in DB: %v", id, result.Error)
		return nil, result.Error
	}

	respTemplate := &pb.TaskTemplate{
		Id: uint32(dbTemplate.ID), Name: dbTemplate.Name, Description: dbTemplate.Description, TaskType: dbTemplate.TaskType,
		ParamSchema: dbTemplate.ParamSchema, ResultSchema: dbTemplate.ResultSchema, ExecutorType: dbTemplate.ExecutorType,
		CronExpression: dbTemplate.CronExpression, CreatedAt: timestamppb.New(dbTemplate.CreatedAt), UpdatedAt: timestamppb.New(dbTemplate.UpdatedAt),
	}
	return &pb.UpdateTaskTemplateResponse{Template: respTemplate}, nil
}

func (s *TaskTemplateServiceImpl) DeleteTaskTemplate(ctx context.Context, c *app.RequestContext, req *pb.DeleteTaskTemplateRequest) (*emptypb.Empty, error) {
	idStr := c.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		log.Printf("Error parsing 'id' path parameter '%s': %v", idStr, err)
		return nil, fmt.Errorf("invalid ID format in path: %w", err)
	}
	log.Printf("Generated Handler: DeleteTaskTemplate called for ID %d", id)

	var tmpl gorm_models.TaskTemplate
	if dbErr := s.DB.First(&tmpl, uint32(id)).Error; dbErr != nil {
		if dbErr == gorm.ErrRecordNotFound {
			log.Printf("Task template ID %d not found for deletion.", id)
			return nil, dbErr
		}
		log.Printf("Error finding task template ID %d for deletion: %v", id, dbErr)
		return nil, dbErr
	}

	if result := s.DB.Delete(&gorm_models.TaskTemplate{}, uint32(id)); result.Error != nil {
		log.Printf("Error deleting task template ID %d from DB: %v", id, result.Error)
		return nil, result.Error
	}

	log.Printf("TaskTemplate ID %d deleted. Associated cron job should be unscheduled if it existed.", id)
	return &emptypb.Empty{}, nil
}
