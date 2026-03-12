package nexusoperation

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

// FrontendHandler provides the frontend-facing API for standalone Nexus operations.
type FrontendHandler interface {
	StartNexusOperationExecution(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error)
	DescribeNexusOperationExecution(context.Context, *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error)
	PollNexusOperationExecution(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error)
	ListNexusOperationExecutions(context.Context, *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error)
	CountNexusOperationExecutions(context.Context, *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error)
	RequestCancelNexusOperationExecution(context.Context, *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error)
	TerminateNexusOperationExecution(context.Context, *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error)
	DeleteNexusOperationExecution(context.Context, *workflowservice.DeleteNexusOperationExecutionRequest) (*workflowservice.DeleteNexusOperationExecutionResponse, error)
}

type frontendHandler struct {
	config            *Config
	logger            log.Logger
	namespaceRegistry namespace.Registry
}

func NewFrontendHandler(
	config *Config,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
) FrontendHandler {
	return &frontendHandler{
		config:            config,
		logger:            logger,
		namespaceRegistry: namespaceRegistry,
	}
}

func (h *frontendHandler) StartNexusOperationExecution(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("StartNexusOperationExecution not implemented")
}

func (h *frontendHandler) DescribeNexusOperationExecution(context.Context, *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("DescribeNexusOperationExecution not implemented")
}

func (h *frontendHandler) PollNexusOperationExecution(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("PollNexusOperationExecution not implemented")
}

func (h *frontendHandler) ListNexusOperationExecutions(context.Context, *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error) {
	return nil, serviceerror.NewUnimplemented("ListNexusOperationExecutions not implemented")
}

func (h *frontendHandler) CountNexusOperationExecutions(context.Context, *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
	return nil, serviceerror.NewUnimplemented("CountNexusOperationExecutions not implemented")
}

func (h *frontendHandler) RequestCancelNexusOperationExecution(context.Context, *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("RequestCancelNexusOperationExecution not implemented")
}

func (h *frontendHandler) TerminateNexusOperationExecution(context.Context, *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("TerminateNexusOperationExecution not implemented")
}

func (h *frontendHandler) DeleteNexusOperationExecution(context.Context, *workflowservice.DeleteNexusOperationExecutionRequest) (*workflowservice.DeleteNexusOperationExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("DeleteNexusOperationExecution not implemented")
}
