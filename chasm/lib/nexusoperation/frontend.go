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
	IsStandaloneNexusOperationEnabled(namespaceName string) bool
}

var ErrStandaloneNexusOperationDisabled = serviceerror.NewUnimplemented("Standalone Nexus operation is disabled")

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

// IsStandaloneNexusOperationEnabled checks if standalone Nexus operations are enabled for the given namespace.
func (h *frontendHandler) IsStandaloneNexusOperationEnabled(namespaceName string) bool {
	return h.config.Enabled(namespaceName)
}

func (h *frontendHandler) StartNexusOperationExecution(_ context.Context, req *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("StartNexusOperationExecution not implemented")
}

func (h *frontendHandler) DescribeNexusOperationExecution(_ context.Context, req *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("DescribeNexusOperationExecution not implemented")
}

func (h *frontendHandler) PollNexusOperationExecution(_ context.Context, req *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("PollNexusOperationExecution not implemented")
}

func (h *frontendHandler) ListNexusOperationExecutions(_ context.Context, req *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("ListNexusOperationExecutions not implemented")
}

func (h *frontendHandler) CountNexusOperationExecutions(_ context.Context, req *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("CountNexusOperationExecutions not implemented")
}

func (h *frontendHandler) RequestCancelNexusOperationExecution(_ context.Context, req *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("RequestCancelNexusOperationExecution not implemented")
}

func (h *frontendHandler) TerminateNexusOperationExecution(_ context.Context, req *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("TerminateNexusOperationExecution not implemented")
}

func (h *frontendHandler) DeleteNexusOperationExecution(_ context.Context, req *workflowservice.DeleteNexusOperationExecutionRequest) (*workflowservice.DeleteNexusOperationExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}
	return nil, serviceerror.NewUnimplemented("DeleteNexusOperationExecution not implemented")
}
