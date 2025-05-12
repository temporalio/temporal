package frontend

import (
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
)

const (
	WorkflowServiceName = "temporal.api.workflowservice.v1.WorkflowService"
	OperatorServiceName = "temporal.api.operatorservice.v1.OperatorService"
	AdminServiceName    = "temporal.api.adminservice.v1.AdminService"
)

type (
	// Handler is interface wrapping frontend workflow handler
	Handler interface {
		workflowservice.WorkflowServiceServer
		GetConfig() *Config
		Start()
		Stop()
	}

	// OperatorHandler is interface wrapping frontend workflow handler
	OperatorHandler interface {
		operatorservice.OperatorServiceServer
		Start()
		Stop()
	}
)
