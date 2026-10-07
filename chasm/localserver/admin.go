package localserver

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
)

// Admin serves the AdminService methods a host needs to run, on the local server, a workflow run
// that a server owns: importing the run's history, reading the history the run added locally, and
// deleting the run when the server takes it back.
type Admin struct {
	s *Server
}

func (s *Server) Admin() Admin {
	return Admin{s: s}
}

func (a Admin) ImportWorkflowExecution(
	context.Context,
	*adminservice.ImportWorkflowExecutionRequest,
) (*adminservice.ImportWorkflowExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("ImportWorkflowExecution")
}

func (a Admin) GetWorkflowExecutionRawHistoryV2(
	context.Context,
	*adminservice.GetWorkflowExecutionRawHistoryV2Request,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	return nil, serviceerror.NewUnimplemented("GetWorkflowExecutionRawHistoryV2")
}

func (a Admin) DeleteWorkflowExecution(
	context.Context,
	*adminservice.DeleteWorkflowExecutionRequest,
) (*adminservice.DeleteWorkflowExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("DeleteWorkflowExecution")
}
