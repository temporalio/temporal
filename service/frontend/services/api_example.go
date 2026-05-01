//go:build experimental

package services

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// exampleHandler implements the experimental Echo RPC, demonstrating
// experimental_method and experimental_message.
type exampleHandler struct{}

func (exampleHandler) Echo(_ context.Context, req *workflowservice.EchoRequest) (*workflowservice.EchoResponse, error) {
	return &workflowservice.EchoResponse{Payload: req.GetPayload()}, nil
}

// exampleWorkflowWrapper wraps the stable WorkflowServiceServer to intercept
// StartWorkflowExecution, demonstrating how a feature reads experimental_enum_value
// and experimental_field (overlay) in production server code.
type exampleWorkflowWrapper struct {
	workflowservice.WorkflowServiceServer
}

func (w *exampleWorkflowWrapper) StartWorkflowExecution(
	ctx context.Context,
	req *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	// experimental_enum_value: handle the FOO conflict policy.
	if req.GetWorkflowIdConflictPolicy() == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FOO {
		return nil, status.Error(codes.InvalidArgument, "WORKFLOW_ID_CONFLICT_POLICY_FOO is not yet supported")
	}

	// experimental_field (overlay): read foo_text from the request overlay.
	if overlay, ok, err := workflowservice.GetStartWorkflowExecutionRequestOverlay(req); err != nil {
		return nil, err
	} else if ok && overlay.GetFooText() != "" {
		// A real feature would use overlay.GetFooText() to influence behavior,
		// e.g. attach it as a search attribute or pass it to history.
		_ = overlay.GetFooText()
	}

	return w.WorkflowServiceServer.StartWorkflowExecution(ctx, req)
}

func init() {
	register("example", variant{registerWorkflow: func(server *grpc.Server, workflow workflowservice.WorkflowServiceServer) {
		wrapped := &exampleWorkflowWrapper{workflow}
		registerServiceOverlay(server, workflowservice.WorkflowService_ServiceDesc, wrapped, workflowservice.ExampleWorkflowService_ServiceDesc, exampleHandler{})
	}})
}
