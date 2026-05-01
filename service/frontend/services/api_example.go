package services

import (
	"context"

	expenums "github.com/temporalio/api-go/experimental/enums/v1"
	expworkflowservice "github.com/temporalio/api-go/experimental/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// exampleHandler implements the experimental Echo RPC (experimental_method,
// experimental_message). Types come from the experimental module — no build
// tags required.
type exampleHandler struct{}

func (exampleHandler) Echo(_ context.Context, req *expworkflowservice.EchoRequest) (*expworkflowservice.EchoResponse, error) {
	return &expworkflowservice.EchoResponse{Payload: req.GetPayload()}, nil
}

// exampleWorkflowWrapper intercepts stable RPCs to add experimental behaviour.
// Embedding WorkflowServiceServer means every other method passes through.
type exampleWorkflowWrapper struct {
	workflowservice.WorkflowServiceServer
}

func (w *exampleWorkflowWrapper) StartWorkflowExecution(
	ctx context.Context,
	req *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	// experimental_enum_value: react to the FOO conflict policy.
	if req.GetWorkflowIdConflictPolicy() == expenums.WORKFLOW_ID_CONFLICT_POLICY_FOO {
		return nil, status.Error(codes.InvalidArgument, "WORKFLOW_ID_CONFLICT_POLICY_FOO is not yet supported")
	}

	// experimental_field (overlay): read foo_text from the request overlay.
	if overlay, ok, err := expworkflowservice.GetStartWorkflowExecutionRequestOverlay(req); err != nil {
		return nil, err
	} else if ok {
		_ = overlay.GetFooText() // a real feature would use this
	}

	return w.WorkflowServiceServer.StartWorkflowExecution(ctx, req)
}

func init() {
	register("example", variant{registerWorkflow: func(server *grpc.Server, workflow workflowservice.WorkflowServiceServer) {
		wrapped := &exampleWorkflowWrapper{workflow}
		registerServiceOverlay(server, workflowservice.WorkflowService_ServiceDesc, wrapped, expworkflowservice.ExampleWorkflowService_ServiceDesc, exampleHandler{})
	}})
}
