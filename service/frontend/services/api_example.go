//go:build experimental

package services

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

// exampleHandler implements the experimental Echo RPC, demonstrating
// experimental_method and experimental_message.
//
// experimental_enum_value and experimental_field (overlay) are handled in
// workflow_handler_experimental.go via the build-tag-gated function pattern:
// the stable workflow_handler.go calls startWorkflowExperimentalCheck(), which
// is a no-op in stable builds and has real logic in experimental builds.
// No interface, no registration, no global variable — just two files.
type exampleHandler struct{}

func (exampleHandler) Echo(_ context.Context, req *workflowservice.EchoRequest) (*workflowservice.EchoResponse, error) {
	return &workflowservice.EchoResponse{Payload: req.GetPayload()}, nil
}

func init() {
	register("example", variant{registerWorkflow: func(server *grpc.Server, workflow workflowservice.WorkflowServiceServer) {
		registerServiceOverlay(server, workflowservice.WorkflowService_ServiceDesc, workflow, workflowservice.ExampleWorkflowService_ServiceDesc, exampleHandler{})
	}})
}
