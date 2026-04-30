//go:build experimental

package services

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

type exampleHandler struct{}

func (exampleHandler) Echo(_ context.Context, req *workflowservice.EchoRequest) (*workflowservice.EchoResponse, error) {
	return &workflowservice.EchoResponse{Payload: req.GetPayload()}, nil
}

func init() {
	register("example", variant{registerWorkflow: func(server *grpc.Server, workflow workflowservice.WorkflowServiceServer) {
		registerServiceOverlay(server, workflowservice.WorkflowService_ServiceDesc, workflow, workflowservice.ExampleWorkflowService_ServiceDesc, exampleHandler{})
	}})
}
