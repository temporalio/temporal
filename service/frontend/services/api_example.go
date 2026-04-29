//go:build experimental

package services

import (
	"context"

	expexample "github.com/temporalio/api-go/experimental/example/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

type exampleHandler struct{}

func (exampleHandler) Echo(_ context.Context, req *expexample.EchoRequest) (*expexample.EchoResponse, error) {
	return &expexample.EchoResponse{Payload: req.GetPayload()}, nil
}

func init() {
	register("example", variant{registerWorkflow: func(server *grpc.Server, workflow workflowservice.WorkflowServiceServer) {
		registerServiceOverlay(server, workflowservice.WorkflowService_ServiceDesc, workflow, expexample.WorkflowService_ServiceDesc, exampleHandler{})
	}})
}
