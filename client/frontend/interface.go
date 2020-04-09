package frontend

import (
	"go.temporal.io/temporal-proto/workflowservice"
)

// Client is the interface exposed by frontend service client
type Client interface {
	workflowservice.WorkflowServiceClient
}
