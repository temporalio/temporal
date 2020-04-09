package host

import (
	"google.golang.org/grpc"

	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
)

// AdminClient is the interface exposed by admin service client
type AdminClient interface {
	adminservice.AdminServiceClient
}

// FrontendClient is the interface exposed by frontend service client
type FrontendClient interface {
	workflowservice.WorkflowServiceClient
}

// HistoryClient is the interface exposed by history service client
type HistoryClient interface {
	historyservice.HistoryServiceClient
}

// NewAdminClient creates a client to temporal admin client
func NewAdminClient(connection *grpc.ClientConn) AdminClient {
	return adminservice.NewAdminServiceClient(connection)
}

// NewFrontendClient creates a client to temporal frontend client
func NewFrontendClient(connection *grpc.ClientConn) workflowservice.WorkflowServiceClient {
	return workflowservice.NewWorkflowServiceClient(connection)
}

// NewHistoryClient creates a client to temporal history service client
func NewHistoryClient(connection *grpc.ClientConn) HistoryClient {
	return historyservice.NewHistoryServiceClient(connection)
}
