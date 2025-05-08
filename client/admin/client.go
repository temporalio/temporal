// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/genrpcwrappers -service admin

package admin

import (
	"context"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/debug"
	"google.golang.org/grpc"
)

var _ adminservice.AdminServiceClient = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second * debug.TimeoutMultiplier
	// DefaultLargeTimeout is the default timeout used to make calls
	DefaultLargeTimeout = time.Minute * debug.TimeoutMultiplier
)

type clientImpl struct {
	timeout      time.Duration
	largeTimeout time.Duration
	client       adminservice.AdminServiceClient
}

// NewClient creates a new admin service gRPC client
func NewClient(
	timeout time.Duration,
	largeTimeout time.Duration,
	client adminservice.AdminServiceClient,
) adminservice.AdminServiceClient {
	return &clientImpl{
		timeout:      timeout,
		largeTimeout: largeTimeout,
		client:       client,
	}
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createContextWithLargeTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.largeTimeout)
	}
	return context.WithTimeout(parent, c.largeTimeout)
}

func (c *clientImpl) StreamWorkflowReplicationMessages(
	ctx context.Context,
	opts ...grpc.CallOption,
) (adminservice.AdminService_StreamWorkflowReplicationMessagesClient, error) {
	// do not use createContext function, let caller manage stream API lifecycle
	return c.client.StreamWorkflowReplicationMessages(ctx, opts...)
}
