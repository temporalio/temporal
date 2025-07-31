// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/genrpcwrappers -service frontend

package frontend

import (
	"context"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/debugtimeout"
)

var (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second * debugtimeout.Multiplier
	// DefaultLongPollTimeout is the long poll default timeout used to make calls
	DefaultLongPollTimeout = time.Minute * 3 * debugtimeout.Multiplier
)

var _ workflowservice.WorkflowServiceClient = (*clientImpl)(nil)

type clientImpl struct {
	timeout         time.Duration
	longPollTimeout time.Duration
	client          workflowservice.WorkflowServiceClient
}

// NewClient creates a new frontend service gRPC client
func NewClient(
	timeout time.Duration,
	longPollTimeout time.Duration,
	client workflowservice.WorkflowServiceClient,
) workflowservice.WorkflowServiceClient {
	return &clientImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		client:          client,
	}
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.longPollTimeout)
}
