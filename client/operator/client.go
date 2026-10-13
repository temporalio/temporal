// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/genrpcwrappers -service operator

package operator

import (
	"context"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/server/common/debug"
)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

var _ operatorservice.OperatorServiceClient = (*clientImpl)(nil)

type clientImpl struct {
	timeout time.Duration
	client  operatorservice.OperatorServiceClient
}

// NewClient creates a new operator service gRPC client
func NewClient(
	timeout time.Duration,
	client operatorservice.OperatorServiceClient,
) operatorservice.OperatorServiceClient {
	return &clientImpl{
		timeout: timeout,
		client:  client,
	}
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}
