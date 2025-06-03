package interceptor

import (
	"context"
	"strings"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"google.golang.org/grpc"
)

// HealthInterceptor rejects frontend requests if the host has not been marked as healthy.
type (
	HealthInterceptor struct {
		healthy atomic.Bool
	}
)

var _ grpc.UnaryServerInterceptor = (*HealthInterceptor)(nil).Intercept

var notHealthyErr = serviceerror.NewUnavailable("Frontend is not healthy yet")

// NewHealthInterceptor returns a new HealthInterceptor. It starts with state not healthy.
func NewHealthInterceptor() *HealthInterceptor {
	return &HealthInterceptor{}
}

func (i *HealthInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	// only enforce health check on WorkflowService and OperatorService
	if strings.HasPrefix(info.FullMethod, api.WorkflowServicePrefix) ||
		strings.HasPrefix(info.FullMethod, api.OperatorServicePrefix) {
		if !i.healthy.Load() {
			return nil, notHealthyErr
		}
	}
	return handler(ctx, req)
}

func (i *HealthInterceptor) SetHealthy(healthy bool) {
	i.healthy.Store(healthy)
}
