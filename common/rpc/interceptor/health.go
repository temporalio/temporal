package interceptor

import (
	"context"
	"strings"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
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
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if i.isNotHealthy(info.FullMethod) {
		return nil, notHealthyErr
	}
	return handler(ctx, req)
}

// InterceptNexus is a no-op as nexus APIs are considered internal
func (i *HealthInterceptor) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	return next(ctx, in)
}

func (i *HealthInterceptor) isNotHealthy(methodName string) bool {
	if i.healthy.Load() {
		return false
	}

	// only enforce health check on WorkflowService and OperatorService
	return strings.HasPrefix(methodName, api.WorkflowServicePrefix) ||
		strings.HasPrefix(methodName, api.OperatorServicePrefix)
}

func (i *HealthInterceptor) SetHealthy(healthy bool) {
	i.healthy.Store(healthy)
}
