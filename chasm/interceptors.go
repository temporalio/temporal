package chasm

import (
	"context"
	"strings"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
)

const chasmRequestPrefix = "/temporal.server.chasm"

// ChasmRequestEngineInterceptor Interceptor that intercepts RPC requests,
// detects CHASM-specific calls and does additional boilerplate processing before
// handing off. Visibility is injected separately with
// ChasmRequestVisibilityInterceptor.
type ChasmRequestEngineInterceptor struct {
	engine         Engine
	logger         log.Logger
	metricsHandler metrics.Handler
}

var _ grpc.UnaryServerInterceptor = (*ChasmRequestEngineInterceptor)(nil).Intercept

func (i *ChasmRequestEngineInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, retError error) {
	if strings.HasPrefix(info.FullMethod, chasmRequestPrefix) {
		defer metrics.CapturePanic(i.logger, i.metricsHandler, &retError)
	}

	ctx = NewEngineContext(ctx, i.engine)
	return handler(ctx, req)
}

func ChasmRequestEngineInterceptorProvider(
	engine Engine,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ChasmRequestEngineInterceptor {
	return &ChasmRequestEngineInterceptor{
		engine:         engine,
		logger:         logger,
		metricsHandler: metricsHandler,
	}
}

// ChasmRequestVisibilityInterceptor intercepts RPC requests and adds the CHASM
// VisibilityManager to their context.
type ChasmRequestVisibilityInterceptor struct {
	visibilityMgr VisibilityManager
}

var _ grpc.UnaryServerInterceptor = (*ChasmRequestVisibilityInterceptor)(nil).Intercept

func (i *ChasmRequestVisibilityInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, retError error) {
	ctx = NewVisibilityManagerContext(ctx, i.visibilityMgr)
	return handler(ctx, req)
}

func ChasmRequestVisibilityInterceptorProvider(visibilityMgr VisibilityManager) *ChasmRequestVisibilityInterceptor {
	return &ChasmRequestVisibilityInterceptor{
		visibilityMgr: visibilityMgr,
	}
}
