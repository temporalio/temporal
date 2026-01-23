package chasm

import (
	"context"
	"strings"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
)

const chasmRequestPrefix = "/temporal.server.chasm"

// ChasmEngineInterceptor Interceptor that intercepts RPC requests,
// detects CHASM-specific calls and does additional boilerplate processing before
// handing off. Visibility is injected separately with
// ChasmVisibilityInterceptor.
type ChasmEngineInterceptor struct {
	engine         Engine
	logger         log.Logger
	metricsHandler metrics.Handler
}

func (i *ChasmEngineInterceptor) Intercept(
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

func ChasmEngineInterceptorProvider(
	engine Engine,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ChasmEngineInterceptor {
	return &ChasmEngineInterceptor{
		engine:         engine,
		logger:         logger,
		metricsHandler: metricsHandler,
	}
}

// ChasmVisibilityInterceptor intercepts RPC requests and adds the CHASM
// VisibilityManager to their context.
type ChasmVisibilityInterceptor struct {
	visibilityMgr VisibilityManager
}

func (i *ChasmVisibilityInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, retError error) {
	ctx = NewVisibilityManagerContext(ctx, i.visibilityMgr)
	return handler(ctx, req)
}

func ChasmVisibilityInterceptorProvider(visibilityMgr VisibilityManager) *ChasmVisibilityInterceptor {
	return &ChasmVisibilityInterceptor{
		visibilityMgr: visibilityMgr,
	}
}
