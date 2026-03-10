package chasm

import (
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
)

// ChasmEngineInterceptor Interceptor that intercepts RPC requests,
// detects CHASM-specific calls and does additional boilerplate processing before
// handing off. Visibility is injected separately with
// ChasmVisibilityInterceptor.
type ChasmEngineInterceptor struct {
	engine         Engine
	logger         log.SnTaggedLogger
	metricsHandler metrics.Handler
}

func (i *ChasmEngineInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, retError error) {
	// Capture panics for any handler method, not just CHASM-specific ones. This could have gone into a separate
	// interceptor, but having it here avoids the overhead of adding another layer to the interceptor chain.
	defer metrics.CapturePanic(i.logger, i.metricsHandler, &retError)

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
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, retError error) {
	ctx = NewVisibilityManagerContext(ctx, i.visibilityMgr)
	return handler(ctx, req)
}

func ChasmVisibilityInterceptorProvider(visibilityMgr VisibilityManager) *ChasmVisibilityInterceptor {
	return &ChasmVisibilityInterceptor{
		visibilityMgr: visibilityMgr,
	}
}
