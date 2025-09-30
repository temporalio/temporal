package chasm

import (
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
)

// Interceptor that intercepts RPC requests, detects Chasm-specific calls and does additional processing before handing off.
type ChasmRequestInterceptor struct {
	engine         Engine
	logger         log.Logger
	metricsHandler metrics.Handler
}

var _ grpc.UnaryServerInterceptor = (*ChasmRequestInterceptor)(nil).Intercept

func (i *ChasmRequestInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, retError error) {
	if _, ok := ChasmRequestMethods[info.FullMethod]; ok {
		defer metrics.CapturePanic(i.logger, i.metricsHandler, &retError)

		// TODO need to await the engine ready, which ultimately should await handler startWG

		ctx = NewEngineContext(ctx, i.engine)

		// TODO need to extract namespace from request. We should implement an interface for requests so we can cast it. Also need to grab shardContext.
	}

	return handler(ctx, req)
}

func ChasmRequestInterceptorProvider(engine Engine, logger log.Logger, metricsHandler metrics.Handler) *ChasmRequestInterceptor {
	return &ChasmRequestInterceptor{
		engine:         engine,
		logger:         logger,
		metricsHandler: metricsHandler,
	}
}
