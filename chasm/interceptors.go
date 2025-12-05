package chasm

import (
	"context"
	"strings"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
)

const chasmRequestPrefix = "/temporal.server.chasm"

// ChasmRequestInterceptor Interceptor that intercepts RPC requests, detects Chasm-specific calls and does additional
// boilerplate processing before handing off.
type ChasmRequestInterceptor struct {
	engine         Engine
	visibilityMgr  VisibilityManager
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
	if strings.HasPrefix(info.FullMethod, chasmRequestPrefix) {
		defer metrics.CapturePanic(i.logger, i.metricsHandler, &retError)
	}

	ctx = NewEngineContext(ctx, i.engine)
	ctx = NewVisibilityManagerContext(ctx, i.visibilityMgr)

	return handler(ctx, req)
}

func ChasmRequestInterceptorProvider(
	engine Engine,
	visibilityMgr VisibilityManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ChasmRequestInterceptor {
	return &ChasmRequestInterceptor{
		engine:         engine,
		visibilityMgr:  visibilityMgr,
		logger:         logger,
		metricsHandler: metricsHandler,
	}
}
