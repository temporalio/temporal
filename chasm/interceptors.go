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
	// TODO https://temporalio.atlassian.net/browse/ACT-69 is the work to have existing RPC calls utilize this
	// interceptor so we can remove this check.
	// There's also additional boiler plate code we can reduce, but needs thinking:
	// 1. Need to await the engine ready, which ultimately should await handler startWG.
	// 2. How to best handle other actions like getting namespace, shard ctx, etc. since req is of any type. See if
	// need a base message proto and cast it first to extract boiler plate.
	if strings.HasPrefix(info.FullMethod, chasmRequestPrefix) {
		defer metrics.CapturePanic(i.logger, i.metricsHandler, &retError)

		i.logger.Error("Inside ChasmRequestInterceptor.Intercept ")

		ctx = NewEngineContext(ctx, i.engine)
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
