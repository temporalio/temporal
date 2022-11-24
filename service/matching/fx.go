// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"

	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/matching/configs"
)

var Module = fx.Options(
	fx.Provide(dynamicconfig.NewCollection),
	fx.Provide(NewConfig),
	fx.Provide(PersistenceRateLimitingParamsProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(HandlerProvider),
	fx.Provide(service.GrpcServerOptionsProvider),
	resource.Module,
	fx.Provide(ServiceResolverProvider),
	fx.Provide(NewService),
	fx.Invoke(ServiceLifetimeHooks),
)

func RetryableInterceptorProvider() *interceptor.RetryableInterceptor {
	return interceptor.NewRetryableInterceptor(
		common.CreateMatchingHandlerRetryPolicy(),
		common.IsServiceHandlerRetryableError,
	)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.MetricsHandler,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsHandler,
		logger,
	)
}

func ThrottledLoggerRpsFnProvider(serviceConfig *Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
}

func RateLimitInterceptorProvider(
	serviceConfig *Config,
) *interceptor.RateLimitInterceptor {
	return interceptor.NewRateLimitInterceptor(
		configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }),
		map[string]int{},
	)
}

// This function is the same between services but uses different config sources.
// if-case comes from resourceImpl.New.
func PersistenceRateLimitingParamsProvider(
	serviceConfig *Config,
) service.PersistenceRateLimitingParams {
	return service.NewPersistenceRateLimitingParams(
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.PersistenceNamespaceMaxQPS,
		serviceConfig.EnablePersistencePriorityRateLimiting,
	)
}

func ServiceResolverProvider(membershipMonitor membership.Monitor) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(primitives.MatchingService)
}

func HandlerProvider(
	config *Config,
	logger log.SnTaggedLogger,
	throttledLogger log.ThrottledLogger,
	taskManager persistence.TaskManager,
	historyClient historyservice.HistoryServiceClient,
	matchingRawClient resource.MatchingRawClient,
	matchingServiceResolver membership.ServiceResolver,
	metricsHandler metrics.MetricsHandler,
	namespaceRegistry namespace.Registry,
	clusterMetadata cluster.Metadata,
) *Handler {
	return NewHandler(
		config,
		logger,
		throttledLogger,
		taskManager,
		historyClient,
		matchingRawClient,
		matchingServiceResolver,
		metricsHandler,
		namespaceRegistry,
		clusterMetadata,
	)
}

func ServiceLifetimeHooks(
	lc fx.Lifecycle,
	svcStoppedCh chan struct{},
	svc *Service,
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				go func(svc common.Daemon, svcStoppedCh chan<- struct{}) {
					// Start is blocked until Stop() is called.
					svc.Start()
					close(svcStoppedCh)
				}(svc, svcStoppedCh)

				return nil
			},
			OnStop: func(ctx context.Context) error {
				svc.Stop()
				return nil
			},
		},
	)

}
