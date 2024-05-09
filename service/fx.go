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

package service

import (
	"sync/atomic"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/telemetry"
)

type (
	PersistenceLazyLoadedServiceResolver struct {
		*atomic.Value // value type is membership.ServiceResolver
	}

	PersistenceRateLimitingParams struct {
		fx.Out

		PersistenceMaxQps                  persistenceClient.PersistenceMaxQps
		PersistenceNamespaceMaxQps         persistenceClient.PersistenceNamespaceMaxQps
		PersistencePerShardNamespaceMaxQPS persistenceClient.PersistencePerShardNamespaceMaxQPS
		OperatorRPSRatio                   persistenceClient.OperatorRPSRatio
		PersistenceBurstRatio              persistenceClient.PersistenceBurstRatio
		DynamicRateLimitingParams          persistenceClient.DynamicRateLimitingParams
	}

	GrpcServerOptionsParams struct {
		fx.In

		Logger                 log.Logger
		RpcFactory             common.RPCFactory
		RetryableInterceptor   *interceptor.RetryableInterceptor
		TelemetryInterceptor   *interceptor.TelemetryInterceptor
		RateLimitInterceptor   *interceptor.RateLimitInterceptor
		TracingInterceptor     telemetry.ServerTraceInterceptor
		AdditionalInterceptors []grpc.UnaryServerInterceptor `optional:"true"`
	}
)

var PersistenceLazyLoadedServiceResolverModule = fx.Options(
	fx.Provide(func() PersistenceLazyLoadedServiceResolver {
		return PersistenceLazyLoadedServiceResolver{
			Value: &atomic.Value{},
		}
	}),
	fx.Invoke(initPersistenceLazyLoadedServiceResolver),
)

func initPersistenceLazyLoadedServiceResolver(
	serviceName primitives.ServiceName,
	logger log.SnTaggedLogger,
	serviceResolver membership.ServiceResolver,
	lazyLoadedServiceResolver PersistenceLazyLoadedServiceResolver,
) {
	lazyLoadedServiceResolver.Store(serviceResolver)
	logger.Info("Initialized service resolver for persistence rate limiting", tag.Service(serviceName))
}

func (p PersistenceLazyLoadedServiceResolver) AvailableMemberCount() int {
	if value := p.Load(); value != nil {
		return value.(membership.ServiceResolver).AvailableMemberCount()
	}
	return 0
}

func NewPersistenceRateLimitingParams(
	maxQps dynamicconfig.IntPropertyFn,
	globalMaxQps dynamicconfig.IntPropertyFn,
	namespaceMaxQps dynamicconfig.IntPropertyFnWithNamespaceFilter,
	globalNamespaceMaxQps dynamicconfig.IntPropertyFnWithNamespaceFilter,
	perShardNamespaceMaxQps dynamicconfig.IntPropertyFnWithNamespaceFilter,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
	burstRatio dynamicconfig.FloatPropertyFn,
	dynamicRateLimitingParams dynamicconfig.MapPropertyFn,
	lazyLoadedServiceResolver PersistenceLazyLoadedServiceResolver,
	logger log.Logger,
) PersistenceRateLimitingParams {
	hostCalculator := calculator.NewLoggedCalculator(
		calculator.ClusterAwareQuotaCalculator{
			MemberCounter:    lazyLoadedServiceResolver,
			PerInstanceQuota: maxQps,
			GlobalQuota:      globalMaxQps,
		},
		log.With(logger, tag.ComponentPersistence, tag.ScopeHost),
	)
	namespaceCalculator := calculator.NewLoggedNamespaceCalculator(
		calculator.ClusterAwareNamespaceQuotaCalculator{
			MemberCounter:    lazyLoadedServiceResolver,
			PerInstanceQuota: namespaceMaxQps,
			GlobalQuota:      globalNamespaceMaxQps,
		},
		log.With(logger, tag.ComponentPersistence, tag.ScopeNamespace),
	)
	return PersistenceRateLimitingParams{
		PersistenceMaxQps: func() int {
			return int(hostCalculator.GetQuota())
		},
		PersistenceNamespaceMaxQps: func(namespace string) int {
			return int(namespaceCalculator.GetQuota(namespace))
		},
		PersistencePerShardNamespaceMaxQPS: persistenceClient.PersistencePerShardNamespaceMaxQPS(perShardNamespaceMaxQps),
		OperatorRPSRatio:                   persistenceClient.OperatorRPSRatio(operatorRPSRatio),
		PersistenceBurstRatio:              persistenceClient.PersistenceBurstRatio(burstRatio),
		DynamicRateLimitingParams:          persistenceClient.DynamicRateLimitingParams(dynamicRateLimitingParams),
	}
}

func GrpcServerOptionsProvider(
	params GrpcServerOptionsParams,
) []grpc.ServerOption {

	grpcServerOptions, err := params.RpcFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		params.Logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}

	return append(
		grpcServerOptions,
		grpc.ChainUnaryInterceptor(getUnaryInterceptors(params)...),
		grpc.ChainStreamInterceptor(params.TelemetryInterceptor.StreamIntercept),
	)
}

func getUnaryInterceptors(params GrpcServerOptionsParams) []grpc.UnaryServerInterceptor {
	interceptors := []grpc.UnaryServerInterceptor{
		rpc.NewServiceErrorInterceptor(params.Logger),
		grpc.UnaryServerInterceptor(params.TracingInterceptor),
		metrics.NewServerMetricsContextInjectorInterceptor(),
		metrics.NewServerMetricsTrailerPropagatorInterceptor(params.Logger),
		params.TelemetryInterceptor.UnaryIntercept,
	}

	interceptors = append(interceptors, params.AdditionalInterceptors...)

	return append(
		interceptors,
		params.RateLimitInterceptor.Intercept,
		params.RetryableInterceptor.Intercept)
}
