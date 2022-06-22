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
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/telemetry"
)

func PersistenceMaxQpsFn(
	maxQps dynamicconfig.IntPropertyFn,
	globalMaxQps dynamicconfig.IntPropertyFn,
) persistenceClient.PersistenceMaxQps {
	return func(...dynamicconfig.FilterOption) int {
		// if globalMaxQps() > 0 {
		// 	// TODO: We have a bootstrap issue to correctly find memberCount.  Membership relies on
		// 	// persistence to bootstrap membership ring, so we cannot have persistence rely on membership
		// 	// as it will cause circular dependency.
		// 	// ringSize, err := membershipMonitor.GetMemberCount(serviceName)
		// 	// if err == nil && ringSize > 0 {
		// 	// 	avgQuota := common.MaxInt(globalMaxQps()/ringSize, 1)
		// 	// 	return common.MinInt(avgQuota, maxQps())
		// 	// }
		// }
		return maxQps()
	}
}

func GrpcServerOptionsProvider(
	logger log.Logger,
	rpcFactory common.RPCFactory,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	tracingInterceptor telemetry.ServerTraceInterceptor,
) []grpc.ServerOption {

	grpcServerOptions, err := rpcFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}

	return append(
		grpcServerOptions,
		grpc.ChainUnaryInterceptor(
			rpc.ServiceErrorInterceptor,
			grpc.UnaryServerInterceptor(tracingInterceptor),
			metrics.NewServerMetricsContextInjectorInterceptor(),
			metrics.NewServerMetricsTrailerPropagatorInterceptor(logger),
			telemetryInterceptor.Intercept,
			rateLimitInterceptor.Intercept,
		),
	)
}
