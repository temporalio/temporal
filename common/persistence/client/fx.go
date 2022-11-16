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

package client

import (
	"go.uber.org/fx"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
)

type (
	PersistenceMaxQps          dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQps dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnablePriorityRateLimiting dynamicconfig.BoolPropertyFn
	ClusterName                string

	NewFactoryParams struct {
		fx.In

		DataStoreFactory           DataStoreFactory
		Cfg                        *config.Persistence
		PersistenceMaxQPS          PersistenceMaxQps
		PersistenceNamespaceMaxQPS PersistenceNamespaceMaxQps
		EnablePriorityRateLimiting EnablePriorityRateLimiting
		ClusterName                ClusterName
		MetricsHandler             metrics.MetricsHandler
		Logger                     log.Logger
	}

	FactoryProviderFn func(NewFactoryParams) Factory
)

var Module = fx.Options(
	BeanModule,
	fx.Provide(ClusterNameProvider),
	fx.Provide(DataStoreFactoryProvider),
)

func ClusterNameProvider(config *cluster.Config) ClusterName {
	return ClusterName(config.CurrentClusterName)
}

func FactoryProvider(
	params NewFactoryParams,
) Factory {
	var requestRatelimiter quotas.RequestRateLimiter
	if params.PersistenceMaxQPS != nil && params.PersistenceMaxQPS() > 0 {
		if params.EnablePriorityRateLimiting != nil && params.EnablePriorityRateLimiting() {
			requestRatelimiter = NewPriorityRateLimiter(
				params.PersistenceNamespaceMaxQPS,
				params.PersistenceMaxQPS,
				RequestPriorityFn,
			)
		} else {
			requestRatelimiter = NewNoopPriorityRateLimiter(params.PersistenceMaxQPS)
		}
	}

	return NewFactory(
		params.DataStoreFactory,
		params.Cfg,
		requestRatelimiter,
		serialization.NewSerializer(),
		string(params.ClusterName),
		params.MetricsHandler,
		params.Logger,
	)
}
