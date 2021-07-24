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
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/cassandra"
	"go.temporal.io/server/common/persistence/visibility/sql"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

// NewVisibilityManager returns a new visibility manager for standard visibility.
func NewVisibilityManager(
	cfg config.Persistence,
	persistenceMaxQPS dynamicconfig.IntPropertyFn,
	metricsClient metrics.Client,
	r resolver.ServiceResolver,
	logger log.Logger,
) (visibility.VisibilityManager, error) {
	if cfg.VisibilityConfig == nil {
		return nil, nil
	}

	// If standard visibility is not configured.
	if cfg.VisibilityStore == "" {
		return nil, nil
	}

	visibilityStoreCfg := cfg.DataStores[cfg.VisibilityStore]

	var (
		store visibility.VisibilityStore
		err   error
	)
	switch {
	case visibilityStoreCfg.Cassandra != nil:
		store, err = cassandra.NewVisibilityStore(*visibilityStoreCfg.Cassandra, r, logger)
	case visibilityStoreCfg.SQL != nil:
		store, err = sql.NewSQLVisibilityStore(*visibilityStoreCfg.SQL, r, logger)
	}

	if err != nil {
		return nil, err
	}

	if store == nil {
		logger.Fatal("invalid config: one of cassandra or sql params must be specified for visibility store")
		return nil, nil
	}

	// This visibility manager is used by DB visibility only and doesn't care about search attributes.
	result := visibility.NewVisibilityManagerImpl(store, searchattribute.NewSystemProvider(), "", logger)

	if persistenceMaxQPS != nil && persistenceMaxQPS() > 0 {
		rateLimiter := quotas.NewDefaultOutgoingDynamicRateLimiter(
			func() float64 { return float64(persistenceMaxQPS()) },
		)
		result = visibility.NewVisibilityPersistenceRateLimitedClient(result, rateLimiter, logger)
	}

	if cfg.VisibilityConfig.EnableSampling() {
		result = visibility.NewVisibilitySamplingClient(result, cfg.VisibilityConfig, metricsClient, logger)
	}
	if metricsClient != nil {
		result = visibility.NewVisibilityPersistenceMetricsClient(result, metricsClient, logger)
	}

	return result, nil
}
