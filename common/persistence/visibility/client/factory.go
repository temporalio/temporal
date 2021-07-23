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
