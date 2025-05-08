package client

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resolver"
)

type (

	// AbstractDataStoreFactory creates a DataStoreFactory, can be used to implement custom datastore support outside
	// of the Temporal core.
	AbstractDataStoreFactory interface {
		NewFactory(
			cfg config.CustomDatastoreConfig,
			r resolver.ServiceResolver,
			clusterName string,
			logger log.Logger,
			metricsHandler metrics.Handler,
		) persistence.DataStoreFactory
	}
)
