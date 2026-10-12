//go:build !temporal_minimal || temporal_with_cassandra

package client

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

func newCassandraDataStoreFactory(
	cfg config.Cassandra,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
	serializer serialization.Serializer,
) persistence.DataStoreFactory {
	return cassandra.NewFactory(cfg, r, clusterName, logger, metricsHandler, serializer)
}
