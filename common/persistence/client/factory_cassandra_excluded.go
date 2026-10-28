//go:build temporal_minimal && !temporal_with_cassandra

package client

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

// newCassandraDataStoreFactory fails, since Cassandra persistence is excluded from this build.
func newCassandraDataStoreFactory(
	_ config.Cassandra,
	_ resolver.ServiceResolver,
	_ string,
	logger log.Logger,
	_ metrics.Handler,
	_ serialization.Serializer,
) persistence.DataStoreFactory {
	logger.Fatal("cassandra persistence is not compiled into this server; rebuild with the temporal_with_cassandra build tag")
	return nil
}
