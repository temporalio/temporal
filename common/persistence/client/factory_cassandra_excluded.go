//go:build temporal_minimal && !temporal_with_cassandra

package client

import (
	"fmt"
	"maps"
	"slices"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

// newCassandraDataStoreFactory fails, since Cassandra persistence is excluded from this build.
// It is unreachable on server startup, since verifyCassandraCompatibleVersion rejects Cassandra datastores first.
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

// verifyCassandraCompatibleVersion fails if any datastore uses Cassandra, since Cassandra persistence is excluded from this build.
func verifyCassandraCompatibleVersion(cfg config.Persistence, _ resolver.ServiceResolver, _ log.Logger) error {
	for _, name := range slices.Sorted(maps.Keys(cfg.DataStores)) {
		if cfg.DataStores[name].Cassandra != nil {
			return fmt.Errorf("datastore %q: cassandra persistence is not compiled into this server; remove this config or rebuild with the temporal_with_cassandra build tag", name)
		}
	}
	return nil
}
