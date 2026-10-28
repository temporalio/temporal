//go:build temporal_minimal && !temporal_with_cassandra

package temporal

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/resolver"
)

// verifyCassandraCompatibleVersion is a no-op, since config validation rejects Cassandra
// datastores when Cassandra persistence is excluded from this build.
func verifyCassandraCompatibleVersion(config.Persistence, resolver.ServiceResolver, log.Logger) error {
	return nil
}
