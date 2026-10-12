//go:build temporal_minimal && !temporal_with_cassandra

package temporal

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/resolver"
)

// Cassandra is compiled out of this build; config validation rejects Cassandra data stores.
func verifyCassandraCompatibleVersion(
	config.Persistence,
	resolver.ServiceResolver,
	log.Logger,
) error {
	return nil
}
