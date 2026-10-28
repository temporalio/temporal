//go:build !temporal_minimal || temporal_with_cassandra

package temporal

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/resolver"
)

func verifyCassandraCompatibleVersion(
	cfg config.Persistence,
	persistenceServiceResolver resolver.ServiceResolver,
	logger log.Logger,
) error {
	return cassandra.VerifyCompatibleVersion(cfg, persistenceServiceResolver, logger)
}
