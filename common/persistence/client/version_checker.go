package client

import (
	"fmt"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
)

// VerifyCompatibleVersion checks that the schema versions of the configured persistence stores are compatible with this server.
func VerifyCompatibleVersion(
	cfg config.Persistence,
	persistenceServiceResolver resolver.ServiceResolver,
	logger log.Logger,
) error {
	// cassandra schema version validation
	if err := verifyCassandraCompatibleVersion(cfg, persistenceServiceResolver, logger); err != nil {
		return fmt.Errorf("cassandra schema version compatibility check failed: %w", err)
	}
	// sql schema version validation
	if err := sql.VerifyCompatibleVersion(cfg, persistenceServiceResolver, logger); err != nil {
		return fmt.Errorf("sql schema version compatibility check failed: %w", err)
	}
	return nil
}
