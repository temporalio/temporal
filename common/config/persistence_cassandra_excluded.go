//go:build temporal_minimal && !temporal_with_cassandra

package config

import (
	"errors"
)

// validateCassandra fails if cfg is set, since Cassandra persistence is excluded from this build.
func validateCassandra(cfg *Cassandra) error {
	if cfg != nil {
		return errors.New("cassandra: Cassandra persistence is not compiled into this server; remove this config or rebuild with the temporal_with_cassandra build tag")
	}
	return nil
}
