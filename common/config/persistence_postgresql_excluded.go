//go:build temporal_minimal && !temporal_with_postgresql

package config

import (
	"errors"
)

func validatePostgreSQL(pluginName string) error {
	if pluginName == "postgres12" || pluginName == "postgres12_pgx" {
		return errors.New("postgresql: PostgreSQL persistence is not compiled into this server; remove this config or rebuild with the temporal_with_postgresql build tag")
	}
	return nil
}
