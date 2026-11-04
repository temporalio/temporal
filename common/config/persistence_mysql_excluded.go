//go:build temporal_minimal && !temporal_with_mysql

package config

import (
	"errors"
)

func validateMySQL(pluginName string) error {
	if pluginName == "mysql8" {
		return errors.New("mysql: MySQL persistence is not compiled into this server; remove this config or rebuild with the temporal_with_mysql build tag")
	}
	return nil
}
