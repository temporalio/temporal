//go:build temporal_minimal && !temporal_with_cassandra

package config

import (
	"errors"
)

func (c *Cassandra) validate() error {
	return errors.New("cassandra persistence is excluded from this build, see build tag temporal_with_cassandra")
}
