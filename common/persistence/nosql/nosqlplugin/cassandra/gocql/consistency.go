package gocql

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
)

// Definition of all Consistency levels
const (
	Any Consistency = iota
	One
	Two
	Three
	Quorum
	All
	LocalQuorum
	EachQuorum
	LocalOne
)

// Definition of all SerialConsistency levels
const (
	Serial SerialConsistency = iota
	LocalSerial
)

func mustConvertConsistency(c Consistency) gocql.Consistency {
	switch c {
	case Any:
		return gocql.Any
	case One:
		return gocql.One
	case Two:
		return gocql.Two
	case Three:
		return gocql.Three
	case Quorum:
		return gocql.Quorum
	case All:
		return gocql.All
	case LocalQuorum:
		return gocql.LocalQuorum
	case EachQuorum:
		return gocql.EachQuorum
	case LocalOne:
		return gocql.LocalOne
	default:
		panic(fmt.Sprintf("Unknown gocql Consistency level: %v", c))
	}
}

// getConsistency returns the gosql.Consistency setting from the configuration for the given store type
func getConsistency(c *config.CassandraStoreConsistency) gocql.Consistency {
	return gocql.ParseConsistency(getConsistencySettings(c).Consistency)
}

// getSerialConsistency returns the gosql.SerialConsistency setting from the configuration for the store
func getSerialConsistency(c *config.CassandraStoreConsistency) (gocql.SerialConsistency, error) {
	res, err := parseSerialConsistency(getConsistencySettings(c).SerialConsistency)
	if err != nil {
		return res, fmt.Errorf("unable to decode cassandra serial consistency: %w", err)
	}
	return res, nil
}

func getConsistencySettings(c *config.CassandraStoreConsistency) *config.CassandraConsistencySettings {
	return ensureStoreConsistencyNotNil(c).Default
}

func ensureStoreConsistencyNotNil(c *config.CassandraStoreConsistency) *config.CassandraStoreConsistency {
	if c == nil {
		c = &config.CassandraStoreConsistency{}
	}
	if c.Default == nil {
		c.Default = &config.CassandraConsistencySettings{}
	}
	if c.Default.Consistency == "" {
		c.Default.Consistency = "LOCAL_QUORUM"
	}
	if c.Default.SerialConsistency == "" {
		c.Default.SerialConsistency = "LOCAL_SERIAL"
	}

	return c
}

func parseSerialConsistency(serialConsistency string) (gocql.SerialConsistency, error) {
	var s gocql.SerialConsistency
	err := s.UnmarshalText([]byte(strings.ToUpper(serialConsistency)))
	return s, err
}
