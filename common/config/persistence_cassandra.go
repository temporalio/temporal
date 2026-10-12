//go:build !temporal_minimal || temporal_with_cassandra

package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
)

// GetConsistency returns the gosql.Consistency setting from the configuration for the given store type
func (c *CassandraStoreConsistency) GetConsistency() gocql.Consistency {
	return gocql.ParseConsistency(c.getConsistencySettings().Consistency)
}

// GetSerialConsistency returns the gosql.SerialConsistency setting from the configuration for the store
func (c *CassandraStoreConsistency) GetSerialConsistency() gocql.SerialConsistency {
	res, err := parseSerialConsistency(c.getConsistencySettings().SerialConsistency)
	if err != nil {
		panic(fmt.Sprintf("unable to decode cassandra serial consistency: %v", err))
	}
	return res
}

func (c *Cassandra) validate() error {
	return c.Consistency.validate()
}

func (c *CassandraStoreConsistency) validate() error {
	if c == nil {
		return nil
	}

	v := reflect.ValueOf(*c)

	for _, field := range v.Fields() {
		s, ok := field.Interface().(*CassandraConsistencySettings)
		if ok {
			if err := s.validate(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *CassandraConsistencySettings) validate() error {
	if c == nil {
		return nil
	}

	if c.Consistency != "" {
		_, err := gocql.ParseConsistencyWrapper(c.Consistency)
		if err != nil {
			return fmt.Errorf("bad cassandra consistency: %v", err)
		}
	}

	if c.SerialConsistency != "" {
		_, err := parseSerialConsistency(c.SerialConsistency)
		if err != nil {
			return fmt.Errorf("bad cassandra serial consistency: %v", err)
		}
	}

	return nil
}

func parseSerialConsistency(serialConsistency string) (gocql.SerialConsistency, error) {
	var s gocql.SerialConsistency
	err := s.UnmarshalText([]byte(strings.ToUpper(serialConsistency)))
	return s, err
}
