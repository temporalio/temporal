//go:build !temporal_minimal || temporal_with_cassandra

package config

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// cassandraConsistencies and cassandraSerialConsistencies are the consistency levels accepted by the Cassandra driver.
var (
	cassandraConsistencies       = []string{"ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE"}
	cassandraSerialConsistencies = []string{"SERIAL", "LOCAL_SERIAL"}
)

func validateCassandra(c *Cassandra) error {
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
		if !slices.Contains(cassandraConsistencies, strings.ToUpper(c.Consistency)) {
			return fmt.Errorf("bad cassandra consistency: invalid consistency %q", c.Consistency)
		}
	}

	if c.SerialConsistency != "" {
		if !slices.Contains(cassandraSerialConsistencies, strings.ToUpper(c.SerialConsistency)) {
			return fmt.Errorf("bad cassandra serial consistency: invalid consistency %q", c.SerialConsistency)
		}
	}

	return nil
}
