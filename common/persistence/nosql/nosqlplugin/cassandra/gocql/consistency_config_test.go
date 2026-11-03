//go:build !temporal_minimal || temporal_with_cassandra

package gocql

import (
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

// TestConfigAcceptsDriverConsistencies guards config's driver-free consistency validation against drifting from the driver,
// by checking that config accepts exactly the consistency levels the driver can parse.
func TestConfigAcceptsDriverConsistencies(t *testing.T) {
	t.Parallel()

	validate := func(settings config.CassandraConsistencySettings) error {
		ds := config.DataStore{Cassandra: &config.Cassandra{Hosts: "127.0.0.1", Consistency: &config.CassandraStoreConsistency{
			Default: &settings,
		}}}
		return ds.Validate()
	}

	var driverAccepted, configAccepted []string
	for v := range math.MaxUint16 + 1 {
		name := gocql.Consistency(v).String()
		var c gocql.Consistency
		if c.UnmarshalText([]byte(name)) == nil {
			driverAccepted = append(driverAccepted, name)
		}
		if validate(config.CassandraConsistencySettings{Consistency: name}) == nil {
			configAccepted = append(configAccepted, name)
		}
	}
	require.NotEmpty(t, driverAccepted)
	require.Equal(t, driverAccepted, configAccepted)

	var driverAcceptedSerial, configAcceptedSerial []string
	for v := range math.MaxUint16 + 1 {
		name := gocql.SerialConsistency(v).String()
		var s gocql.SerialConsistency
		if s.UnmarshalText([]byte(name)) == nil {
			driverAcceptedSerial = append(driverAcceptedSerial, name)
		}
		if validate(config.CassandraConsistencySettings{SerialConsistency: name}) == nil {
			configAcceptedSerial = append(configAcceptedSerial, name)
		}
	}
	require.NotEmpty(t, driverAcceptedSerial)
	require.Equal(t, driverAcceptedSerial, configAcceptedSerial)
}
