//go:build !temporal_minimal || temporal_with_cassandra

package gocql

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
)

// TestConfigAcceptsDriverConsistencies guards config's driver-free consistency validation against drifting from the driver.
func TestConfigAcceptsDriverConsistencies(t *testing.T) {
	t.Parallel()

	for _, c := range []gocql.Consistency{
		gocql.Any, gocql.One, gocql.Two, gocql.Three, gocql.Quorum, gocql.All, gocql.LocalQuorum, gocql.EachQuorum, gocql.LocalOne,
	} {
		ds := config.DataStore{Cassandra: &config.Cassandra{Hosts: "127.0.0.1", Consistency: &config.CassandraStoreConsistency{
			Default: &config.CassandraConsistencySettings{Consistency: c.String()},
		}}}
		require.NoError(t, ds.Validate(), c.String())
	}
	for _, s := range []gocql.SerialConsistency{gocql.Serial, gocql.LocalSerial} {
		ds := config.DataStore{Cassandra: &config.Cassandra{Hosts: "127.0.0.1", Consistency: &config.CassandraStoreConsistency{
			Default: &config.CassandraConsistencySettings{SerialConsistency: s.String()},
		}}}
		require.NoError(t, ds.Validate(), s.String())
	}
}
