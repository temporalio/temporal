//go:build temporal_minimal && !temporal_with_cassandra

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataStoreValidate_CassandraExcluded(t *testing.T) {
	ds := &DataStore{Cassandra: &Cassandra{Hosts: "127.0.0.1"}}
	require.EqualError(t, ds.Validate(),
		"cassandra: Cassandra persistence is not compiled into this server; remove this config or rebuild with the temporal_with_cassandra build tag")
}
