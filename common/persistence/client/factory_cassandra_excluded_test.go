//go:build temporal_minimal && !temporal_with_cassandra

package client

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

func TestVerifyCompatibleVersion_CassandraExcluded(t *testing.T) {
	cfg := config.Persistence{
		DefaultStore: "default",
		DataStores: map[string]config.DataStore{
			"default": {Cassandra: &config.Cassandra{Hosts: "127.0.0.1"}},
		},
	}
	err := VerifyCompatibleVersion(cfg, nil, log.NewNoopLogger())
	require.EqualError(t, err, `datastore "default": cassandra persistence is not compiled into this server; remove this config or rebuild with the temporal_with_cassandra build tag`)
}
