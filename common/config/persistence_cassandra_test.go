//go:build !temporal_minimal || temporal_with_cassandra

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataStoreValidate_Cassandra(t *testing.T) {
	ds := &DataStore{Cassandra: &Cassandra{Hosts: "127.0.0.1"}}
	require.NoError(t, ds.Validate())
}
