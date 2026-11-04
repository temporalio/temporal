//go:build temporal_minimal && !temporal_with_mysql

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataStoreValidateMySQLExcluded(t *testing.T) {
	t.Parallel()

	ds := &DataStore{SQL: &SQL{PluginName: "mysql8"}}
	require.EqualError(t, ds.Validate(),
		"mysql: MySQL persistence is not compiled into this server; remove this config or rebuild with the temporal_with_mysql build tag")
}
