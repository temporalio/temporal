//go:build !temporal_minimal || (temporal_with_mysql && temporal_with_postgresql)

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql"
)

func TestSQLPluginsRegistered(t *testing.T) {
	for _, pluginName := range []string{"mysql8", "postgres12", "postgres12_pgx", "sqlite"} {
		t.Run(pluginName, func(t *testing.T) {
			converter, err := sql.GetPluginVisibilityQueryConverter(pluginName)
			require.NoError(t, err)
			require.NotNil(t, converter)
		})
	}
}
