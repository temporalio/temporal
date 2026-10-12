//go:build temporal_minimal && !temporal_with_mysql && !temporal_with_postgresql

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql"
)

func TestSlimSQLPlugins(t *testing.T) {
	_, err := sql.GetPluginVisibilityQueryConverter("sqlite")
	require.NoError(t, err)

	for _, pluginName := range []string{"mysql8", "postgres12", "postgres12_pgx"} {
		t.Run(pluginName, func(t *testing.T) {
			_, err := sql.GetPluginVisibilityQueryConverter(pluginName)
			require.Error(t, err)
		})
	}
}
