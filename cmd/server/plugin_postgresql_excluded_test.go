//go:build temporal_minimal && !temporal_with_postgresql

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func TestPostgreSQLPluginsExcluded(t *testing.T) {
	t.Parallel()

	for _, name := range []string{sqlplugin.PluginNamePostgreSQL, sqlplugin.PluginNamePostgreSQLPGX} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sql.GetPluginVisibilityQueryConverter(name)
			require.ErrorIs(t, err, sql.ErrPluginNotSupported)
		})
	}
}
