//go:build gomad_wasm_slim && !gomad

package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func TestSlimGuestRefusesUnlinkedSQLDriversWithoutFallback(t *testing.T) {
	previous := cliFlags.persistenceDriver
	t.Cleanup(func() { cliFlags.persistenceDriver = previous })
	for _, driver := range []string{sqlplugin.PluginNameMySQL, sqlplugin.PluginNamePostgreSQL, sqlplugin.PluginNamePostgreSQLPGX} {
		cliFlags.persistenceDriver = driver
		require.True(t, UseSQLVisibility(), "%s must not fall back to Elasticsearch visibility", driver)
		for _, kind := range []sqlplugin.DbKind{sqlplugin.DbKindMain, sqlplugin.DbKindVisibility} {
			db, err := sql.NewSQLDB(kind, &config.SQL{PluginName: driver}, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
			require.ErrorIs(t, err, sql.ErrPluginNotSupported, driver)
			require.ErrorContains(t, err, "supported plugins: [sqlite]")
			require.Nil(t, db)
		}
	}
	cliFlags.persistenceDriver = sqlplugin.PluginNameSQLite
	require.True(t, UseSQLVisibility())
}
