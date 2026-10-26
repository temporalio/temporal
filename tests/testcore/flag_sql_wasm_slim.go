//go:build gomad_wasm_slim && !gomad

package testcore

import (
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

// sqlPluginNames keeps every SQL driver name in the gomad_wasm_slim WASM guest
// build, so selecting MySQL or PostgreSQL still chooses SQL visibility rather
// than falling back to Elasticsearch. Only SQLite is linked and registered:
// the other drivers fail at plugin lookup with sql.ErrPluginNotSupported.
var sqlPluginNames = []string{sqlplugin.PluginNameMySQL, sqlplugin.PluginNamePostgreSQL, sqlplugin.PluginNamePostgreSQLPGX, sqlite.PluginName}
