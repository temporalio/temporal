//go:build !gomad && !gomad_wasm_slim

package testcore

import (
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

// sqlPluginNames are the SQL persistence drivers a functional test may select.
var sqlPluginNames = []string{mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName}
