//go:build gomad

package testcore

import "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"

// sqlPluginNames holds only SQLite under Gomad: the MySQL and PostgreSQL
// drivers reach the syscall and os/user packages the deterministic runtime
// forbids, and a simulated cluster persists to SQLite.
var sqlPluginNames = []string{sqlite.PluginName}
