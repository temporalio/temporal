package testcore

import (
	"flag"

	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

// TestFlags contains the feature flags for functional tests
var TestFlags struct {
	PersistenceType          string
	PersistenceDriver        string
	FaultInjectionConfigFile string
}

func init() {
	flag.StringVar(&TestFlags.PersistenceType, "persistenceType", "sql", "type of persistence - [nosql or sql]")
	flag.StringVar(&TestFlags.PersistenceDriver, "persistenceDriver", "sqlite", "driver of nosql/sql - [cassandra, mysql8, postgres12, sqlite]")
	flag.StringVar(&TestFlags.FaultInjectionConfigFile, "faultInjectionConfigFile", "", "fault injection config file location")
}

func UseSQLVisibility() bool {
	switch TestFlags.PersistenceDriver {
	case mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
		return true
	// If the main storage is Cassandra, Elasticsearch is used for visibility.
	default:
		return false
	}
}
