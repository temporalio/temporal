package testcore

import (
	"flag"
	"slices"

	"go.temporal.io/server/common/config"
)

// cliFlags contains the feature flags for functional tests
var cliFlags struct {
	persistenceType      string
	persistenceDriver    string
	enableFaultInjection string
}

func init() {
	flag.StringVar(&cliFlags.persistenceType, "persistenceType", "sql", "type of persistence - [nosql or sql]")
	flag.StringVar(&cliFlags.persistenceDriver, "persistenceDriver", "sqlite", "driver of nosql/sql - [cassandra, mysql8, postgres12, sqlite]")
	flag.StringVar(&cliFlags.enableFaultInjection, "enableFaultInjection", "", "enable global fault injection")
}

// UseSQLVisibility reports whether the persistence driver is a SQL plugin. If
// the main storage is Cassandra, Elasticsearch is used for visibility.
func UseSQLVisibility() bool {
	return slices.Contains(sqlPluginNames, cliFlags.persistenceDriver)
}

func UseCassandraPersistence() bool {
	return cliFlags.persistenceType == config.StoreTypeNoSQL && cliFlags.persistenceDriver == "cassandra"
}
