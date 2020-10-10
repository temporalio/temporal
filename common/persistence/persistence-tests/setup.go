package persistencetests

import (
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/environment"
)

const (
	testMySQLUser      = "temporal"
	testMySQLPassword  = "temporal"
	testMySQLSchemaDir = "schema/mysql/v57"

	testPostgreSQLUser      = "temporal"
	testPostgreSQLPassword  = "temporal"
	testPostgreSQLSchemaDir = "schema/postgresql/v96"
)

// GetMySQLTestClusterOption return test options
func GetMySQLTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: mysql.PluginName,
		DBUsername:      testMySQLUser,
		DBPassword:      testMySQLPassword,
		DBHost:          environment.GetMySQLAddress(),
		DBPort:          environment.GetMySQLPort(),
		SchemaDir:       testMySQLSchemaDir,
		StoreType:       config.StoreTypeSQL,
	}
}

// GetPostgreSQLTestClusterOption return test options
func GetPostgreSQLTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: postgresql.PluginName,
		DBUsername:      testPostgreSQLUser,
		DBPassword:      testPostgreSQLPassword,
		DBHost:          environment.GetPostgreSQLAddress(),
		DBPort:          environment.GetPostgreSQLPort(),
		SchemaDir:       testPostgreSQLSchemaDir,
		StoreType:       config.StoreTypeSQL,
	}
}
