package persistencetests

import (
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mssql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/temporal/environment"
)

const (
	testCassandraSchemaDir = "schema/cassandra/"

	testMySQLUser      = "temporal"
	testMySQLPassword  = "temporal"
	testMySQLSchemaDir = "schema/mysql/v8"

	testPostgreSQLUser      = "temporal"
	testPostgreSQLPassword  = "temporal"
	testPostgreSQLSchemaDir = "schema/postgresql/v12"

	testSQLiteUser      = ""
	testSQLitePassword  = ""
	testSQLiteMode      = "memory"
	testSQLiteCache     = "private"
	testSQLiteSchemaDir = "schema/sqlite/v3" // specify if mode is not "memory"

	// SQL Server enforces password complexity for the sa login; this matches
	// the MSSQL_SA_PASSWORD used by the dev docker-compose definitions.
	testMSSQLUser      = "sa"
	testMSSQLPassword  = "Temporal123!"
	testMSSQLSchemaDir = "schema/mssql/v2019"
)

// GetTestClusterOption returns test options for the given store type and driver.
func GetTestClusterOption(storeType, driver string) *TestBaseOptions {
	switch storeType {
	case config.StoreTypeSQL:
		switch driver {
		case mysql.PluginName:
			return GetMySQLTestClusterOption()
		case postgresql.PluginName, postgresql.PluginNamePGX:
			return GetPostgreSQLTestClusterOption(driver, nil)
		case sqlite.PluginName:
			return GetSQLiteMemoryTestClusterOption()
		case mssql.PluginName:
			return GetMSSQLTestClusterOption(driver, nil)
		default:
			panic(fmt.Sprintf("unknown sql driver: %v", driver))
		}
	case config.StoreTypeNoSQL:
		return GetCassandraTestClusterOption()
	default:
		panic(fmt.Sprintf("unknown store type: %v", storeType))
	}
}

// GetCassandraTestClusterOption returns test options
func GetCassandraTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		DBName:    GenerateRandomDBName(),
		DBHost:    environment.GetCassandraAddress(),
		DBPort:    environment.GetCassandraPort(),
		SchemaDir: testCassandraSchemaDir,
		StoreType: config.StoreTypeNoSQL,
	}
}

// GetMySQLTestClusterOption return test options
func GetMySQLTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: mysql.PluginName,
		DBName:          GenerateRandomDBName(),
		DBUsername:      testMySQLUser,
		DBPassword:      testMySQLPassword,
		DBHost:          environment.GetMySQLAddress(),
		DBPort:          environment.GetMySQLPort(),
		SchemaDir:       testMySQLSchemaDir,
		StoreType:       config.StoreTypeSQL,
	}
}

// GetPostgreSQLTestClusterOption return test options
func GetPostgreSQLTestClusterOption(
	pluginName string,
	connectAttributes map[string]string,
) *TestBaseOptions {
	switch pluginName {
	case postgresql.PluginName, postgresql.PluginNamePGX:
		// no-op
	default:
		panic(fmt.Sprintf(
			"invalid postgresql plugin name: %s (valid options: %s, %s)",
			pluginName,
			postgresql.PluginName,
			postgresql.PluginNamePGX,
		))
	}
	return &TestBaseOptions{
		SQLDBPluginName:   pluginName,
		DBName:            GenerateRandomDBName(),
		DBUsername:        testPostgreSQLUser,
		DBPassword:        testPostgreSQLPassword,
		DBHost:            environment.GetPostgreSQLAddress(),
		DBPort:            environment.GetPostgreSQLPort(),
		SchemaDir:         testPostgreSQLSchemaDir,
		StoreType:         config.StoreTypeSQL,
		ConnectAttributes: connectAttributes,
	}
}

// GetMSSQLTestClusterOption return test options
func GetMSSQLTestClusterOption(pluginName string, connectAttributes map[string]string) *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName:   pluginName,
		DBName:            GenerateRandomDBName(),
		DBUsername:        testMSSQLUser,
		DBPassword:        testMSSQLPassword,
		DBHost:            environment.GetMSSQLAddress(),
		DBPort:            environment.GetMSSQLPort(),
		SchemaDir:         testMSSQLSchemaDir,
		StoreType:         config.StoreTypeSQL,
		ConnectAttributes: connectAttributes,
	}
}

// GetSQLiteFileTestClusterOption return test options
func GetSQLiteFileTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: sqlite.PluginName,
		DBName:          filepath.Join(os.TempDir(), GenerateRandomDBName()), // put files in temp to avoid cluttering the project
		DBUsername:      testSQLiteUser,
		DBPassword:      testSQLitePassword,
		DBHost:          environment.GetLocalhostIP(),
		DBPort:          0,
		SchemaDir:       testSQLiteSchemaDir,
		StoreType:       config.StoreTypeSQL,
		ConnectAttributes: map[string]string{
			"cache":        "shared",
			"busy_timeout": "30000",
			"journal_mode": "wal",
			"synchronous":  "normal",
		},
	}
}

// GetSQLiteMemoryTestClusterOption return test options
func GetSQLiteMemoryTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName:   sqlite.PluginName,
		DBName:            GenerateRandomDBName(),
		DBUsername:        testSQLiteUser,
		DBPassword:        testSQLitePassword,
		DBHost:            environment.GetLocalhostIP(),
		DBPort:            0,
		SchemaDir:         "",
		StoreType:         config.StoreTypeSQL,
		ConnectAttributes: map[string]string{"mode": testSQLiteMode, "cache": testSQLiteCache},
	}
}
