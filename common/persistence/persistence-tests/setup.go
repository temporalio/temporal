package persistencetests

import (
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
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
)

// GetTestClusterOption returns test options for the given store type and driver.
func GetTestClusterOption(storeType, driver string) *TestBaseOptions {
	switch storeType {
	case config.StoreTypeSQL:
		switch driver {
		case sqlplugin.PluginNameMySQL:
			return GetMySQLTestClusterOption()
		case sqlplugin.PluginNamePostgreSQL, sqlplugin.PluginNamePostgreSQLPGX:
			return GetPostgreSQLTestClusterOption(driver, nil)
		case sqlplugin.PluginNameSQLite:
			return GetSQLiteMemoryTestClusterOption()
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
		SQLDBPluginName: sqlplugin.PluginNameMySQL,
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
	case sqlplugin.PluginNamePostgreSQL, sqlplugin.PluginNamePostgreSQLPGX:
		// no-op
	default:
		panic(fmt.Sprintf(
			"invalid postgresql plugin name: %s (valid options: %s, %s)",
			pluginName,
			sqlplugin.PluginNamePostgreSQL,
			sqlplugin.PluginNamePostgreSQLPGX,
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

// GetSQLiteFileTestClusterOption return test options
func GetSQLiteFileTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: sqlplugin.PluginNameSQLite,
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
		SQLDBPluginName:   sqlplugin.PluginNameSQLite,
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

func GetEsTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		DBName:     "temporal_visibility_v1_" + GenerateRandomDBName(),
		DBUsername: "",
		DBPassword: "",
		DBHost:     environment.GetESAddress(),
		DBPort:     environment.GetESPort(),
	}
}
