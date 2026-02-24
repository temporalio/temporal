package persistencetests

import (
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/server/common/config"
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
)

// GetTestClusterOption returns test options for the given store type and driver.
func GetTestClusterOption(storeType, driver string) *TestBaseOptions {
	switch storeType {
	case config.StoreTypeSQL:
		switch driver {
		case mysql.PluginName:
			return GetMySQLTestClusterOption()
		case postgresql.PluginName:
			return GetPostgreSQLTestClusterOption()
		case postgresql.PluginNamePGX:
			return GetPostgreSQLPGXTestClusterOption()
		case sqlite.PluginName:
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
		DBName:    "test_" + GenerateRandomDBName(3),
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
		DBName:          "test_" + GenerateRandomDBName(3),
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
		DBName:          "test_" + GenerateRandomDBName(3),
		DBUsername:      testPostgreSQLUser,
		DBPassword:      testPostgreSQLPassword,
		DBHost:          environment.GetPostgreSQLAddress(),
		DBPort:          environment.GetPostgreSQLPort(),
		SchemaDir:       testPostgreSQLSchemaDir,
		StoreType:       config.StoreTypeSQL,
	}
}

// GetPostgreSQLPGXTestClusterOption return test options
func GetPostgreSQLPGXTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: postgresql.PluginNamePGX,
		DBName:          "test_" + GenerateRandomDBName(3),
		DBUsername:      testPostgreSQLUser,
		DBPassword:      testPostgreSQLPassword,
		DBHost:          environment.GetPostgreSQLAddress(),
		DBPort:          environment.GetPostgreSQLPort(),
		SchemaDir:       testPostgreSQLSchemaDir,
		StoreType:       config.StoreTypeSQL,
	}
}

// GetSQLiteFileTestClusterOption return test options
func GetSQLiteFileTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName: sqlite.PluginName,
		DBName:          filepath.Join(os.TempDir(), "test_"+GenerateRandomDBName(3)), // put files in temp to avoid cluttering the project
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
		DBName:            "test_" + GenerateRandomDBName(3),
		DBUsername:        testSQLiteUser,
		DBPassword:        testSQLitePassword,
		DBHost:            environment.GetLocalhostIP(),
		DBPort:            0,
		SchemaDir:         "",
		StoreType:         config.StoreTypeSQL,
		ConnectAttributes: map[string]string{"mode": testSQLiteMode, "cache": testSQLiteCache},
	}
}
