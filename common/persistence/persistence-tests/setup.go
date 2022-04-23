// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistencetests

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/environment"
)

const (
	testMySQLUser      = "temporal"
	testMySQLPassword  = "temporal"
	testMySQLSchemaDir = "schema/mysql/v57"

	testPostgreSQLUser      = "temporal"
	testPostgreSQLPassword  = "temporal"
	testPostgreSQLSchemaDir = "schema/postgresql/v96"

	testSQLiteUser      = ""
	testSQLitePassword  = ""
	testSQLiteMode      = "memory"
	testSQLiteCache     = "private"
	testSQLiteSchemaDir = "schema/sqlite/v3" // specify if mode is not "memory"
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

// GetSQLiteTestClusterOption return test options
func GetSQLiteFileTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName:   sqlite.PluginName,
		DBUsername:        testSQLiteUser,
		DBPassword:        testSQLitePassword,
		DBHost:            environment.Localhost,
		DBPort:            0,
		SchemaDir:         testSQLiteSchemaDir,
		StoreType:         config.StoreTypeSQL,
		ConnectAttributes: map[string]string{"cache": testSQLiteCache},
	}
}

// GetSQLiteTestClusterOption return test options
func GetSQLiteMemoryTestClusterOption() *TestBaseOptions {
	return &TestBaseOptions{
		SQLDBPluginName:   sqlite.PluginName,
		DBUsername:        testSQLiteUser,
		DBPassword:        testSQLitePassword,
		DBHost:            environment.Localhost,
		DBPort:            0,
		SchemaDir:         "",
		StoreType:         config.StoreTypeSQL,
		ConnectAttributes: map[string]string{"mode": testSQLiteMode, "cache": testSQLiteCache},
	}
}
