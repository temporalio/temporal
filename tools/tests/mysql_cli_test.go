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

package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/environment"
	mysqlversionV8 "go.temporal.io/server/schema/mysql/v8"
	"go.temporal.io/server/tools/sql/clitest"
)

func TestMySQLConnTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewSQLConnTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLQuery,
	))
}

func TestMySQLHandlerTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewHandlerTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
	))
}

func TestMySQLSetupSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetMySQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetMySQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewSetupSchemaTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLQuery,
	))
}

func TestMySQLUpdateSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetMySQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetMySQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewUpdateSchemaTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLQuery,
		testMySQLExecutionSchemaVersionDir,
		mysqlversionV8.Version,
		testMySQLVisibilitySchemaVersionDir,
		mysqlversionV8.VisibilityVersion,
	))
}

func TestMySQLVersionTestSuite(t *testing.T) {
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewVersionTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLExecutionSchemaFile,
		testMySQLVisibilitySchemaFile,
	))
}
