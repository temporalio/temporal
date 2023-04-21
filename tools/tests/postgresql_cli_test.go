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

	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/environment"
	postgresqlversionV12 "go.temporal.io/server/schema/postgresql/v12"
	postgresqlversionV96 "go.temporal.io/server/schema/postgresql/v96"
	"go.temporal.io/server/tools/sql/clitest"
)

func TestPostgreSQLConnTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewSQLConnTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName, testPostgreSQLQuery,
	))
}

func TestPostgreSQLHandlerTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewHandlerTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
	))
}

func TestPostgreSQLSetupSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewSetupSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
		testPostgreSQLQuery,
	))
}

func TestPostgreSQLUpdateSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewUpdateSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
		testPostgreSQLQuery,
		testPostgreSQLExecutionSchemaVersionDir,
		postgresqlversionV96.Version,
		testPostgreSQLVisibilitySchemaVersionDir,
		postgresqlversionV96.VisibilityVersion,
	))
}

func TestPostgreSQLVersionTestSuite(t *testing.T) {
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewVersionTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginName,
		testPostgreSQLExecutionSchemaFile,
		testPostgreSQLVisibilitySchemaFile,
	))
}

func TestPostgreSQL12ConnTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewSQLConnTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginNameV12,
		testPostgreSQLQuery,
	))
}

func TestPostgreSQL12HandlerTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewHandlerTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginNameV12,
	))
}

func TestPostgreSQL12SetupSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewSetupSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginNameV12,
		testPostgreSQLQuery,
	))
}

func TestPostgreSQL12UpdateSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewUpdateSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginNameV12,
		testPostgreSQLQuery,
		testPostgreSQL12ExecutionSchemaVersionDir,
		postgresqlversionV12.Version,
		testPostgreSQL12VisibilitySchemaVersionDir,
		postgresqlversionV12.VisibilityVersion,
	))
}

func TestPostgreSQL12VersionTestSuite(t *testing.T) {
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewVersionTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		postgresql.PluginNameV12,
		testPostgreSQL12ExecutionSchemaFile,
		testPostgreSQL12VisibilitySchemaFile,
	))
}
