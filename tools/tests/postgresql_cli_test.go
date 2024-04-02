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
	"go.temporal.io/server/tools/sql/clitest"
)

type PostgresqlSuite struct {
	suite.Suite
	pluginName string
}

func (p *PostgresqlSuite) TestPostgreSQLConnTestSuite() {
	suite.Run(p.T(), clitest.NewSQLConnTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLHandlerTestSuite() {
	suite.Run(p.T(), clitest.NewHandlerTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLSetupSchemaTestSuite() {
	p.T().Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	p.T().Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewSetupSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLUpdateSchemaTestSuite() {
	p.T().Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	p.T().Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewUpdateSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
		testPostgreSQLExecutionSchemaVersionDir,
		postgresqlversionV12.Version,
		testPostgreSQLVisibilitySchemaVersionDir,
		postgresqlversionV12.VisibilityVersion,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLVersionTestSuite() {
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewVersionTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLExecutionSchemaFile,
		testPostgreSQLVisibilitySchemaFile,
	))
}

func TestPostgres(t *testing.T) {
	s := &PostgresqlSuite{pluginName: postgresql.PluginName}
	suite.Run(t, s)
}

func TestPostgresPGX(t *testing.T) {
	s := &PostgresqlSuite{pluginName: postgresql.PluginNamePGX}
	suite.Run(t, s)
}
