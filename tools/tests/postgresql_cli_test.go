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

type Postgresql12Suite struct {
	suite.Suite
	pluginName string
}

func (p *Postgresql12Suite) TestPostgreSQL12ConnTestSuite() {
	suite.Run(p.T(), clitest.NewSQLConnTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
	))
}

func (p *Postgresql12Suite) TestPostgreSQL12HandlerTestSuite() {
	suite.Run(p.T(), clitest.NewHandlerTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
	))
}

func (p *Postgresql12Suite) TestPostgreSQL12SetupSchemaTestSuite() {
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

func (p *Postgresql12Suite) TestPostgreSQL12UpdateSchemaTestSuite() {
	p.T().Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	p.T().Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewUpdateSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
		testPostgreSQL12ExecutionSchemaVersionDir,
		postgresqlversionV12.Version,
		testPostgreSQL12VisibilitySchemaVersionDir,
		postgresqlversionV12.VisibilityVersion,
	))
}

func (p *Postgresql12Suite) TestPostgreSQL12VersionTestSuite() {
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewVersionTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQL12ExecutionSchemaFile,
		testPostgreSQL12VisibilitySchemaFile,
	))
}

func TestPostgres12(t *testing.T) {
	s := &Postgresql12Suite{pluginName: postgresql.PluginNameV12}
	suite.Run(t, s)
}

func TestPostgre12PGX(t *testing.T) {
	s := &Postgresql12Suite{pluginName: postgresql.PluginNameV12PGX}
	suite.Run(t, s)
}
