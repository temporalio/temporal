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
	cockroachschema "go.temporal.io/server/schema/cockroach"
	"go.temporal.io/server/tools/sql/clitest"
)

type CockroachSuite struct {
	suite.Suite
	pluginName string
}

func (p *CockroachSuite) TestCockroachConnTestSuite() {
	suite.Run(p.T(), clitest.NewSQLConnTestSuite(
		environment.GetCockroachAddress(),
		strconv.Itoa(environment.GetCockroachPort()),
		p.pluginName,
		testCockroachQuery,
	))
}

func (p *CockroachSuite) TestCockroachHandlerTestSuite() {
	suite.Run(p.T(), clitest.NewHandlerTestSuite(
		environment.GetCockroachAddress(),
		strconv.Itoa(environment.GetCockroachPort()),
		p.pluginName,
	))
}

func (p *CockroachSuite) TestCockroachSetupSchemaTestSuite() {
	p.T().Setenv("SQL_HOST", environment.GetCockroachAddress())
	p.T().Setenv("SQL_PORT", strconv.Itoa(environment.GetCockroachPort()))
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewSetupSchemaTestSuite(
		environment.GetCockroachAddress(),
		strconv.Itoa(environment.GetCockroachPort()),
		p.pluginName,
		testCockroachQuery,
	))
}

func (p *CockroachSuite) TestCockroachUpdateSchemaTestSuite() {
	p.T().Setenv("SQL_HOST", environment.GetCockroachAddress())
	p.T().Setenv("SQL_PORT", strconv.Itoa(environment.GetCockroachPort()))
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewUpdateSchemaTestSuite(
		environment.GetCockroachAddress(),
		strconv.Itoa(environment.GetCockroachPort()),
		p.pluginName,
		testCockroachQuery,
		testCockroachExecutionSchemaVersionDir,
		cockroachschema.Version,
		testCockroachVisibilitySchemaVersionDir,
		cockroachschema.VisibilityVersion,
	))
}

func (p *CockroachSuite) TestCockroachVersionTestSuite() {
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewVersionTestSuite(
		environment.GetCockroachAddress(),
		strconv.Itoa(environment.GetCockroachPort()),
		p.pluginName,
		testCockroachExecutionSchemaFile,
		testCockroachVisibilitySchemaFile,
	))
}

func TestCockroachSuite(t *testing.T) {
	s := &CockroachSuite{pluginName: postgresql.PluginName}
	suite.Run(t, s)
}
