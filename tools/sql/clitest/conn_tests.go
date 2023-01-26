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

package clitest

import (
	"net"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/common/schema/test"
	"go.temporal.io/server/tools/sql"
)

type (
	// SQLConnTestSuite defines a test suite
	SQLConnTestSuite struct {
		test.DBTestBase
		host       string
		port       string
		pluginName string
		sqlQuery   string
	}
)

var _ test.DB = (*sql.Connection)(nil)

const (
	testUser     = "temporal"
	testPassword = "temporal"
)

// NewSQLConnTestSuite returns the test suite
func NewSQLConnTestSuite(
	host string,
	port string,
	pluginName string,
	sqlQuery string,
) *SQLConnTestSuite {
	return &SQLConnTestSuite{
		host:       host,
		port:       port,
		pluginName: pluginName,
		sqlQuery:   sqlQuery,
	}
}

// SetupTest setups test
func (s *SQLConnTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

// SetupSuite setups test suite
func (s *SQLConnTestSuite) SetupSuite() {
	conn, err := newTestConn("", s.host, s.port, s.pluginName)
	if err != nil {
		logger := log.NewTestLogger()
		logger.Fatal("error creating sql conn", tag.Error(err))
	}
	s.SetupSuiteBase(conn)
}

// TearDownSuite tear down test suite
func (s *SQLConnTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestParseCQLFile test
func (s *SQLConnTestSuite) TestParseCQLFile() {
	s.RunParseFileTest(s.sqlQuery)
}

// TestSQLConn test
func (s *SQLConnTestSuite) TestSQLConn() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.Nil(err)
	s.RunCreateTest(conn)
	s.RunUpdateTest(conn)
	s.RunDropTest(conn)
	conn.Close()
}

func newTestConn(
	database string,
	host string,
	port string,
	pluginName string,
) (*sql.Connection, error) {
	connectAddr := net.JoinHostPort(host, port)
	return sql.NewConnection(&config.SQL{
		ConnectAddr:  connectAddr,
		User:         testUser,
		Password:     testPassword,
		PluginName:   pluginName,
		DatabaseName: database,
	})
}
