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
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/tools/common/schema/test"
	"go.temporal.io/server/tools/sql"
)

type (
	// SetupSchemaTestSuite defines a test suite
	SetupSchemaTestSuite struct {
		test.SetupSchemaTestBase
		host       string
		port       string
		pluginName string
		sqlQuery   string

		conn *sql.Connection
	}
)

const (
	testCLIDatabasePrefix = "test_"
	testCLIDatabaseSuffix = "cli_database"
)

// NewSetupSchemaTestSuite returns a test suite
func NewSetupSchemaTestSuite(
	host string,
	port string,
	pluginName string,
	sqlQuery string,
) *SetupSchemaTestSuite {
	return &SetupSchemaTestSuite{
		host:       host,
		port:       port,
		pluginName: pluginName,
		sqlQuery:   sqlQuery,
	}
}

// SetupSuite setup test suite
func (s *SetupSchemaTestSuite) SetupSuite() {
	conn, err := newTestConn("", s.host, s.port, s.pluginName)
	if err != nil {
		s.Fail("error creating sql connection:%v", err)
	}
	s.conn = conn
	s.SetupSuiteBase(conn, s.pluginName)
}

// TearDownSuite tear down test suite
func (s *SetupSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestCreateDatabase test
func (s *SetupSchemaTestSuite) TestCreateDatabase() {
	testDatabase := testCLIDatabasePrefix + shuffle.String(testCLIDatabaseSuffix)
	err := sql.RunTool([]string{
		"./tool",
		"--ep", s.host,
		"--p", s.port,
		"-u", testUser,
		"--pw", testPassword,
		"--pl", s.pluginName,
		"--db", testDatabase,
		"create",
	})
	s.NoError(err)
	err = s.conn.DropDatabase(testDatabase)
	s.NoError(err)
}

func (s *SetupSchemaTestSuite) TestCreateDatabaseIdempotent() {
	testDatabase := testCLIDatabasePrefix + shuffle.String(testCLIDatabaseSuffix)
	err := sql.RunTool([]string{
		"./tool",
		"--ep", s.host,
		"--p", s.port,
		"-u", testUser,
		"--pw", testPassword,
		"--pl", s.pluginName,
		"--db", testDatabase,
		"create",
	})
	s.NoError(err)

	err = sql.RunTool([]string{
		"./tool",
		"--ep", s.host,
		"--p", s.port,
		"-u", testUser,
		"--pw", testPassword,
		"--pl", s.pluginName,
		"--db", testDatabase,
		"create",
	})
	s.NoError(err)

	err = s.conn.DropDatabase(testDatabase)
	s.NoError(err)
}

// TestSetupSchema test
func (s *SetupSchemaTestSuite) TestSetupSchema() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.NoError(err)
	defer conn.Close()
	s.RunSetupTest(sql.BuildCLIOptions(), conn, "--db", s.sqlQuery, []string{"executions", "current_executions"})
}
