// Copyright (c) 2017 Uber Technologies, Inc.
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
	log "github.com/sirupsen/logrus"

	"os"

	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/tools/common/schema/test"
	"github.com/uber/cadence/tools/sql"
)

type (
	// SetupSchemaTestSuite defines a test suite
	SetupSchemaTestSuite struct {
		test.SetupSchemaTestBase
		conn       *sql.Connection
		pluginName string
	}
)

// NewSetupSchemaTestSuite returns a test suite
func NewSetupSchemaTestSuite(pluginName string) *SetupSchemaTestSuite {
	return &SetupSchemaTestSuite{
		pluginName: pluginName,
	}
}

// SetupSuite setup test suite
func (s *SetupSchemaTestSuite) SetupSuite() {
	os.Setenv("SQL_HOST", environment.GetMySQLAddress())
	os.Setenv("SQL_USER", testUser)
	os.Setenv("SQL_PASSWORD", testPassword)
	conn, err := newTestConn("", s.pluginName)
	if err != nil {
		log.Fatalf("error creating sql connection:%v", err)
	}
	s.conn = conn
	s.SetupSuiteBase(conn)
}

// TearDownSuite tear down test suite
func (s *SetupSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestCreateDatabase test
func (s *SetupSchemaTestSuite) TestCreateDatabase() {
	s.NoError(sql.RunTool([]string{"./tool", "-u", testUser, "--pw", testPassword, "create", "--db", "foobar123"}))
	err := s.conn.DropDatabase("foobar123")
	s.Nil(err)
}

// TestSetupSchema test
func (s *SetupSchemaTestSuite) TestSetupSchema() {
	conn, err := newTestConn(s.DBName, s.pluginName)
	s.Nil(err)
	s.RunSetupTest(sql.BuildCLIOptions(), conn, "--db", createTestSQLFileContent(), []string{"task_maps", "tasks"})
}
