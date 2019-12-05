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
	"log"
	"os"

	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/schema/mysql"
	"github.com/uber/cadence/tools/common/schema/test"
	"github.com/uber/cadence/tools/sql"
)

// UpdateSchemaTestSuite defines a test suite
type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
	pluginName string
}

// NewUpdateSchemaTestSuite returns a test suite
func NewUpdateSchemaTestSuite(pluginName string) *UpdateSchemaTestSuite {
	return &UpdateSchemaTestSuite{
		pluginName: pluginName,
	}
}

// SetupSuite setups test suite
func (s *UpdateSchemaTestSuite) SetupSuite() {
	os.Setenv("SQL_HOST", environment.GetMySQLAddress())
	os.Setenv("SQL_USER", testUser)
	os.Setenv("SQL_PASSWORD", testPassword)
	conn, err := newTestConn("", s.pluginName)
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.SetupSuiteBase(conn)
}

// TearDownSuite tear down test suite
func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestUpdateSchema test
func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	conn, err := newTestConn(s.DBName, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	s.RunUpdateSchemaTest(sql.BuildCLIOptions(), conn, "--db", createTestSQLFileContent(), []string{"task_maps", "tasks"})
}

// TestDryrun test
func (s *UpdateSchemaTestSuite) TestDryrun() {
	conn, err := newTestConn(s.DBName, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	dir := "../../../../../schema/mysql/v57/cadence/versioned"
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, mysql.Version)
}

// TestVisibilityDryrun test
func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	conn, err := newTestConn(s.DBName, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	dir := "../../../../../schema/mysql/v57/visibility/versioned"
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, mysql.VisibilityVersion)
}
