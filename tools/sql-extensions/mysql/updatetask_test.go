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

package mysql

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/environment"
	"github.com/temporalio/temporal/schema/mysql"
	"github.com/temporalio/temporal/tools/common/schema/test"
	"github.com/temporalio/temporal/tools/sql"
)

type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
}

func TestUpdateSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(UpdateSchemaTestSuite))
}

func (s *UpdateSchemaTestSuite) SetupSuite() {
	os.Setenv("SQL_HOST", environment.GetMySQLAddress())
	os.Setenv("SQL_USER", testUser)
	os.Setenv("SQL_PASSWORD", testPassword)
	conn, err := newTestConn("")
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.SetupSuiteBase(conn)
}

func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	conn, err := newTestConn(s.DBName)
	s.Nil(err)
	defer conn.Close()
	s.RunUpdateSchemaTest(sql.BuildCLIOptions(), conn, "--db", createTestSQLFileContent(), []string{"task_maps", "tasks"})
}

func (s *UpdateSchemaTestSuite) TestDryrun() {
	conn, err := newTestConn(s.DBName)
	s.Nil(err)
	defer conn.Close()
	dir := "../../../schema/mysql/v57/cadence/versioned"
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, mysql.Version)
}

func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	conn, err := newTestConn(s.DBName)
	s.Nil(err)
	defer conn.Close()
	dir := "../../../schema/mysql/v57/visibility/versioned"
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, mysql.VisibilityVersion)
}
