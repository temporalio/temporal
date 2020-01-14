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
	"net"
	"strconv"

	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/environment"
	"github.com/uber/cadence/tools/common/schema/test"
	"github.com/uber/cadence/tools/sql"
)

type (
	// SQLConnTestSuite defines a test suite
	SQLConnTestSuite struct {
		test.DBTestBase
		pluginName string
	}
)

var _ test.DB = (*sql.Connection)(nil)

const (
	testUser     = "uber"
	testPassword = "uber"
)

// NewSQLConnTestSuite returns the test suite
func NewSQLConnTestSuite(pluginName string) *SQLConnTestSuite {
	return &SQLConnTestSuite{
		pluginName: pluginName,
	}
}

// SetupTest setups test
func (s *SQLConnTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

// SetupSuite setups test suite
func (s *SQLConnTestSuite) SetupSuite() {
	conn, err := newTestConn("", s.pluginName)
	if err != nil {
		s.Log.Fatal("error creating sql conn, ", tag.Error(err))
	}
	s.SetupSuiteBase(conn)
}

// TearDownSuite tear down test suite
func (s *SQLConnTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestParseCQLFile test
func (s *SQLConnTestSuite) TestParseCQLFile() {
	s.RunParseFileTest(createTestSQLFileContent())
}

// TestSQLConn test
// TODO refactor the whole package to support testing against Postgres
// https://github.com/uber/cadence/issues/2856
func (s *SQLConnTestSuite) TestSQLConn() {
	conn, err := sql.NewConnection(&config.SQL{
		ConnectAddr: net.JoinHostPort(
			environment.GetMySQLAddress(),
			strconv.Itoa(environment.GetMySQLPort()),
		),
		User:         testUser,
		Password:     testPassword,
		PluginName:   s.pluginName,
		DatabaseName: s.DBName,
	})
	s.Nil(err)
	s.RunCreateTest(conn)
	s.RunUpdateTest(conn)
	s.RunDropTest(conn)
	conn.Close()
}

func newTestConn(database, pluginName string) (*sql.Connection, error) {
	return sql.NewConnection(&config.SQL{
		ConnectAddr: net.JoinHostPort(
			environment.GetMySQLAddress(),
			strconv.Itoa(environment.GetMySQLPort()),
		),
		User:         testUser,
		Password:     testPassword,
		PluginName:   pluginName,
		DatabaseName: database,
	})
}

func createTestSQLFileContent() string {
	return `
-- test sql file content

CREATE TABLE task_maps (
  shard_id INT NOT NULL,
  domain_id BINARY(16) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id BINARY(16) NOT NULL,
  first_event_id BIGINT NOT NULL,
--
  version BIGINT NOT NULL,
  next_event_id BIGINT NOT NULL,
  history MEDIUMBLOB,
  history_encoding VARCHAR(16) NOT NULL,
  new_run_history BLOB,
  new_run_history_encoding VARCHAR(16) NOT NULL DEFAULT 'json',
  event_store_version          INT NOT NULL, -- indiciates which version of event store to query
  new_run_event_store_version  INT NOT NULL, -- indiciates which version of event store to query for new run(continueAsNew)
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, first_event_id)
);


CREATE TABLE tasks (
  domain_id BINARY(16) NOT NULL,
  task_list_name VARCHAR(255) NOT NULL,
  task_type TINYINT NOT NULL, -- {Activity, Decision}
  task_id BIGINT NOT NULL,
  --
  data BLOB NOT NULL,
  data_encoding VARCHAR(16) NOT NULL,
  PRIMARY KEY (domain_id, task_list_name, task_type, task_id)
);
`
}
