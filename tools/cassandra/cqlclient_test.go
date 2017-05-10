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

package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	"time"
)

type (
	CQLClientTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		keyspace string
		session  *gocql.Session
		client   CQLClient
		log      bark.Logger
	}
)

func TestCQLClientTestSuite(t *testing.T) {
	suite.Run(t, new(CQLClientTestSuite))
}

func (s *CQLClientTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *CQLClientTestSuite) SetupSuite() {
	s.log = bark.NewLoggerFromLogrus(log.New())
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	s.keyspace = fmt.Sprintf("cql_client_test_%v", rand.Int63())

	client, err := newCQLClient("127.0.0.1", "system")
	if err != nil {
		log.Fatalf("error creating CQLClient, err=%v", err)
	}

	err = client.CreateKeyspace(s.keyspace, 1)
	if err != nil {
		log.Fatalf("error creating keyspace, err=%v", err)
	}

	s.client = client
}

func (s *CQLClientTestSuite) TearDownSuite() {
	s.client.Exec("DROP keyspace " + s.keyspace)
}

func (s *CQLClientTestSuite) TestParseCQLFile() {
	rootDir, err := ioutil.TempDir("", "cqlClientTestDir")
	s.Nil(err)
	defer os.Remove(rootDir)

	cqlFile, err := ioutil.TempFile(rootDir, "parseCQLTest")
	s.Nil(err)
	defer os.Remove(cqlFile.Name())

	cqlFile.WriteString(createTestCQLFileContent())
	stmts, err := ParseCQLFile(cqlFile.Name())
	s.Nil(err)
	s.Equal(2, len(stmts), "wrong number of cql statements")
}

func (s *CQLClientTestSuite) testUpdate(client CQLClient) {
	// Update / Read schema version test
	err := client.UpdateSchemaVersion(10, 5)
	s.Nil(err)
	err = client.WriteSchemaUpdateLog(9, 10, "abc", "test")
	s.Nil(err)

	ver, err := client.ReadSchemaVersion()
	s.Nil(err)
	s.Equal(10, int(ver))

	err = client.UpdateSchemaVersion(12, 5)
	ver, err = client.ReadSchemaVersion()
	s.Nil(err)
	s.Equal(12, int(ver))
}

func (s *CQLClientTestSuite) testDrop(client CQLClient) {

	tables, err := client.ListTables()
	s.Nil(err)
	s.True(len(tables) > 0)

	types, err := client.ListTypes()
	s.Nil(err)
	s.True(len(types) > 0)

	// Drop table / type test
	for _, t := range tables {
		err1 := client.DropTable(t)
		s.Nil(err1)
	}
	for _, t := range types {
		err1 := client.DropType(t)
		s.Nil(err1)
	}

	tables, err = client.ListTables()
	s.Nil(err)
	s.Equal(0, len(tables))

	types, err = client.ListTypes()
	s.Nil(err)
	s.Equal(0, len(types))

	_, err = client.ReadSchemaVersion()
	s.NotNil(err)
}

func (s *CQLClientTestSuite) testCreate(client CQLClient) {

	tables, err := client.ListTables()
	s.Nil(err)
	s.Equal(0, len(tables))

	err = client.CreateSchemaVersionTables()
	s.Nil(err)

	expectedTables := make(map[string]struct{})
	expectedTables["schema_version"] = struct{}{}
	expectedTables["schema_update_history"] = struct{}{}

	tables, err = client.ListTables()
	s.Nil(err)
	s.Equal(len(expectedTables), len(tables))

	for _, t := range tables {
		_, ok := expectedTables[t]
		s.True(ok)
		delete(expectedTables, t)
	}
	s.Equal(0, len(expectedTables))

	types, err := client.ListTypes()
	s.Nil(err)
	s.Equal(0, len(types))

	err = client.Exec("CREATE TYPE name(first text, second text);")
	s.Nil(err)
	err = client.Exec("CREATE TYPE city(name text, zip text);")
	s.Nil(err)

	expectedTypes := make(map[string]struct{})
	expectedTypes["name"] = struct{}{}
	expectedTypes["city"] = struct{}{}

	types, err = client.ListTypes()
	s.Nil(err)
	s.Equal(len(expectedTypes), len(types))

	for _, t := range types {
		_, ok := expectedTypes[t]
		s.True(ok)
		delete(expectedTypes, t)
	}
	s.Equal(0, len(expectedTypes))
}

func (s *CQLClientTestSuite) TestCQLClient() {
	client, err := newCQLClient("127.0.0.1", s.keyspace)
	s.Nil(err)
	s.testCreate(client)
	s.testUpdate(client)
	s.testDrop(client)
}

func createTestCQLFileContent() string {
	return `
-- test cql file content

CREATE TABLE events (
  domain_id      uuid,
  workflow_id    text,
  run_id         uuid,
  -- We insert a batch of events with each append transaction.
  -- This field stores the event id of first event in the batch.
  first_event_id bigint,
  range_id       bigint,
  tx_id          bigint,
  data           blob, -- Batch of workflow execution history events as a blob
  data_encoding  text, -- Protocol used for history serialization
  data_version   int,  -- history blob version
  PRIMARY KEY ((domain_id, workflow_id, run_id), first_event_id)
);

-- Stores activity or workflow tasks
CREATE TABLE tasks (
  domain_id        uuid,
  task_list_name   text,
  task_list_type   int, -- enum TaskListType {ActivityTask, DecisionTask}
  type             int, -- enum rowType {Task, TaskList}
  task_id          bigint,  -- unique identifier for tasks, monotonically increasing
  range_id         bigint static, -- Used to ensure that only one process can write to the table
  task             text,
  task_list        text,
  PRIMARY KEY ((domain_id, task_list_name, task_list_type), type, task_id)
);

`
}
