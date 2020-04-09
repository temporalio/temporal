package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/environment"
	"github.com/temporalio/temporal/tools/common/schema/test"
)

type (
	CQLClientTestSuite struct {
		test.DBTestBase
	}
)

var _ test.DB = (*cqlClient)(nil)

func TestCQLClientTestSuite(t *testing.T) {
	suite.Run(t, new(CQLClientTestSuite))
}

func (s *CQLClientTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *CQLClientTestSuite) SetupSuite() {
	client, err := newTestCQLClient(systemKeyspace)
	if err != nil {
		s.Log.Fatal("error creating CQLClient, ", tag.Error(err))
	}
	s.SetupSuiteBase(client)
}

func (s *CQLClientTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *CQLClientTestSuite) TestParseCQLFile() {
	s.RunParseFileTest(createTestCQLFileContent())
}

func (s *CQLClientTestSuite) TestCQLClient() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	s.RunCreateTest(client)
	s.RunUpdateTest(client)
	s.RunDropTest(client)
	client.Close()
}

func newTestCQLClient(keyspace string) (*cqlClient, error) {
	return newCQLClient(&CQLClientConfig{
		Hosts:       environment.GetCassandraAddress(),
		Port:        defaultCassandraPort,
		Keyspace:    keyspace,
		Timeout:     defaultTimeout,
		numReplicas: 1,
	})
}

func createTestCQLFileContent() string {
	return `
-- test cql file content

CREATE TABLE events (
  namespace_id      uuid,
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
  PRIMARY KEY ((namespace_id, workflow_id, run_id), first_event_id)
);

-- Stores activity or workflow tasks
CREATE TABLE tasks (
  namespace_id        uuid,
  task_list_name   text,
  task_list_type   int, -- enum TaskListType {ActivityTask, DecisionTask}
  type             int, -- enum rowType {Task, TaskList}
  task_id          bigint,  -- unique identifier for tasks, monotonically increasing
  range_id         bigint static, -- Used to ensure that only one process can write to the table
  task             text,
  task_list        text,
  PRIMARY KEY ((namespace_id, task_list_name, task_list_type), type, task_id)
);

`
}
