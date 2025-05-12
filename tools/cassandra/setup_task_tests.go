package cassandra

import (
	"os"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/common/schema/test"
)

type (
	SetupSchemaTestSuite struct {
		test.SetupSchemaTestBase
		client *cqlClient
	}
)

func (s *SetupSchemaTestSuite) SetupSuite() {
	if err := os.Setenv("CASSANDRA_HOST", environment.GetCassandraAddress()); err != nil {
		s.Logger.Fatal("Failed to set CASSANDRA_HOST", tag.Error(err))
	}
	client, err := newTestCQLClient(systemKeyspace)
	if err != nil {
		s.Logger.Fatal("Error creating CQLClient", tag.Error(err))
	}
	s.client = client
	s.SetupSuiteBase(client, "")
}

func (s *SetupSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *SetupSchemaTestSuite) TestCreateKeyspace() {
	s.Nil(RunTool([]string{"./tool", "create", "-k", "foobar123", "--rf", "1"}))
	err := s.client.dropKeyspace("foobar123")
	s.Nil(err)
}

func (s *SetupSchemaTestSuite) TestSetupSchema() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	s.RunSetupTest(buildCLIOptions(), client, "-k", createTestCQLFileContent(), []string{"tasks", "events"})
}
