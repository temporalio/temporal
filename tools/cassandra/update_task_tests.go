package cassandra

import (
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/schema/cassandra"
	"go.temporal.io/server/tools/common/schema/test"
)

type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
}

func (s *UpdateSchemaTestSuite) SetupSuite() {
	client, err := newTestCQLClient(systemKeyspace)
	if err != nil {
		s.Logger.Fatal("Error creating CQLClient", tag.Error(err))
	}
	s.SetupSuiteBase(client, "")
}

func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	s.RunUpdateSchemaTest(buildCLIOptions(), client, "-k", createTestCQLFileContent(), []string{"events", "tasks"})
}

func (s *UpdateSchemaTestSuite) TestDryrun() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	dir := "../../schema/cassandra/temporal/versioned"
	s.RunDryrunTest(buildCLIOptions(), client, "-k", dir, cassandra.Version)
}
