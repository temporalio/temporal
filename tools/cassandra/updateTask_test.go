package cassandra

import (
	"log"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/schema/cassandra"
	"github.com/temporalio/temporal/tools/common/schema/test"
)

type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
}

func TestUpdateSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(UpdateSchemaTestSuite))
}

func (s *UpdateSchemaTestSuite) SetupSuite() {
	client, err := newTestCQLClient(systemKeyspace)
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.SetupSuiteBase(client)
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

func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	client, err := newTestCQLClient(s.DBName)
	s.Nil(err)
	defer client.Close()
	dir := "../../schema/cassandra/visibility/versioned"
	s.RunDryrunTest(buildCLIOptions(), client, "-k", dir, cassandra.VisibilityVersion)
}
