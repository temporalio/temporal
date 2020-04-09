package cassandra

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/environment"
	"github.com/temporalio/temporal/tools/common/schema/test"
)

type (
	SetupSchemaTestSuite struct {
		test.SetupSchemaTestBase
		client *cqlClient
	}
)

func TestSetupSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(SetupSchemaTestSuite))
}

func (s *SetupSchemaTestSuite) SetupSuite() {
	os.Setenv("CASSANDRA_HOST", environment.GetCassandraAddress())
	client, err := newTestCQLClient(systemKeyspace)
	if err != nil {
		log.Fatal("Error creating CQLClient")
	}
	s.client = client
	s.SetupSuiteBase(client)
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
