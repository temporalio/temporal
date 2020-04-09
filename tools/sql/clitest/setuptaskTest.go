package clitest

import (
	log "github.com/sirupsen/logrus"

	"os"

	"github.com/temporalio/temporal/environment"
	"github.com/temporalio/temporal/tools/common/schema/test"
	"github.com/temporalio/temporal/tools/sql"
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
