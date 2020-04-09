package clitest

import (
	"log"
	"os"

	"github.com/temporalio/temporal/environment"
	"github.com/temporalio/temporal/schema/mysql"
	"github.com/temporalio/temporal/tools/common/schema/test"
	"github.com/temporalio/temporal/tools/sql"
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
	dir := "../../../../../schema/mysql/v57/temporal/versioned"
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
