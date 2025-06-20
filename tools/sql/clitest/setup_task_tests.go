package clitest

import (
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/tools/common/schema/test"
	"go.temporal.io/server/tools/sql"
)

type (
	// SetupSchemaTestSuite defines a test suite
	SetupSchemaTestSuite struct {
		test.SetupSchemaTestBase
		host       string
		port       string
		pluginName string
		sqlQuery   string

		conn *sql.Connection
	}
)

const (
	testCLIDatabasePrefix = "test_"
	testCLIDatabaseSuffix = "cli_database"
)

// NewSetupSchemaTestSuite returns a test suite
func NewSetupSchemaTestSuite(
	host string,
	port string,
	pluginName string,
	sqlQuery string,
) *SetupSchemaTestSuite {
	return &SetupSchemaTestSuite{
		host:       host,
		port:       port,
		pluginName: pluginName,
		sqlQuery:   sqlQuery,
	}
}

// SetupSuite setup test suite
func (s *SetupSchemaTestSuite) SetupSuite() {
	conn, err := newTestConn("", s.host, s.port, s.pluginName)
	if err != nil {
		s.Fail("error creating sql connection:%v", err)
	}
	s.conn = conn
	s.SetupSuiteBase(conn, s.pluginName)
}

// TearDownSuite tear down test suite
func (s *SetupSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestCreateDatabase test
func (s *SetupSchemaTestSuite) TestCreateDatabase() {
	testDatabase := testCLIDatabasePrefix + shuffle.String(testCLIDatabaseSuffix)
	err := sql.RunTool([]string{
		"./tool",
		"--ep", s.host,
		"--p", s.port,
		"-u", testUser,
		"--pw", testPassword,
		"--pl", s.pluginName,
		"--db", testDatabase,
		"create",
	})
	s.NoError(err)
	err = s.conn.DropDatabase(testDatabase)
	s.NoError(err)
}

func (s *SetupSchemaTestSuite) TestCreateDatabaseIdempotent() {
	testDatabase := testCLIDatabasePrefix + shuffle.String(testCLIDatabaseSuffix)
	err := sql.RunTool([]string{
		"./tool",
		"--ep", s.host,
		"--p", s.port,
		"-u", testUser,
		"--pw", testPassword,
		"--pl", s.pluginName,
		"--db", testDatabase,
		"create",
	})
	s.NoError(err)

	err = sql.RunTool([]string{
		"./tool",
		"--ep", s.host,
		"--p", s.port,
		"-u", testUser,
		"--pw", testPassword,
		"--pl", s.pluginName,
		"--db", testDatabase,
		"create",
	})
	s.NoError(err)

	err = s.conn.DropDatabase(testDatabase)
	s.NoError(err)
}

// TestSetupSchema test
func (s *SetupSchemaTestSuite) TestSetupSchema() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.NoError(err)
	defer conn.Close()
	s.RunSetupTest(sql.BuildCLIOptions(), conn, "--db", s.sqlQuery, []string{"executions", "current_executions"})
}
