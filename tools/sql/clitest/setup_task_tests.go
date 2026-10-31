package clitest

import (
	"net"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
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
	s.SetupSuiteBase(conn, s.pluginName, test.ConnectParams{
		Host:     s.host,
		Port:     s.port,
		User:     testUser,
		Password: testPassword,
	})
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

func (s *SetupSchemaTestSuite) TestCreateDatabaseWithPasswordCommand() {
	testDatabase := testCLIDatabasePrefix + shuffle.String(testCLIDatabaseSuffix)
	err := sql.RunTool([]string{
		"./tool",
		"--ep", s.host,
		"--p", s.port,
		"-u", testUser,
		"--password-command", "echo",
		"--password-command-arg", testPassword,
		"--pl", s.pluginName,
		"--db", testDatabase,
		"create",
	})
	s.NoError(err)
	err = s.conn.DropDatabase(testDatabase)
	s.NoError(err)
}

func (s *SetupSchemaTestSuite) TestCreateDatabaseWithWrongPasswordFromCommand() {
	err := sql.DoCreateDatabase(&config.SQL{
		ConnectAddr: net.JoinHostPort(s.host, s.port),
		User:        testUser,
		PasswordCommand: &config.PasswordCommandConfig{
			Command: "echo",
			Args:    []string{"wrong-password"},
		},
		PluginName:   s.pluginName,
		DatabaseName: testCLIDatabasePrefix + shuffle.String(testCLIDatabaseSuffix),
	}, "", log.NewTestLogger())
	s.Error(err)
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
