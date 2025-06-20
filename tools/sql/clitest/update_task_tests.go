package clitest

import (
	"path/filepath"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/common/schema/test"
	"go.temporal.io/server/tools/sql"
)

// UpdateSchemaTestSuite defines a test suite
type UpdateSchemaTestSuite struct {
	test.UpdateSchemaTestBase
	host                       string
	port                       string
	pluginName                 string
	sqlQuery                   string
	executionSchemaVersionDir  string
	executionVersion           string
	visibilitySchemaVersionDir string
	visibilityVersion          string
}

// NewUpdateSchemaTestSuite returns a test suite
func NewUpdateSchemaTestSuite(
	host string,
	port string,
	pluginName string,
	sqlQuery string,
	executionSchemaVersionDir string,
	executionVersion string,
	visibilitySchemaVersionDir string,
	visibilityVersion string,
) *UpdateSchemaTestSuite {
	return &UpdateSchemaTestSuite{
		host:                       host,
		port:                       port,
		pluginName:                 pluginName,
		sqlQuery:                   sqlQuery,
		executionSchemaVersionDir:  executionSchemaVersionDir,
		executionVersion:           executionVersion,
		visibilitySchemaVersionDir: visibilitySchemaVersionDir,
		visibilityVersion:          visibilityVersion,
	}
}

// SetupSuite setups test suite
func (s *UpdateSchemaTestSuite) SetupSuite() {
	conn, err := newTestConn("", s.host, s.port, s.pluginName)
	if err != nil {
		s.Logger.Fatal("Error creating CQLClient", tag.Error(err))
	}
	s.SetupSuiteBase(conn, s.pluginName)
}

// TearDownSuite tear down test suite
func (s *UpdateSchemaTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestUpdateSchema test
func (s *UpdateSchemaTestSuite) TestUpdateSchema() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.Nil(err)
	defer conn.Close()
	s.RunUpdateSchemaTest(sql.BuildCLIOptions(), conn, "--db", s.sqlQuery, []string{"executions", "current_executions"})
}

// TestDryrun test
func (s *UpdateSchemaTestSuite) TestDryrun() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.NoError(err)
	defer conn.Close()
	dir, err := filepath.Abs(s.executionSchemaVersionDir)
	s.NoError(err)
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, s.executionVersion)
}

// TestVisibilityDryrun test
func (s *UpdateSchemaTestSuite) TestVisibilityDryrun() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.NoError(err)
	defer conn.Close()
	dir, err := filepath.Abs(s.visibilitySchemaVersionDir)
	s.NoError(err)
	s.RunDryrunTest(sql.BuildCLIOptions(), conn, "--db", dir, s.visibilityVersion)
}
