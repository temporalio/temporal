package clitest

import (
	"net"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/common/schema/test"
	"go.temporal.io/server/tools/sql"
)

type (
	// SQLConnTestSuite defines a test suite
	SQLConnTestSuite struct {
		test.DBTestBase
		host       string
		port       string
		pluginName string
		sqlQuery   string
	}
)

var _ test.DB = (*sql.Connection)(nil)

const (
	testUser     = "temporal"
	testPassword = "temporal"
)

// NewSQLConnTestSuite returns the test suite
func NewSQLConnTestSuite(
	host string,
	port string,
	pluginName string,
	sqlQuery string,
) *SQLConnTestSuite {
	return &SQLConnTestSuite{
		host:       host,
		port:       port,
		pluginName: pluginName,
		sqlQuery:   sqlQuery,
	}
}

// SetupTest setups test
func (s *SQLConnTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

// SetupSuite setups test suite
func (s *SQLConnTestSuite) SetupSuite() {
	conn, err := newTestConn("", s.host, s.port, s.pluginName)
	if err != nil {
		logger := log.NewTestLogger()
		logger.Fatal("error creating sql conn", tag.Error(err))
	}
	s.SetupSuiteBase(conn)
}

// TearDownSuite tear down test suite
func (s *SQLConnTestSuite) TearDownSuite() {
	s.TearDownSuiteBase()
}

// TestParseCQLFile test
func (s *SQLConnTestSuite) TestParseCQLFile() {
	s.RunParseFileTest(s.sqlQuery)
}

// TestSQLConn test
func (s *SQLConnTestSuite) TestSQLConn() {
	conn, err := newTestConn(s.DBName, s.host, s.port, s.pluginName)
	s.Nil(err)
	s.RunCreateTest(conn)
	s.RunUpdateTest(conn)
	s.RunDropTest(conn)
	conn.Close()
}

func newTestConn(
	database string,
	host string,
	port string,
	pluginName string,
) (*sql.Connection, error) {
	connectAddr := net.JoinHostPort(host, port)
	return sql.NewConnection(&config.SQL{
		ConnectAddr:  connectAddr,
		User:         testUser,
		Password:     testPassword,
		PluginName:   pluginName,
		DatabaseName: database,
	}, log.NewTestLogger())
}
