package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	mysqlversionV8 "go.temporal.io/server/schema/mysql/v8"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/sql/clitest"
)

func TestMySQLConnTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewSQLConnTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLQuery,
	))
}

func TestMySQLHandlerTestSuite(t *testing.T) {
	suite.Run(t, clitest.NewHandlerTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
	))
}

func TestMySQLSetupSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetMySQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetMySQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewSetupSchemaTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLQuery,
	))
}

func TestMySQLUpdateSchemaTestSuite(t *testing.T) {
	t.Setenv("SQL_HOST", environment.GetMySQLAddress())
	t.Setenv("SQL_PORT", strconv.Itoa(environment.GetMySQLPort()))
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewUpdateSchemaTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLQuery,
		testMySQLExecutionSchemaVersionDir,
		mysqlversionV8.Version,
		testMySQLVisibilitySchemaVersionDir,
		mysqlversionV8.VisibilityVersion,
	))
}

func TestMySQLVersionTestSuite(t *testing.T) {
	t.Setenv("SQL_USER", testUser)
	t.Setenv("SQL_PASSWORD", testPassword)
	suite.Run(t, clitest.NewVersionTestSuite(
		environment.GetMySQLAddress(),
		strconv.Itoa(environment.GetMySQLPort()),
		mysql.PluginName,
		testMySQLExecutionSchemaFile,
		testMySQLVisibilitySchemaFile,
	))
}
