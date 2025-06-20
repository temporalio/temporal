package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	postgresqlversionV12 "go.temporal.io/server/schema/postgresql/v12"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/sql/clitest"
)

type PostgresqlSuite struct {
	suite.Suite
	pluginName string
}

func (p *PostgresqlSuite) TestPostgreSQLConnTestSuite() {
	suite.Run(p.T(), clitest.NewSQLConnTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLHandlerTestSuite() {
	suite.Run(p.T(), clitest.NewHandlerTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLSetupSchemaTestSuite() {
	p.T().Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	p.T().Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewSetupSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLUpdateSchemaTestSuite() {
	p.T().Setenv("SQL_HOST", environment.GetPostgreSQLAddress())
	p.T().Setenv("SQL_PORT", strconv.Itoa(environment.GetPostgreSQLPort()))
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewUpdateSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
		testPostgreSQLExecutionSchemaVersionDir,
		postgresqlversionV12.Version,
		testPostgreSQLVisibilitySchemaVersionDir,
		postgresqlversionV12.VisibilityVersion,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLVersionTestSuite() {
	p.T().Setenv("SQL_USER", testUser)
	p.T().Setenv("SQL_PASSWORD", testPassword)
	suite.Run(p.T(), clitest.NewVersionTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLExecutionSchemaFile,
		testPostgreSQLVisibilitySchemaFile,
	))
}

func TestPostgres(t *testing.T) {
	s := &PostgresqlSuite{pluginName: postgresql.PluginName}
	suite.Run(t, s)
}

func TestPostgresPGX(t *testing.T) {
	s := &PostgresqlSuite{pluginName: postgresql.PluginNamePGX}
	suite.Run(t, s)
}
