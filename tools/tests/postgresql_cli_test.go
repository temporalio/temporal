package tests

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	postgresqlversionV12 "go.temporal.io/server/schema/postgresql/v12"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/sql"
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
	suite.Run(p.T(), clitest.NewSetupSchemaTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLQuery,
	))
}

func (p *PostgresqlSuite) TestPostgreSQLUpdateSchemaTestSuite() {
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
	suite.Run(p.T(), clitest.NewVersionTestSuite(
		environment.GetPostgreSQLAddress(),
		strconv.Itoa(environment.GetPostgreSQLPort()),
		p.pluginName,
		testPostgreSQLExecutionSchemaFile,
		testPostgreSQLVisibilitySchemaFile,
	))
}

// setup-schema --overwrite must only drop tables in the first schema on search_path,
// and leave tables in other schemas alone.
func (p *PostgresqlSuite) TestPostgreSQLSetupSchemaOverwriteWithSearchPath() {
	host := environment.GetPostgreSQLAddress()
	port := strconv.Itoa(environment.GetPostgreSQLPort())
	schemaFile := filepath.Join(p.T().TempDir(), "schema.sql")
	p.Require().NoError(os.WriteFile(schemaFile, []byte(testPostgreSQLQuery), 0o600))

	for _, tc := range []struct {
		name       string
		searchPath string
	}{
		{name: "custom_public", searchPath: "custom,public"},
		{name: "custom", searchPath: "custom"},
	} {
		p.Run(tc.name, func() {
			t := p.T()
			dbName := "search_path_" + p.pluginName + "_" + tc.name
			newConn := func(database string, connectAttributes map[string]string) *sql.Connection {
				conn, err := sql.NewConnection(&config.SQL{
					ConnectAddr:       net.JoinHostPort(host, port),
					User:              "temporal",
					Password:          "temporal",
					PluginName:        p.pluginName,
					DatabaseName:      database,
					ConnectAttributes: connectAttributes,
				}, log.NewTestLogger())
				require.NoError(t, err)
				t.Cleanup(conn.Close)
				return conn
			}

			adminConn := newConn("", nil)
			require.NoError(t, adminConn.DropDatabase(dbName))
			require.NoError(t, adminConn.CreateDatabase(dbName))
			t.Cleanup(func() { require.NoError(t, adminConn.DropDatabase(dbName)) })

			conn := newConn(dbName, map[string]string{"search_path": tc.searchPath})
			require.NoError(t, conn.Exec("CREATE SCHEMA custom"))
			require.NoError(t, conn.Exec("CREATE TABLE public.sentinel (id INT)"))

			// -q stops the tool from calling os.Exit on error, so the checks below verify the outcome.
			setupSchema := func(args ...string) {
				require.NoError(t, sql.RunTool(append([]string{
					"./tool",
					"--ep", host,
					"--p", port,
					"-u", "temporal",
					"--pw", "temporal",
					"--pl", p.pluginName,
					"--db", dbName,
					"--ca", "search_path=" + tc.searchPath,
					"-q",
					"setup-schema",
					"-f", schemaFile,
				}, args...)))
			}
			setupSchema("-version", "1.0")
			setupSchema("-version", "2.0", "-o")

			require.NoError(t, conn.Exec("SELECT 1 FROM public.sentinel"))
			version, err := conn.ReadSchemaVersion()
			require.NoError(t, err)
			require.Equal(t, "2.0", version)
			tables, err := conn.ListTables()
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"executions", "current_executions", "schema_version", "schema_update_history"}, tables)
		})
	}
}

func TestPostgres(t *testing.T) {
	t.Parallel()
	s := &PostgresqlSuite{pluginName: postgresql.PluginName}
	suite.Run(t, s)
}

func TestPostgresPGX(t *testing.T) {
	t.Parallel()
	s := &PostgresqlSuite{pluginName: postgresql.PluginNamePGX}
	suite.Run(t, s)
}
