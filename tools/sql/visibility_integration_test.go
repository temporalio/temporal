//go:build integration

package sql

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/resolver"
	mysqlschema "go.temporal.io/server/schema/mysql/v8"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tools/common/schema"
)

func TestMySQLCombinedSchemaVersions(t *testing.T) {
	t.Parallel()
	logger := log.NewNoopLogger()
	cfg := config.SQL{
		ConnectAddr: net.JoinHostPort(environment.GetMySQLAddress(), strconv.Itoa(environment.GetMySQLPort())),
		User:        "temporal", Password: "temporal", PluginName: mysql.PluginName,
	}
	admin, err := NewConnection(&cfg, logger)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	database := "visibility_version_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, admin.CreateDatabase(database))
	t.Cleanup(func() { require.NoError(t, admin.DropDatabase(database)) })
	cfg.DatabaseName = database
	core, err := NewConnection(&cfg, logger)
	require.NoError(t, err)
	t.Cleanup(core.Close)
	visibility, err := newConnection(&cfg, logger, true)
	require.NoError(t, err)
	t.Cleanup(visibility.Close)

	require.NoError(t, schema.NewSetupSchemaTask(core, &schema.SetupConfig{InitialVersion: "0.0"}, logger).Run())
	require.NoError(t, schema.NewUpdateSchemaTask(core, &schema.UpdateConfig{SchemaName: "mysql/v8/temporal"}, logger).Run())
	version, err := core.ReadSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, mysqlschema.Version, version)

	// Visibility must not mistake the core schema version for its own version.
	_, err = visibility.ReadSchemaVersion()
	require.Error(t, err)
	require.NoError(t, schema.NewSetupSchemaTask(visibility, &schema.SetupConfig{InitialVersion: "0.0"}, logger).Run())
	require.NoError(t, schema.NewUpdateSchemaTask(visibility, &schema.UpdateConfig{SchemaName: "mysql/v8/visibility", TargetVersion: "1.0"}, logger).Run())
	version, err = visibility.ReadSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, "1.0", version)
	require.NoError(t, schema.NewUpdateSchemaTask(visibility, &schema.UpdateConfig{SchemaName: "mysql/v8/visibility"}, logger).Run())
	version, err = visibility.ReadSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, mysqlschema.VisibilityVersion, version)
	version, err = core.ReadSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, mysqlschema.Version, version)
	tables, err := core.ListTables()
	require.NoError(t, err)
	for _, table := range []string{"schema_version", "schema_update_history", "visibility_version", "visibility_update_history", "executions", "executions_visibility"} {
		require.Contains(t, tables, table)
	}
	for _, kind := range []sqlplugin.DbKind{sqlplugin.DbKindMain, sqlplugin.DbKindVisibility} {
		db, err := persistencesql.NewSQLAdminDB(kind, &cfg, resolver.NewNoopResolver(), logger, metrics.NoopMetricsHandler)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.NoError(t, db.VerifyVersion())
		if kind == sqlplugin.DbKindVisibility {
			// A valid core version must not hide an outdated or empty visibility version.
			require.NoError(t, visibility.UpdateSchemaVersion("0.0", "0.0"))
			require.ErrorContains(t, db.VerifyVersion(), "version mismatch")
			require.NoError(t, core.Exec("DELETE FROM visibility_version"))
			require.Error(t, db.VerifyVersion())
			// A missing table retains compatibility with legacy dedicated visibility databases.
			require.NoError(t, core.DropTable("visibility_version"))
			require.NoError(t, core.UpdateSchemaVersion(mysqlschema.VisibilityVersion, "0.0"))
			require.NoError(t, db.VerifyVersion())
		}
	}
}
