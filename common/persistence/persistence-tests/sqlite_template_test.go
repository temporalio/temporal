package persistencetests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
)

func TestSQLiteTemplateTestCluster(t *testing.T) {
	options := GetSQLiteTemplateTestClusterOption()
	options.DBName = filepath.Join(t.TempDir(), "test.db")

	testBase := NewTestBaseWithSQL(options)
	cluster, ok := testBase.DefaultTestCluster.(*sqliteTemplateTestCluster)
	require.True(t, ok)

	cluster.SetupTestDatabase()
	require.FileExists(t, options.DBName)

	db, err := sql.NewSQLAdminDB(
		sqlplugin.DbKindMain,
		cluster.Config().DataStores["test"].SQL,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)

	tables, err := db.ListTables(options.DBName)
	require.NoError(t, err)
	require.Contains(t, tables, "namespaces")
	require.Contains(t, tables, "executions_visibility")
	require.NoError(t, db.Close())

	cluster.TearDownTestDatabase()
	_, err = os.Stat(options.DBName)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestGetTestClusterOptionUsesSQLiteTemplate(t *testing.T) {
	options := GetTestClusterOption(config.StoreTypeSQL, sqlite.PluginName)

	require.Equal(t, sqlite.PluginName, options.SQLDBPluginName)
	require.Empty(t, options.SchemaDir)
	require.NotEqual(t, "memory", options.ConnectAttributes["mode"])
	require.Equal(t, "off", options.ConnectAttributes["synchronous"])
}

func TestTestBaseReleasesSQLiteDatabaseLeaseBeforeRemovingFile(t *testing.T) {
	options := GetSQLiteTemplateTestClusterOption()
	options.DBName = filepath.Join(t.TempDir(), "leased.db")
	testBase := NewTestBaseWithSQL(options)

	testBase.Setup(nil)
	cfg := testBase.DefaultTestCluster.Config()
	sqlCfg := cfg.DataStores[cfg.DefaultStore].SQL
	stats, supported, err := sql.GetDatabaseLeaseStats(sqlCfg)
	require.NoError(t, err)
	require.True(t, supported)
	require.True(t, stats.Open)
	require.Equal(t, 1, stats.Leases)
	require.FileExists(t, options.DBName)

	testBase.TearDownWorkflowStore()
	stats, supported, err = sql.GetDatabaseLeaseStats(sqlCfg)
	require.NoError(t, err)
	require.True(t, supported)
	require.False(t, stats.Open)
	require.NoFileExists(t, options.DBName)
}

func TestRepeatedTestBaseTeardownDoesNotRetainSQLiteDatabases(t *testing.T) {
	for i := range 5 {
		options := GetSQLiteTemplateTestClusterOption()
		options.DBName = filepath.Join(t.TempDir(), fmt.Sprintf("leased-%d.db", i))
		testBase := NewTestBaseWithSQL(options)
		testBase.Setup(nil)
		cfg := testBase.DefaultTestCluster.Config()
		sqlCfg := cfg.DataStores[cfg.DefaultStore].SQL

		testBase.TearDownWorkflowStore()
		stats, supported, err := sql.GetDatabaseLeaseStats(sqlCfg)
		require.NoError(t, err)
		require.True(t, supported)
		require.False(t, stats.Open)
	}
}
