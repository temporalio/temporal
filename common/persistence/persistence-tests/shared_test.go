package persistencetests

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

type partialSetupTestCluster struct {
	tornDown bool
}

func (*partialSetupTestCluster) SetupTestDatabase() {}

func (c *partialSetupTestCluster) TearDownTestDatabase() {
	c.tornDown = true
}

func (*partialSetupTestCluster) Config() config.Persistence {
	return config.Persistence{}
}

func TestTearDownWorkflowStoreAllowsPartialSetup(t *testing.T) {
	cluster := &partialSetupTestCluster{}
	testBase := &TestBase{DefaultTestCluster: cluster}

	require.NotPanics(t, testBase.TearDownWorkflowStore)
	require.True(t, cluster.tornDown)
}

func TestTestBaseAcquiresDistinctMainAndVisibilityLeases(t *testing.T) {
	mainCfg := &config.SQL{
		PluginName:   sqlite.PluginName,
		DatabaseName: filepath.Join(t.TempDir(), "main.db"),
	}
	visibilityCfg := &config.SQL{
		PluginName:   sqlite.PluginName,
		DatabaseName: filepath.Join(t.TempDir(), "visibility.db"),
	}
	testBase := &TestBase{Logger: log.NewNoopLogger()}
	cfg := config.Persistence{
		DefaultStore:    "main",
		VisibilityStore: "visibility",
		DataStores: map[string]config.DataStore{
			"main":       {SQL: mainCfg},
			"visibility": {SQL: visibilityCfg},
		},
	}

	require.NoError(t, testBase.acquireDatabaseLeases(cfg))
	for _, sqlCfg := range []*config.SQL{mainCfg, visibilityCfg} {
		stats, supported, err := sql.GetDatabaseLeaseStats(sqlCfg)
		require.NoError(t, err)
		require.True(t, supported)
		require.True(t, stats.Open)
		require.Equal(t, 1, stats.Leases)
	}

	require.NoError(t, testBase.releaseDatabaseLeases())
	for _, sqlCfg := range []*config.SQL{mainCfg, visibilityCfg} {
		stats, supported, err := sql.GetDatabaseLeaseStats(sqlCfg)
		require.NoError(t, err)
		require.True(t, supported)
		require.False(t, stats.Open)
	}
}

func TestGarbageCleanupInfo(t *testing.T) {
	namespaceID := "10000000-5000-f000-f000-000000000000"
	workflowID := "workflow-id"
	runID := "10000000-5000-f000-f000-000000000002"

	info := persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID)
	namespaceID2, workflowID2, runID2, err := persistence.SplitHistoryGarbageCleanupInfo(info)
	if err != nil || namespaceID != namespaceID2 || workflowID != workflowID2 || runID != runID2 {
		t.Fail()
	}
}

func TestGarbageCleanupInfo_WithColonInWorklfowID(t *testing.T) {
	namespaceID := "10000000-5000-f000-f000-000000000000"
	workflowID := "workflow-id:2"
	runID := "10000000-5000-f000-f000-000000000002"

	info := persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID)
	namespaceID2, workflowID2, runID2, err := persistence.SplitHistoryGarbageCleanupInfo(info)
	if err != nil || namespaceID != namespaceID2 || workflowID != workflowID2 || runID != runID2 {
		t.Fail()
	}
}
