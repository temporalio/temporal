package persistencetests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

func TestTestBaseLeasePreservesDatabaseCreatedDuringSetup(t *testing.T) {
	base := NewTestBaseWithSQL(GetSQLiteMemoryTestClusterOption())
	base.DefaultTestCluster = &sentinelTestCluster{
		PersistenceTestCluster: base.DefaultTestCluster,
		t:                      t,
	}
	base.Setup(nil)
	t.Cleanup(base.TearDownWorkflowStore)

	cfg := base.DefaultTestCluster.Config()
	db, err := sql.NewSQLAdminDB(
		sqlplugin.DbKindUnknown,
		cfg.DataStores[cfg.DefaultStore].SQL,
		resolver.NewNoopResolver(),
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.Exec("INSERT INTO lease_sentinel (value) VALUES (1)"))
}

type sentinelTestCluster struct {
	PersistenceTestCluster
	t *testing.T
}

func (c *sentinelTestCluster) SetupTestDatabase() {
	c.PersistenceTestCluster.SetupTestDatabase()
	cfg := c.Config()
	db, err := sql.NewSQLAdminDB(
		sqlplugin.DbKindUnknown,
		cfg.DataStores[cfg.DefaultStore].SQL,
		resolver.NewNoopResolver(),
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(c.t, err)
	require.NoError(c.t, db.Exec("CREATE TABLE lease_sentinel (value INTEGER)"))
	require.NoError(c.t, db.Close())
}
