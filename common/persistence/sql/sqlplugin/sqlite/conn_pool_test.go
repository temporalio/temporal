package sqlite

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

func TestDropDatabase_ClosesPooledConnections(t *testing.T) {
	t.Parallel()

	p := &plugin{connPool: newConnPool()}
	cfg := func(name string) *config.SQL {
		return &config.SQL{
			PluginName:        PluginName,
			DatabaseName:      name,
			ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
		}
	}
	createDB := func(name string) *db {
		genericDB, err := p.CreateDB(sqlplugin.DbKindMain, cfg(name), resolver.NewNoopResolver(), log.NewNoopLogger(), nil)
		require.NoError(t, err)
		return genericDB.(*db)
	}

	first := createDB("drop_target")
	second := createDB("drop_target")
	other := createDB("drop_other")
	require.Same(t, first.db, second.db)
	require.Equal(t, 2, p.connPool.pool[mustBuildDSN(t, cfg("drop_target"))].refCount)

	require.NoError(t, other.DropDatabase("drop_target"))

	require.Len(t, p.connPool.pool, 1)
	require.ErrorContains(t, first.db.Ping(), "database is closed")
	require.NoError(t, other.db.Ping())
	require.NotSame(t, first.db, createDB("drop_target").db)
}

func mustBuildDSN(t *testing.T, cfg *config.SQL) string {
	t.Helper()
	dsn, err := buildDSN(cfg)
	require.NoError(t, err)
	return dsn
}
