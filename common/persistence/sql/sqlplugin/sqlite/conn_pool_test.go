package sqlite

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/resolver"
)

func TestConnPool_ClosesIdleDBAfterDelay(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t)
	dsn, err := buildDSN(cfg)
	require.NoError(t, err)

	db, err := pool.Allocate(cfg, resolver.NewNoopResolver(), log.NewTestLogger(), newConnPoolTestDB)
	require.NoError(t, err)

	pool.Close(cfg)
	requireConnPoolEntryRemoved(t, pool, dsn)

	nextDB, err := pool.Allocate(cfg, resolver.NewNoopResolver(), log.NewTestLogger(), newConnPoolTestDB)
	require.NoError(t, err)
	require.NotSame(t, db, nextDB)

	pool.Close(cfg)
	requireConnPoolEntryRemoved(t, pool, dsn)
}

func TestConnPool_CancelsPendingCloseWhenReused(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t)
	dsn, err := buildDSN(cfg)
	require.NoError(t, err)

	db, err := pool.Allocate(cfg, resolver.NewNoopResolver(), log.NewTestLogger(), newConnPoolTestDB)
	require.NoError(t, err)

	pool.Close(cfg)
	nextDB, err := pool.Allocate(cfg, resolver.NewNoopResolver(), log.NewTestLogger(), newConnPoolTestDB)
	require.NoError(t, err)
	require.Same(t, db, nextDB)

	time.Sleep(closeDelay * 2)
	pool.mu.Lock()
	_, ok := pool.pool[dsn]
	pool.mu.Unlock()
	require.True(t, ok)

	pool.Close(cfg)
	requireConnPoolEntryRemoved(t, pool, dsn)
}

func newConnPoolTestConfig(t *testing.T) *config.SQL {
	t.Helper()
	return &config.SQL{
		PluginName:   PluginName,
		DatabaseName: filepath.Join(t.TempDir(), "test.db"),
		ConnectAttributes: map[string]string{
			"cache": "shared",
		},
	}
}

func newConnPoolTestDB(
	cfg *config.SQL,
	_ resolver.ServiceResolver,
	_ log.Logger,
) (*sqlx.DB, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}
	return sqlx.Open(goSQLDriverName, dsn)
}

func requireConnPoolEntryRemoved(t *testing.T, pool *connPool, dsn string) {
	t.Helper()
	require.Eventually(t, func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		_, ok := pool.pool[dsn]
		return !ok
	}, time.Second, 10*time.Millisecond)
}
