package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/resolver"
)

func TestConnPoolLeaseKeepsDatabaseOpenAfterWrappersClose(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())

	lease, err := pool.AcquireLease(cfg)
	require.NoError(t, err)

	db, release, err := pool.Allocate(cfg, resolver.NewNoopResolver(), log.NewNoopLogger(), openConnPoolTestDB)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	require.NoError(t, release())
	require.NoError(t, db.Ping())

	require.NoError(t, lease.Close())
	require.Error(t, db.Ping())
	require.Empty(t, pool.pool)
}

func TestConnPoolClosesDatabaseAfterLastWrapperWithoutLease(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())

	db1, release1 := allocateConnPoolTestDB(t, pool, cfg)
	db2, release2 := allocateConnPoolTestDB(t, pool, cfg)
	require.Same(t, db1, db2)

	require.NoError(t, release1())
	require.NoError(t, db1.Ping())
	require.NoError(t, release2())
	require.Error(t, db1.Ping())
	require.Empty(t, pool.pool)
}

func TestConnPoolDefersCloseUntilWrapperReleases(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())

	lease, err := pool.AcquireLease(cfg)
	require.NoError(t, err)
	db, release := allocateConnPoolTestDB(t, pool, cfg)

	require.NoError(t, lease.Close())
	require.NoError(t, db.Ping())
	require.NoError(t, release())
	require.Error(t, db.Ping())
	require.Empty(t, pool.pool)
}

func TestConnPoolRequiresEveryLeaseToClose(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())

	lease1, err := pool.AcquireLease(cfg)
	require.NoError(t, err)
	lease2, err := pool.AcquireLease(cfg)
	require.NoError(t, err)
	db, release := allocateConnPoolTestDB(t, pool, cfg)
	require.NoError(t, release())

	require.NoError(t, lease1.Close())
	require.NoError(t, db.Ping())
	require.NoError(t, lease2.Close())
	require.Error(t, db.Ping())
	require.Empty(t, pool.pool)
}

func TestConnPoolReleaseHandlesAreIdempotent(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())

	lease, err := pool.AcquireLease(cfg)
	require.NoError(t, err)
	_, release := allocateConnPoolTestDB(t, pool, cfg)

	require.NoError(t, release())
	require.NoError(t, release())
	require.NoError(t, lease.Close())
	require.NoError(t, lease.Close())
	require.Empty(t, pool.pool)
}

func TestConnPoolWrapperReleaseUsesOriginalDSN(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())
	db, release := allocateConnPoolTestDB(t, pool, cfg)

	cfg.DatabaseName = "mutated"

	require.NoError(t, release())
	require.Error(t, db.Ping())
	require.Empty(t, pool.pool)
}

func TestConnPoolUsesOriginalDSNToCreateConnection(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())
	originalDSN, err := buildDSN(cfg)
	require.NoError(t, err)

	db, release, err := pool.Allocate(
		cfg,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		func(
			cfg *config.SQL,
			_ resolver.ServiceResolver,
			_ log.Logger,
			dsn string,
		) (*sqlx.DB, error) {
			cfg.DatabaseName = "mutated"
			return sqlx.Open(goSQLDriverName, dsn)
		},
	)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE sentinel (value INTEGER)")
	require.NoError(t, err)

	originalDB, err := sqlx.Open(goSQLDriverName, originalDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, originalDB.Close()) })
	var tables int
	require.NoError(t, originalDB.Get(
		&tables,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sentinel'",
	))
	require.Equal(t, 1, tables)
	require.NoError(t, release())
}

func TestBuildDSNDoesNotMutateConfig(t *testing.T) {
	cfg := &config.SQL{DatabaseName: t.Name()}

	_, err := buildDSN(cfg)

	require.NoError(t, err)
	require.Nil(t, cfg.ConnectAttributes)
}

func TestConnPoolCreatesFreshDatabaseAfterFinalClose(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())

	lease, err := pool.AcquireLease(cfg)
	require.NoError(t, err)
	db, release := allocateConnPoolTestDB(t, pool, cfg)
	_, err = db.Exec("CREATE TABLE sentinel (value INTEGER)")
	require.NoError(t, err)
	require.NoError(t, release())
	require.NoError(t, lease.Close())

	nextLease, err := pool.AcquireLease(cfg)
	require.NoError(t, err)
	nextDB, nextRelease := allocateConnPoolTestDB(t, pool, cfg)
	var value int
	err = nextDB.Get(&value, "SELECT value FROM sentinel")
	require.ErrorContains(t, err, "no such table")
	require.NoError(t, nextRelease())
	require.NoError(t, nextLease.Close())
}

func TestConnPoolConcurrentAcquireAndRelease(t *testing.T) {
	const goroutines = 100
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := pool.AcquireLease(cfg)
			if err != nil {
				errs <- err
				return
			}
			db, release, err := pool.Allocate(
				cfg,
				resolver.NewNoopResolver(),
				log.NewNoopLogger(),
				openConnPoolTestDB,
			)
			if err == nil {
				err = db.Ping()
			}
			if release != nil {
				err = errors.Join(err, release())
			}
			errs <- errors.Join(err, lease.Close())
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Empty(t, pool.pool)
}

func TestConnPoolUnderflowIsObservable(t *testing.T) {
	pool := newConnPool()
	cfg := newConnPoolTestConfig(t.Name())
	dsn, err := buildDSN(cfg)
	require.NoError(t, err)

	require.Error(t, pool.releaseReference(dsn))
	_, err = pool.AcquireLease(cfg)
	require.NoError(t, err)
	require.NoError(t, pool.releaseLease(dsn))
	require.Error(t, pool.releaseLease(dsn))
	require.Empty(t, pool.pool)
}

func TestConnPoolFinalCloseReleasesFileDatabase(t *testing.T) {
	pool := newConnPool()
	databasePath := filepath.Join(t.TempDir(), "lease.db")
	cfg := &config.SQL{
		PluginName:   PluginName,
		DatabaseName: databasePath,
		ConnectAttributes: map[string]string{
			"journal_mode": "wal",
		},
	}

	lease, err := pool.AcquireLease(cfg)
	require.NoError(t, err)
	db, release := allocateConnPoolTestDB(t, pool, cfg)
	_, err = db.Exec("CREATE TABLE sentinel (value INTEGER)")
	require.NoError(t, err)
	require.NoError(t, release())
	require.NoError(t, lease.Close())
	require.Empty(t, pool.pool)

	require.NoError(t, os.Remove(databasePath))
	for _, suffix := range []string{"-wal", "-shm"} {
		err := os.Remove(databasePath + suffix)
		require.True(t, err == nil || errors.Is(err, os.ErrNotExist))
	}
}

func allocateConnPoolTestDB(
	t *testing.T,
	pool *connPool,
	cfg *config.SQL,
) (*sqlx.DB, func() error) {
	t.Helper()
	db, release, err := pool.Allocate(
		cfg,
		resolver.NewNoopResolver(),
		log.NewNoopLogger(),
		openConnPoolTestDB,
	)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db, release
}

func newConnPoolTestConfig(databaseName string) *config.SQL {
	return &config.SQL{
		PluginName:   PluginName,
		DatabaseName: databaseName,
		ConnectAttributes: map[string]string{
			"cache": "shared",
			"mode":  "memory",
		},
	}
}

func openConnPoolTestDB(
	_ *config.SQL,
	_ resolver.ServiceResolver,
	_ log.Logger,
	dsn string,
) (*sqlx.DB, error) {
	return sqlx.Open(goSQLDriverName, dsn)
}
