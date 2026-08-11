package persistencetests

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

func TestReusableCassandraDatabaseIsReusedOnlyAfterReset(t *testing.T) {
	pool := newCassandraDatabasePool()
	key := cassandraDatabasePoolKey{host: "localhost", port: 9042}

	name, reused := pool.acquire(key, "first")
	require.Equal(t, "first", name)
	require.False(t, reused)

	firstDatabase := &fakeCassandraTestDatabase{name: name}
	first := newReusableCassandraTestCluster(firstDatabase, key, reused, pool, log.NewNoopLogger())
	first.SetupTestDatabase()
	require.Equal(t, 1, firstDatabase.setupCalls)
	require.Zero(t, firstDatabase.connectCalls)

	_, secondReused := pool.acquire(key, "second")
	require.False(t, secondReused, "a leased database must not be handed to another cluster")

	first.TearDownTestDatabase()
	require.Equal(t, 1, firstDatabase.resetCalls)
	require.Equal(t, 1, firstDatabase.closeCalls)

	name, reused = pool.acquire(key, "third")
	require.Equal(t, "first", name)
	require.True(t, reused)

	reusedDatabase := &fakeCassandraTestDatabase{name: name}
	reusedCluster := newReusableCassandraTestCluster(reusedDatabase, key, reused, pool, log.NewNoopLogger())
	reusedCluster.SetupTestDatabase()
	require.Zero(t, reusedDatabase.setupCalls)
	require.Equal(t, 1, reusedDatabase.connectCalls)
}

func TestReusableCassandraDatabaseDiscardsFailedReset(t *testing.T) {
	pool := newCassandraDatabasePool()
	key := cassandraDatabasePoolKey{host: "localhost", port: 9042}
	name, reused := pool.acquire(key, "dirty")
	require.False(t, reused)

	database := &fakeCassandraTestDatabase{name: name, resetErr: errors.New("reset failed")}
	cluster := newReusableCassandraTestCluster(database, key, reused, pool, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	cluster.TearDownTestDatabase()
	require.Equal(t, 1, database.dropCalls)
	require.Equal(t, 1, database.closeCalls)

	name, reused = pool.acquire(key, "clean")
	require.Equal(t, "clean", name)
	require.False(t, reused)
}

type fakeCassandraTestDatabase struct {
	name         string
	resetErr     error
	setupCalls   int
	connectCalls int
	resetCalls   int
	dropCalls    int
	closeCalls   int
}

func (d *fakeCassandraTestDatabase) SetupTestDatabase() {
	d.setupCalls++
}

func (d *fakeCassandraTestDatabase) TearDownTestDatabase() {
	panic("the reusable wrapper owns teardown")
}

func (d *fakeCassandraTestDatabase) Config() config.Persistence {
	return config.Persistence{}
}

func (d *fakeCassandraTestDatabase) StoreType() string {
	return config.StoreTypeNoSQL
}

func (d *fakeCassandraTestDatabase) DatabaseName() string {
	return d.name
}

func (d *fakeCassandraTestDatabase) OpenTestDatabase() error {
	d.connectCalls++
	return nil
}

func (d *fakeCassandraTestDatabase) ResetTestDatabase() error {
	d.resetCalls++
	return d.resetErr
}

func (d *fakeCassandraTestDatabase) DropTestDatabase() error {
	d.dropCalls++
	return nil
}

func (d *fakeCassandraTestDatabase) CloseSession() {
	d.closeCalls++
}
