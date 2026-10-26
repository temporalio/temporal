package testcore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/freeport"
)

type persistentDatabaseProbe struct{ setups, teardowns int }

func (db *persistentDatabaseProbe) SetupTestDatabase()      { db.setups++ }
func (db *persistentDatabaseProbe) TearDownTestDatabase()   { db.teardowns++ }
func (*persistentDatabaseProbe) Config() config.Persistence { return config.Persistence{} }

func TestPersistentDatabaseLifecycle(t *testing.T) {
	underlying := &persistentDatabaseProbe{}
	first := &persistentDatabase{PersistenceTestCluster: underlying, initialize: true}
	first.SetupTestDatabase()
	first.TearDownTestDatabase()
	require.Equal(t, 1, underlying.setups)
	require.Zero(t, underlying.teardowns)
	reopen := &persistentDatabase{PersistenceTestCluster: underlying}
	reopen.SetupTestDatabase()
	reopen.TearDownTestDatabase()
	require.Equal(t, 1, underlying.setups)
	require.Zero(t, underlying.teardowns)
}

func TestPersistentClusterRejectsUnsafeLifecycle(t *testing.T) {
	for _, database := range []string{"", "relative.db", ":memory:", "/"} {
		_, err := NewPersistentTestCluster(t, &TestClusterConfig{Persistence: persistencetests.TestBaseOptions{SQLDBPluginName: "sqlite", DBName: database}}, log.NewNoopLogger(), true, nil, "")
		require.Error(t, err)
	}
	database := filepath.Join(t.TempDir(), "missing.db")
	cfg := &TestClusterConfig{Persistence: persistencetests.TestBaseOptions{SQLDBPluginName: "sqlite", DBName: database}}
	_, err := NewPersistentTestCluster(t, cfg, log.NewNoopLogger(), false, nil, "")
	require.ErrorContains(t, err, "existing database")
	require.NoError(t, os.WriteFile(database, []byte("owned database"), 0600))
	_, err = NewPersistentTestCluster(t, cfg, log.NewNoopLogger(), true, nil, "")
	require.ErrorContains(t, err, "new database")
	contents, err := os.ReadFile(database)
	require.NoError(t, err)
	require.Equal(t, "owned database", string(contents))
	for _, address := range []string{"127.0.0.1:0", "127.0.0.1:not-a-port", "127.0.0.1:65536", "localhost:7233"} {
		require.Error(t, validatePersistentAddress(address))
	}
	require.NoError(t, validatePersistentAddress("127.0.0.1:7233"))
}

func TestPersistentClusterReopenPreservesMetadata(t *testing.T) {
	database := filepath.Join(t.TempDir(), "temporal.db")
	cfg := &TestClusterConfig{
		DisablePProf:  true,
		WorkerConfig:  WorkerConfig{DisableWorker: true},
		HistoryConfig: HistoryConfig{NumHistoryShards: 1},
		ClusterMetadata: cluster.Config{CurrentClusterName: "persistent", MasterClusterName: "persistent", FailoverVersionIncrement: 10,
			ClusterInformation: map[string]cluster.ClusterInformation{"persistent": {Enabled: true, InitialFailoverVersion: 1}}},
		Persistence: persistencetests.TestBaseOptions{StoreType: "sql", SQLDBPluginName: "sqlite", DBName: database,
			SchemaDir: "schema/sqlite/v3", ConnectAttributes: map[string]string{"journal_mode": "wal", "synchronous": "full"}},
	}
	hosts := map[primitives.ServiceName]static.Hosts{}
	for _, name := range []primitives.ServiceName{primitives.FrontendService, primitives.HistoryService, primitives.MatchingService, primitives.WorkerService} {
		hosts[name] = static.Hosts{All: []string{fmt.Sprintf("127.0.0.1:%d", freeport.MustGetFreePort())}}
	}
	http := fmt.Sprintf("127.0.0.1:%d", freeport.MustGetFreePort())
	first, err := NewPersistentTestCluster(t, cfg, log.NewNoopLogger(), true, hosts, http)
	require.NoError(t, err)
	metadata, err := first.TestBase().ClusterMetadataManager.GetClusterMetadata(context.Background(), &persistence.GetClusterMetadataRequest{ClusterName: first.ClusterName()})
	require.NoError(t, err)
	require.NotEmpty(t, metadata.ClusterId)
	require.NoError(t, first.TearDownCluster())
	_, err = os.Stat(database)
	require.NoError(t, err)
	second, err := NewPersistentTestCluster(t, cfg, log.NewNoopLogger(), false, hosts, http)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.TearDownCluster()) }()
	recovered, err := second.TestBase().ClusterMetadataManager.GetClusterMetadata(context.Background(), &persistence.GetClusterMetadataRequest{ClusterName: second.ClusterName()})
	require.NoError(t, err)
	require.Equal(t, metadata.ClusterId, recovered.ClusterId)
	require.Equal(t, metadata.Version, recovered.Version)
}
