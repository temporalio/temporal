package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsregistry"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
)

type renamingNamespacePersistence struct {
	persistence.MetadataManager
	renameOnNextPage atomic.Bool
}

func (p *renamingNamespacePersistence) ListNamespaces(
	ctx context.Context,
	request *persistence.ListNamespacesRequest,
) (*persistence.ListNamespacesResponse, error) {
	pageRequest := *request
	pageRequest.PageSize = 1
	response, err := p.MetadataManager.ListNamespaces(ctx, &pageRequest)
	if err != nil {
		return nil, err
	}
	if len(request.NextPageToken) == 0 && p.renameOnNextPage.Swap(false) {
		err = p.RenameNamespace(ctx, &persistence.RenameNamespaceRequest{
			PreviousName: "z-namespace",
			NewName:      "a-namespace",
		})
	}
	return response, err
}

func TestCassandraNamespaceRegistryRenameDuringPagination(t *testing.T) {
	t.Parallel()
	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)
	store, err := cassandra.NewMetadataStore("active", cluster.GetSession(), log.NewNoopLogger())
	require.NoError(t, err)
	manager := persistence.NewMetadataManagerImpl(store, serialization.NewSerializer(), log.NewNoopLogger(), "active")
	ctx := context.Background()
	var renamedID namespace.ID
	for _, name := range []string{"m-namespace", "z-namespace"} {
		id := namespace.NewID()
		_, err := manager.CreateNamespace(ctx, &persistence.CreateNamespaceRequest{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{
					Id: id.String(), Name: name, State: enumspb.NAMESPACE_STATE_REGISTERED,
				},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
		})
		require.NoError(t, err)
		renamedID = id
	}
	regPersistence := &renamingNamespacePersistence{MetadataManager: manager}
	registry := nsregistry.NewRegistry(
		regPersistence, true, "active", dynamicconfig.GetDurationPropertyFn(10*time.Millisecond),
		dynamicconfig.GetBoolPropertyFn(false), metrics.NoopMetricsHandler, log.NewNoopLogger(),
		namespace.NewDefaultReplicationResolverFactory(), nsregistry.DefaultNamespaceStateChanged,
	)
	registry.Start()
	t.Cleanup(registry.Stop)
	type change struct {
		ns      *namespace.Namespace
		deleted bool
	}
	changes := make(chan change, 1)
	registry.RegisterStateChangeCallback("rename", func(ns *namespace.Namespace, deleted bool) {
		if ns.ID() == renamedID && (deleted || ns.Name() == "a-namespace") {
			select {
			case changes <- change{ns: ns, deleted: deleted}:
			default:
			}
		}
	})
	regPersistence.renameOnNextPage.Store(true)
	select {
	case changed := <-changes:
		persisted, err := manager.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: renamedID.String()})
		require.NoError(t, err)
		require.Equal(t, "a-namespace", persisted.Namespace.Info.Name)
		require.False(t, changed.deleted, "a rename across the page cursor must not be reported as physical deletion")
		require.Equal(t, namespace.Name("a-namespace"), changed.ns.Name())
	case <-time.After(5 * time.Second):
		t.Fatal("namespace rename was not observed")
	}
	entries := registry.GetAllNamespaces()
	require.Len(t, entries, 2)
	entry, err := registry.GetNamespace("a-namespace")
	require.NoError(t, err)
	require.Equal(t, renamedID, entry.ID())
	_, err = registry.GetNamespace("z-namespace")
	require.ErrorAs(t, err, new(*serviceerror.NamespaceNotFound))

	require.NoError(t, manager.DeleteNamespace(ctx, &persistence.DeleteNamespaceRequest{ID: renamedID.String()}))
	select {
	case changed := <-changes:
		require.True(t, changed.deleted)
		require.Equal(t, renamedID, changed.ns.ID())
	case <-time.After(5 * time.Second):
		t.Fatal("physical namespace deletion was not observed")
	}
	require.Len(t, registry.GetAllNamespaces(), 1)
}
