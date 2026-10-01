package matching

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

func TestOnNamespaceStateChange(t *testing.T) {
	active := failoverTestNamespace(cluster.TestCurrentClusterName)
	passive := failoverTestNamespace(cluster.TestAlternativeClusterName)
	local := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: namespaceName}, nil, cluster.TestCurrentClusterName)

	for _, tc := range []struct {
		name          string
		loaded        *namespace.Namespace
		current       *namespace.Namespace
		count         int
		deletedFromDB bool
		engineStopped bool
		unload        bool
	}{
		// More than one batch of partitions.
		{name: "active to passive", loaded: active, current: passive, count: 250, unload: true},
		{name: "passive to active", loaded: passive, current: active, count: 1, unload: true},
		{name: "unchanged active", loaded: active, current: active, count: 1},
		{name: "passive to other passive", loaded: passive, current: failoverTestNamespace("third-cluster"), count: 1},
		{name: "deleted from db", loaded: active, current: active, count: 1, deletedFromDB: true, unload: true},
		{name: "local namespace updated", loaded: local, current: local, count: 1},
		{name: "local namespace deleted from db", loaded: local, current: local, count: 1, deletedFromDB: true, unload: true},
		{name: "stopped engine", loaded: active, current: passive, count: 1, engineStopped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				e := newFailoverTestEngine(t, ctrl, namespace.NewMockRegistry(ctrl))
				if tc.engineStopped {
					e.Stop()
				}
				addMockPartitions(ctrl, e, tc.loaded, tc.count, tc.unload)
				// Another namespace's partition must never be unloaded.
				other := namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: "other-ns-id", Name: "other-ns"}, nil, cluster.TestCurrentClusterName)
				addMockPartitions(ctrl, e, other, 1, false)

				e.onNamespaceStateChange(tc.current, tc.deletedFromDB)
				synctest.Wait()

				if tc.unload {
					require.Len(t, e.getTaskQueuePartitions(1000), 1)
				} else {
					require.Len(t, e.getTaskQueuePartitions(1000), tc.count+1)
				}
			})
		})
	}
}

// Runs the callback and engine Stop concurrently, so that the race detector checks they only touch the
// partition map under partitionsLock, and each partition is stopped by exactly one of them.
func TestOnNamespaceStateChange_ConcurrentEngineStop(t *testing.T) {
	for range 20 {
		ctrl := gomock.NewController(t)
		e := newFailoverTestEngine(t, ctrl, namespace.NewMockRegistry(ctrl))
		for i := range 100 {
			pm := NewMocktaskQueuePartitionManager(ctrl)
			pm.EXPECT().Namespace().Return(failoverTestNamespace(cluster.TestCurrentClusterName)).AnyTimes()
			pm.EXPECT().Stop(gomock.Any())
			e.updateTaskQueue(newRootPartition(namespaceID, fmt.Sprintf("tq-%d", i), enumspb.TASK_QUEUE_TYPE_WORKFLOW), pm)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			e.onNamespaceStateChange(failoverTestNamespace(cluster.TestAlternativeClusterName), false)
		})
		wg.Go(func() {
			<-start
			e.Stop()
		})
		close(start)
		wg.Wait()
	}
}

func TestOnNamespaceStateChange_EngineStopWaitsForUnloads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctrl := gomock.NewController(t)
		e := newFailoverTestEngine(t, ctrl, namespace.NewMockRegistry(ctrl))

		release := make(chan struct{})
		pm := NewMocktaskQueuePartitionManager(ctrl)
		pm.EXPECT().Namespace().Return(failoverTestNamespace(cluster.TestCurrentClusterName)).AnyTimes()
		pm.EXPECT().Stop(unloadCauseNamespaceStateChange).Do(func(unloadCause) { <-release })
		e.updateTaskQueue(newRootPartition(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW), pm)
		e.onNamespaceStateChange(failoverTestNamespace(cluster.TestAlternativeClusterName), false)

		var engineStopped atomic.Bool
		go func() {
			e.Stop()
			engineStopped.Store(true)
		}()
		synctest.Wait()
		require.False(t, engineStopped.Load())

		close(release)
		synctest.Wait()
		require.True(t, engineStopped.Load())
	})
}

func TestGetTaskQueuePartitionManager_NamespaceStateChange(t *testing.T) {
	active := failoverTestNamespace(cluster.TestCurrentClusterName)
	passive := failoverTestNamespace(cluster.TestAlternativeClusterName)

	for _, tc := range []struct {
		name      string
		current   *namespace.Namespace
		wantErr   bool
		wantState *namespace.Namespace
	}{
		// The partition is built from an active snapshot, and the namespace fails over to passive before
		// it's inserted, as if the callback's scan had already run.
		{name: "failed over", current: passive, wantState: passive},
		{name: "unchanged", current: active, wantState: active},
		{name: "removed from registry", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			registry := namespace.NewMockRegistry(ctrl)
			var lookups atomic.Int32
			registry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).DoAndReturn(func(namespace.ID) (*namespace.Namespace, error) {
				if lookups.Add(1) == 1 || tc.current == nil {
					return active, nil
				}
				return tc.current, nil
			}).AnyTimes()
			registry.EXPECT().GetNamespaceByIDWithOptions(
				namespace.ID(namespaceID),
				namespace.GetNamespaceOptions{DisableReadthrough: true},
			).DoAndReturn(func(namespace.ID, namespace.GetNamespaceOptions) (*namespace.Namespace, error) {
				if tc.current == nil {
					return nil, serviceerror.NewNamespaceNotFound(namespaceID)
				}
				return tc.current, nil
			}).AnyTimes()
			registry.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name(namespaceName), nil).AnyTimes()
			e := newFailoverTestEngine(t, ctrl, registry)

			partition := newRootPartition(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			pm, created, err := e.getTaskQueuePartitionManager(t.Context(), partition, true, loadCausePoll)
			if tc.wantErr {
				var notFound *serviceerror.NamespaceNotFound
				require.ErrorAs(t, err, &notFound)
				require.Empty(t, e.getTaskQueuePartitions(10))
				return
			}
			require.NoError(t, err)
			require.True(t, created)
			require.Same(t, tc.wantState, pm.Namespace())
			require.Len(t, e.getTaskQueuePartitions(10), 1)
		})
	}
}

func failoverTestNamespace(activeCluster string) *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: namespaceName},
		nil,
		&persistencespb.NamespaceReplicationConfig{ActiveClusterName: activeCluster},
		1,
	)
}

// newFailoverTestEngine returns a started engine that's stopped when the test finishes.
func newFailoverTestEngine(t *testing.T, ctrl *gomock.Controller, registry *namespace.MockRegistry) *matchingEngineImpl {
	registry.EXPECT().RegisterStateChangeCallback(gomock.Any(), gomock.Any()).AnyTimes()
	registry.EXPECT().UnregisterStateChangeCallback(gomock.Any()).AnyTimes()
	client := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	client.EXPECT().ForceLoadTaskQueuePartition(gomock.Any(), gomock.Any()).Return(&matchingservice.ForceLoadTaskQueuePartitionResponse{}, nil).AnyTimes()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	e := createTestMatchingEngine(logger, ctrl, defaultTestConfig(), client, registry)
	e.Start()
	t.Cleanup(e.Stop)
	return e
}

// addMockPartitions loads count mock partitions of ns into the engine. If expectUnload is set, each one
// expects exactly one Stop, from the namespace state change; otherwise it may only be stopped when the
// engine shuts down.
func addMockPartitions(ctrl *gomock.Controller, e *matchingEngineImpl, ns *namespace.Namespace, count int, expectUnload bool) {
	for i := range count {
		pm := NewMocktaskQueuePartitionManager(ctrl)
		pm.EXPECT().Namespace().Return(ns).AnyTimes()
		if expectUnload {
			pm.EXPECT().Stop(unloadCauseNamespaceStateChange)
		} else {
			pm.EXPECT().Stop(unloadCauseShuttingDown).AnyTimes()
		}
		e.updateTaskQueue(newRootPartition(ns.ID().String(), fmt.Sprintf("tq-%d", i), enumspb.TASK_QUEUE_TYPE_WORKFLOW), pm)
	}
}
