package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
)

func TestOnNamespaceStateChange(t *testing.T) {
	for _, tc := range []struct {
		name          string
		from          string
		to            string
		deletedFromDB bool
		stopped       bool
		unload        bool
	}{
		{name: "active to passive", from: cluster.TestCurrentClusterName, to: cluster.TestAlternativeClusterName, unload: true},
		{name: "passive to active", from: cluster.TestAlternativeClusterName, to: cluster.TestCurrentClusterName, unload: true},
		{name: "unchanged active", from: cluster.TestCurrentClusterName, to: cluster.TestCurrentClusterName},
		{name: "passive to other passive", from: cluster.TestAlternativeClusterName, to: "third-cluster"},
		{name: "deleted namespace", from: cluster.TestCurrentClusterName, to: cluster.TestAlternativeClusterName, deletedFromDB: true},
		{name: "stopped engine", from: cluster.TestCurrentClusterName, to: cluster.TestAlternativeClusterName, stopped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, current, _ := newFailoverTestEngine(t, tc.from)
			partition := newRootPartition(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			_, _, err := e.getTaskQueuePartitionManager(context.Background(), partition, true, loadCausePoll)
			require.NoError(t, err)
			if tc.stopped {
				e.status = common.DaemonStatusStopped
			}

			newNS := failoverTestNamespace(tc.to)
			current.Store(newNS)
			e.onNamespaceStateChange(newNS, tc.deletedFromDB)

			if tc.unload {
				await.RequireTrue(t, func() bool {
					return len(e.getTaskQueuePartitions(10)) == 0
				}, 5*time.Second, 10*time.Millisecond)
			} else {
				require.Len(t, e.getTaskQueuePartitions(10), 1)
			}
		})
	}
}

func TestNamespaceFailoverDuringPartitionCreation(t *testing.T) {
	e, _, nextLookup := newFailoverTestEngine(t, cluster.TestCurrentClusterName)
	// The partition is built from a passive snapshot, but the failover to active lands before the
	// post-insert check, as if the callback's scan had missed this partition.
	nextLookup.Store(failoverTestNamespace(cluster.TestAlternativeClusterName))

	partition := newRootPartition(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	_, created, _ := e.getTaskQueuePartitionManager(context.Background(), partition, true, loadCausePoll)
	require.True(t, created)
	await.RequireTrue(t, func() bool {
		return len(e.getTaskQueuePartitions(10)) == 0
	}, 5*time.Second, 10*time.Millisecond)
}

func failoverTestNamespace(activeCluster string) *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: namespaceName},
		nil,
		&persistencespb.NamespaceReplicationConfig{ActiveClusterName: activeCluster},
		1,
	)
}

// newFailoverTestEngine returns a started engine whose registry serves the namespace in current,
// or the one in nextLookup for the next lookup only.
func newFailoverTestEngine(
	t *testing.T,
	activeCluster string,
) (e *matchingEngineImpl, current, nextLookup *atomic.Pointer[namespace.Namespace]) {
	ctrl := gomock.NewController(t)
	current, nextLookup = &atomic.Pointer[namespace.Namespace]{}, &atomic.Pointer[namespace.Namespace]{}
	current.Store(failoverTestNamespace(activeCluster))
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).DoAndReturn(func(namespace.ID) (*namespace.Namespace, error) {
		if ns := nextLookup.Swap(nil); ns != nil {
			return ns, nil
		}
		return current.Load(), nil
	}).AnyTimes()
	registry.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name(namespaceName), nil).AnyTimes()
	client := matchingservicemock.NewMockMatchingServiceClient(ctrl)
	client.EXPECT().ForceLoadTaskQueuePartition(gomock.Any(), gomock.Any()).Return(&matchingservice.ForceLoadTaskQueuePartitionResponse{}, nil).AnyTimes()
	e = createTestMatchingEngine(log.NewTestLogger(), ctrl, defaultTestConfig(), client, registry)
	e.status = common.DaemonStatusStarted
	t.Cleanup(func() {
		for _, pm := range e.getTaskQueuePartitions(10) {
			e.unloadTaskQueuePartition(pm, unloadCauseShuttingDown)
		}
	})
	return e, current, nextLookup
}
