package frontend

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

type recordingDataMerger struct {
	calls atomic.Int32
}

func (m *recordingDataMerger) MergeData(_ map[string]string, taskData map[string]string, _ int64, _ int64) (map[string]string, bool) {
	m.calls.Add(1)
	return taskData, false
}

type recordingMutationDataMerger struct {
	mu         sync.Mutex
	namespaces []namespace.Name
	ids        []namespace.ID
}

func (m *recordingMutationDataMerger) MergeMutationData(
	namespaceID namespace.ID,
	namespaceName namespace.Name,
	_ map[string]string,
	taskData map[string]string,
	_ int64,
	_ int64,
) (map[string]string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ids = append(m.ids, namespaceID)
	m.namespaces = append(m.namespaces, namespaceName)
	return taskData, false
}

func TestDefaultMutationDataMergerMergesAsSharedMerger(t *testing.T) {
	t.Parallel()
	shared := &recordingDataMerger{}
	merged, ok := nsreplication.NewDefaultMutationDataMerger(shared).MergeMutationData(
		"namespace-id", "payments", map[string]string{"k": "local"}, map[string]string{"k": "task"}, 1, 2)
	require.Equal(t, int32(1), shared.calls.Load(), "by default CHASM deliveries use the shared merger")
	require.Equal(t, map[string]string{"k": "task"}, merged)
	require.False(t, ok)
}

func TestNamespaceMutationExecutorUsesMutationDataMerger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	clusters := cluster.NewMockMetadata(ctrl)
	clusters.EXPECT().GetCurrentClusterName().Return("cellC").AnyTimes()
	metadata := persistence.NewMockMetadataManager(ctrl)
	metadata.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 2}, nil).AnyTimes()
	metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments"},
			Config:            &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cellA", Clusters: []string{"cellA", "cellC"}},
			ConfigVersion:     9,
			FailoverVersion:   1,
		},
		IsGlobalNamespace: true,
	}, nil).AnyTimes()
	metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mutationMerger := &recordingMutationDataMerger{}
	executor := NamespaceMutationTaskExecutorProvider(
		NewConfig(dynamicconfig.NewNoopCollection(), 1),
		clusters,
		metadata,
		mutationMerger,
		nsreplication.NewDefaultAdmitter(),
		log.NewNoopLogger(),
		wideevents.NoopLogger(),
		metrics.NoopMetricsHandler,
		testhooks.TestHooks{},
	)
	_, err := executor.ExecuteWithOutcome(t.Context(), removalTestTask(false), nsreplication.MutationApplyOptions{})
	require.NoError(t, err)
	mutationMerger.mu.Lock()
	defer mutationMerger.mu.Unlock()
	require.NotEmpty(t, mutationMerger.ids, "the CHASM executor must merge with the mutation data merger")
	for i := range mutationMerger.ids {
		require.Equal(t, namespace.ID("namespace-id"), mutationMerger.ids[i], "the merger is told which namespace it merges")
		require.Equal(t, namespace.Name("payments"), mutationMerger.namespaces[i])
	}
}
