package frontend

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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

func TestNamespaceMutationExecutorUsesMutationDataMerger(t *testing.T) {
	t.Parallel()
	shared := nsreplication.NewNoopDataMerger()
	require.Same(t, shared, nsreplication.NewDefaultMutationDataMerger(shared), "by default CHASM deliveries use the shared merger")

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

	mutationMerger := &recordingDataMerger{}
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
	require.Positive(t, mutationMerger.calls.Load(), "the CHASM executor must merge with the mutation data merger")
}
