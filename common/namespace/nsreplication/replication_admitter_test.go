package nsreplication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/mock/gomock"
)

func TestDefaultAdmitter_Admit(t *testing.T) {
	admitter := NewDefaultAdmitter()
	ctx := context.Background()

	build := func(clusters ...string) *replicationspb.NamespaceTaskAttributes {
		repCfg := make([]*replicationpb.ClusterReplicationConfig, 0, len(clusters))
		for _, c := range clusters {
			repCfg = append(repCfg, &replicationpb.ClusterReplicationConfig{ClusterName: c})
		}
		return &replicationspb.NamespaceTaskAttributes{
			ReplicationConfig: &replicationpb.NamespaceReplicationConfig{Clusters: repCfg},
		}
	}

	t.Run("cluster in list is admitted", func(t *testing.T) {
		require.True(t, admitter.Admit(ctx, "cluster-a", build("cluster-a", "cluster-b"), AdmissionOptions{}))
	})

	t.Run("cluster not in list is rejected", func(t *testing.T) {
		require.False(t, admitter.Admit(ctx, "cluster-c", build("cluster-a", "cluster-b"), AdmissionOptions{}))
	})

	t.Run("empty cluster list is rejected", func(t *testing.T) {
		require.False(t, admitter.Admit(ctx, "cluster-a", build(), AdmissionOptions{}))
	})

	t.Run("nil replication config is rejected", func(t *testing.T) {
		// GetReplicationConfig is nil-safe via proto getters; admitter must not panic.
		require.False(t, admitter.Admit(ctx, "cluster-a", &replicationspb.NamespaceTaskAttributes{}, AdmissionOptions{}))
	})

	t.Run("empty current-cluster name never matches", func(t *testing.T) {
		// Defensive: a caller passing "" should never be admitted even if "" somehow slipped into the list.
		require.False(t, admitter.Admit(ctx, "", build("cluster-a", "cluster-b"), AdmissionOptions{}))
	})
}

func TestDefaultAdmitterExplicitRemoval(t *testing.T) {
	t.Parallel()
	task := newTaskForAdmitterTest()
	task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_UPDATE
	task.ReplicationConfig.Clusters = []*replicationpb.ClusterReplicationConfig{{ClusterName: "cluster-other"}}
	admitter := NewDefaultAdmitter()
	require.False(t, admitter.Admit(t.Context(), "cluster-local", task, AdmissionOptions{}), "ordinary delivery retains legacy admission")
	options := AdmissionOptions{RemovedPeer: true}
	require.True(t, admitter.Admit(t.Context(), "cluster-local", task, options))
	require.False(t, admitter.Admit(t.Context(), "", task, options))
	task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_CREATE
	require.False(t, admitter.Admit(t.Context(), "cluster-local", task, options))
}

// stubAdmitter is a deterministic admitter used by shouldProcessTask tests.
type stubAdmitter struct {
	admit   bool
	called  int
	options AdmissionOptions
}

func (s *stubAdmitter) Admit(
	_ context.Context,
	_ string,
	_ *replicationspb.NamespaceTaskAttributes,
	options AdmissionOptions,
) bool {
	s.called++
	s.options = options
	return s.admit
}

func newExecutorForAdmitterTest(t *testing.T, admitter NamespaceReplicationAdmitter) (*taskExecutorImpl, *persistence.MockMetadataManager) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockMgr := persistence.NewMockMetadataManager(ctrl)
	exec := NewTaskExecutor(
		"cluster-local",
		mockMgr,
		NewNoopDataMerger(),
		admitter,
		log.NewTestLogger(),
		testhooks.TestHooks{},
	).(*taskExecutorImpl)
	return exec, mockMgr
}

func newTaskForAdmitterTest() *replicationspb.NamespaceTaskAttributes {
	return &replicationspb.NamespaceTaskAttributes{
		Id: "ns-id",
		Info: &namespacepb.NamespaceInfo{
			Name: "test-ns",
			Data: map[string]string{},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{},
	}
}

func expectNotFound(mockMgr *persistence.MockMetadataManager) {
	mockMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(
		nil, &serviceerror.NamespaceNotFound{},
	).Times(1)
}

func TestShouldProcessTask_ConsultsAdmitterOnNamespaceNotFound(t *testing.T) {
	cases := []struct {
		name         string
		admitterSays bool
		options      AdmissionOptions
		wantProcess  bool
	}{
		{name: "admitter accepts", admitterSays: true, wantProcess: true},
		{name: "admitter rejects", admitterSays: false},
		{name: "custom admitter sees removal intent", admitterSays: true, options: AdmissionOptions{RemovedPeer: true}, wantProcess: true},
		{name: "custom admitter rejects removal", options: AdmissionOptions{RemovedPeer: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admitter := &stubAdmitter{admit: tc.admitterSays}
			exec, mockMgr := newExecutorForAdmitterTest(t, admitter)
			expectNotFound(mockMgr)

			shouldProcess, err := exec.shouldProcessTask(t.Context(), newTaskForAdmitterTest(), tc.options)

			require.NoError(t, err)
			require.Equal(t, tc.wantProcess, shouldProcess)
			require.Equal(t, 1, admitter.called)
			require.Equal(t, tc.options, admitter.options)
		})
	}
}

func TestShouldProcessTask_SkipsAdmitterWhenNamespaceExists(t *testing.T) {
	admitter := &stubAdmitter{admit: false}
	exec, mockMgr := newExecutorForAdmitterTest(t, admitter)

	// GetNamespace returns a matching local record, so shouldProcessTask takes the
	// nil-error branch and must NOT consult the admitter.
	mockMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(
		&persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{Id: "ns-id"},
			},
			IsGlobalNamespace: true,
		}, nil,
	).Times(1)

	shouldProcess, err := exec.shouldProcessTask(context.Background(), newTaskForAdmitterTest(), AdmissionOptions{})

	require.NoError(t, err)
	require.True(t, shouldProcess)
	require.Equal(t, 0, admitter.called, "admitter must not be called when local namespace record exists")
}

func TestShouldProcessTask_PropagatesUnknownGetNamespaceError(t *testing.T) {
	admitter := &stubAdmitter{admit: true}
	exec, mockMgr := newExecutorForAdmitterTest(t, admitter)

	// Non-NamespaceNotFound error must short-circuit without consulting the admitter.
	boom := serviceerror.NewUnavailable("boom")
	mockMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, boom).Times(1)

	shouldProcess, err := exec.shouldProcessTask(context.Background(), newTaskForAdmitterTest(), AdmissionOptions{})

	require.ErrorIs(t, err, boom)
	require.False(t, shouldProcess)
	require.Equal(t, 0, admitter.called, "admitter must not be called when GetNamespace fails unexpectedly")
}
