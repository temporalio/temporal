package nsreplication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
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
		require.True(t, admitter.Admit(ctx, "cluster-a", build("cluster-a", "cluster-b")))
	})

	t.Run("cluster not in list is rejected", func(t *testing.T) {
		require.False(t, admitter.Admit(ctx, "cluster-c", build("cluster-a", "cluster-b")))
	})

	t.Run("empty cluster list is rejected", func(t *testing.T) {
		require.False(t, admitter.Admit(ctx, "cluster-a", build()))
	})

	t.Run("nil replication config is rejected", func(t *testing.T) {
		// GetReplicationConfig is nil-safe via proto getters; admitter must not panic.
		require.False(t, admitter.Admit(ctx, "cluster-a", &replicationspb.NamespaceTaskAttributes{}))
	})

	t.Run("empty current-cluster name never matches", func(t *testing.T) {
		// Defensive: a caller passing "" should never be admitted even if "" somehow slipped into the list.
		require.False(t, admitter.Admit(ctx, "", build("cluster-a", "cluster-b")))
	})
}

// stubAdmitter is a deterministic admitter used by shouldProcessTask tests.
type stubAdmitter struct {
	admit  bool
	called int
}

func (s *stubAdmitter) Admit(
	context.Context,
	string,
	*replicationspb.NamespaceTaskAttributes,
) bool {
	s.called++
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
		wantProcess  bool
	}{
		{"admitter accepts", true, true},
		{"admitter rejects", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			admitter := &stubAdmitter{admit: tc.admitterSays}
			exec, mockMgr := newExecutorForAdmitterTest(t, admitter)
			expectNotFound(mockMgr)

			shouldProcess, err := exec.shouldProcessTask(context.Background(), newTaskForAdmitterTest())

			require.NoError(t, err)
			require.Equal(t, tc.wantProcess, shouldProcess)
			require.Equal(t, 1, admitter.called)
		})
	}
}

func TestShouldProcessTask_SkipsAdmitterWhenNamespaceExists(t *testing.T) {
	admitter := &stubAdmitter{admit: false}
	exec, mockMgr := newExecutorForAdmitterTest(t, admitter)

	// GetNamespace returns a matching local record, so shouldProcessTask takes the
	// nil-error branch and must NOT consult the admitter.
	mockMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(
		&persistence.GetNamespaceResponse{Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: "ns-id"},
		}}, nil,
	).Times(1)

	shouldProcess, err := exec.shouldProcessTask(context.Background(), newTaskForAdmitterTest())

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

	shouldProcess, err := exec.shouldProcessTask(context.Background(), newTaskForAdmitterTest())

	require.ErrorIs(t, err, boom)
	require.False(t, shouldProcess)
	require.Equal(t, 0, admitter.called, "admitter must not be called when GetNamespace fails unexpectedly")
}
