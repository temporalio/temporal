package workflowresend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

const (
	syncTestNamespaceID              = namespace.ID("sync-namespace-id")
	syncTestWorkflowID               = "sync-workflow-id"
	syncTestRunID                    = "sync-run-id"
	syncTestCurrentCluster           = "sync-cluster-b"
	syncTestSourceCluster            = "sync-cluster-a"
	syncTestAlternativeSourceCluster = "sync-cluster-c"
	syncTestCurrentFailoverVersion   = int64(22)
)

type syncWorkflowStateFixture struct {
	shard          *historyi.MockShardContext
	registry       *namespace.MockRegistry
	cluster        *cluster.MockMetadata
	remoteClient   *adminservicemock.MockAdminServiceClient
	engine         *historyi.MockEngine
	execution      *commonpb.WorkflowExecution
	transition     *persistencespb.VersionedTransition
	versionHistory *historyspb.VersionHistories
}

func TestSyncWorkflowStateResultZeroValueSkips(t *testing.T) {
	var result SyncWorkflowStateResult
	require.Equal(t, SyncWorkflowStateResultSkipped, result)
}

func TestSyncWorkflowStateFromSource_AppliesArtifact(t *testing.T) {
	tests := []struct {
		name     string
		applyErr error
	}{
		{name: "applied"},
		{name: "duplicate is applied", applyErr: consts.ErrDuplicate},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newSyncWorkflowStateFixture(t)
			artifact := &replicationspb.VersionedTransitionArtifact{}
			f.expectEligibleNamespace()
			f.remoteClient.EXPECT().SyncWorkflowState(
				gomock.Any(),
				protomock.Eq(f.expectedRequest()),
			).Return(&adminservice.SyncWorkflowStateResponse{
				VersionedTransitionArtifact: artifact,
			}, nil)
			f.registry.EXPECT().GetNamespaceByID(syncTestNamespaceID).Return(
				syncTestNamespace(syncTestSourceCluster, syncTestSourceCluster, syncTestCurrentCluster),
				nil,
			)
			f.shard.EXPECT().GetEngine(gomock.Any()).Return(f.engine, nil)
			f.engine.EXPECT().ReplicateVersionedTransition(
				gomock.Any(),
				chasm.WorkflowArchetypeID,
				artifact,
				syncTestSourceCluster,
			).Return(test.applyErr)

			result, err := f.sync(t.Context())

			require.NoError(t, err)
			require.Equal(t, SyncWorkflowStateResultApplied, result)
		})
	}
}

func TestSyncWorkflowStateFromSource_SourceResponse(t *testing.T) {
	tests := []struct {
		name   string
		rpcErr error
		result SyncWorkflowStateResult
	}{
		{
			name:   "source workflow not found",
			rpcErr: serviceerror.NewNotFound("workflow not found"),
			result: SyncWorkflowStateResultSourceNotFound,
		},
		{
			name:   "transition history unsupported",
			rpcErr: serviceerror.NewFailedPrecondition("transition history disabled"),
			result: SyncWorkflowStateResultSkipped,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newSyncWorkflowStateFixture(t)
			f.expectEligibleNamespace()
			f.remoteClient.EXPECT().SyncWorkflowState(
				gomock.Any(),
				protomock.Eq(f.expectedRequest()),
			).Return(nil, test.rpcErr)

			result, err := f.sync(t.Context())

			require.NoError(t, err)
			require.Equal(t, test.result, result)
		})
	}
}

func TestSyncWorkflowStateFromSource_SkipsWhenNamespaceBecomesIneligible(t *testing.T) {
	t.Run("removed from current cluster before request", func(t *testing.T) {
		f := newSyncWorkflowStateFixture(t)
		f.expectNamespaceLookup(
			syncTestNamespace(syncTestSourceCluster, syncTestSourceCluster),
			nil,
		)

		result, err := f.sync(t.Context())

		require.NoError(t, err)
		require.Equal(t, SyncWorkflowStateResultSkipped, result)
	})

	t.Run("active on current cluster before request", func(t *testing.T) {
		f := newSyncWorkflowStateFixture(t)
		f.expectNamespaceLookup(
			syncTestNamespace(syncTestCurrentCluster, syncTestSourceCluster, syncTestCurrentCluster),
			nil,
		)

		result, err := f.sync(t.Context())

		require.NoError(t, err)
		require.Equal(t, SyncWorkflowStateResultSkipped, result)
	})

	t.Run("namespace deleted before request", func(t *testing.T) {
		f := newSyncWorkflowStateFixture(t)
		f.expectNamespaceLookup(nil, serviceerror.NewNamespaceNotFound(syncTestNamespaceID.String()))

		result, err := f.sync(t.Context())

		require.ErrorAs(t, err, new(*serviceerror.NamespaceNotFound))
		require.Equal(t, SyncWorkflowStateResultSkipped, result)
	})

	postRequestStates := []struct {
		name      string
		entry     *namespace.Namespace
		lookupErr error
	}{
		{
			name:  "namespace removed from current cluster during request",
			entry: syncTestNamespace(syncTestSourceCluster, syncTestSourceCluster),
		},
		{
			name:  "namespace becomes active locally during request",
			entry: syncTestNamespace(syncTestCurrentCluster, syncTestSourceCluster, syncTestCurrentCluster),
		},
		{
			name:  "source changes during request",
			entry: syncTestNamespace(syncTestAlternativeSourceCluster, syncTestAlternativeSourceCluster, syncTestCurrentCluster),
		},
		{
			name:      "namespace deleted during request",
			lookupErr: serviceerror.NewNamespaceNotFound(syncTestNamespaceID.String()),
		},
	}
	for _, postRequestState := range postRequestStates {
		t.Run(postRequestState.name, func(t *testing.T) {
			f := newSyncWorkflowStateFixture(t)
			f.expectEligibleNamespace()
			f.remoteClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
				&adminservice.SyncWorkflowStateResponse{
					VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{},
				},
				nil,
			)
			f.registry.EXPECT().GetNamespaceByID(syncTestNamespaceID).Return(
				postRequestState.entry,
				postRequestState.lookupErr,
			)

			result, err := f.sync(t.Context())

			require.NoError(t, err)
			require.Equal(t, SyncWorkflowStateResultSkipped, result)
		})
	}
}

func TestSyncWorkflowStateFromSource_PropagatesErrors(t *testing.T) {
	t.Run("initial namespace lookup", func(t *testing.T) {
		f := newSyncWorkflowStateFixture(t)
		testErr := errors.New("namespace lookup failed")
		f.expectNamespaceLookup(nil, testErr)

		result, err := f.sync(t.Context())

		require.ErrorIs(t, err, testErr)
		require.Equal(t, SyncWorkflowStateResultSkipped, result)
	})

	t.Run("sync request", func(t *testing.T) {
		f := newSyncWorkflowStateFixture(t)
		testErr := errors.New("sync request failed")
		f.expectEligibleNamespace()
		f.remoteClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(nil, testErr)

		result, err := f.sync(t.Context())

		require.ErrorIs(t, err, testErr)
		require.Equal(t, SyncWorkflowStateResultSkipped, result)
	})

	t.Run("refreshed namespace lookup", func(t *testing.T) {
		f := newSyncWorkflowStateFixture(t)
		testErr := errors.New("namespace refresh failed")
		f.expectEligibleNamespace()
		f.remoteClient.EXPECT().SyncWorkflowState(gomock.Any(), gomock.Any()).Return(
			&adminservice.SyncWorkflowStateResponse{
				VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{},
			},
			nil,
		)
		f.registry.EXPECT().GetNamespaceByID(syncTestNamespaceID).Return(nil, testErr)

		result, err := f.sync(t.Context())

		require.ErrorIs(t, err, testErr)
		require.Equal(t, SyncWorkflowStateResultSkipped, result)
	})
}

func newSyncWorkflowStateFixture(t *testing.T) *syncWorkflowStateFixture {
	controller := gomock.NewController(t)
	return &syncWorkflowStateFixture{
		shard:        historyi.NewMockShardContext(controller),
		registry:     namespace.NewMockRegistry(controller),
		cluster:      cluster.NewMockMetadata(controller),
		remoteClient: adminservicemock.NewMockAdminServiceClient(controller),
		engine:       historyi.NewMockEngine(controller),
		execution: &commonpb.WorkflowExecution{
			WorkflowId: syncTestWorkflowID,
			RunId:      syncTestRunID,
		},
		transition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 11,
			TransitionCount:          12,
		},
		versionHistory: &historyspb.VersionHistories{},
	}
}

func (f *syncWorkflowStateFixture) expectNamespaceLookup(entry *namespace.Namespace, err error) {
	f.shard.EXPECT().GetClusterMetadata().Return(f.cluster)
	f.cluster.EXPECT().GetCurrentClusterName().Return(syncTestCurrentCluster)
	f.shard.EXPECT().GetNamespaceRegistry().Return(f.registry)
	f.registry.EXPECT().GetNamespaceByID(syncTestNamespaceID).Return(entry, err)
}

func (f *syncWorkflowStateFixture) expectEligibleNamespace() {
	f.expectNamespaceLookup(
		syncTestNamespace(syncTestSourceCluster, syncTestSourceCluster, syncTestCurrentCluster),
		nil,
	)
	f.cluster.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		syncTestCurrentCluster: {
			InitialFailoverVersion: syncTestCurrentFailoverVersion,
		},
	})
	f.shard.EXPECT().GetRemoteAdminClient(syncTestSourceCluster).Return(f.remoteClient, nil)
}

func (f *syncWorkflowStateFixture) expectedRequest() *adminservice.SyncWorkflowStateRequest {
	return &adminservice.SyncWorkflowStateRequest{
		NamespaceId:         syncTestNamespaceID.String(),
		Execution:           f.execution,
		ArchetypeId:         chasm.WorkflowArchetypeID,
		VersionedTransition: f.transition,
		VersionHistories:    f.versionHistory,
		TargetClusterId:     int32(syncTestCurrentFailoverVersion),
	}
}

func (f *syncWorkflowStateFixture) sync(ctx context.Context) (SyncWorkflowStateResult, error) {
	return SyncWorkflowStateFromSource(
		ctx,
		f.shard,
		syncTestNamespaceID,
		f.execution,
		f.transition,
		f.versionHistory,
		nil,
	)
}

func syncTestNamespace(activeCluster string, clusters ...string) *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: syncTestNamespaceID.String(), Name: "sync-test-namespace"},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters:          clusters,
		},
		1,
	)
}
