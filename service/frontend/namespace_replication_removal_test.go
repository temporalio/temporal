package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type rejectRemovalAdmitter struct{}

func (rejectRemovalAdmitter) Admit(context.Context, string, *replicationspb.NamespaceTaskAttributes) bool {
	return false
}

// delegatingRemovalAdmitter models a custom policy that adds its own routing
// logic, then falls back to the default admission rules with the original
// context. Removal intent must survive that delegation.
type delegatingRemovalAdmitter struct {
	delegate nsreplication.NamespaceReplicationAdmitter
}

func (a delegatingRemovalAdmitter) Admit(ctx context.Context, currentCluster string, task *replicationspb.NamespaceTaskAttributes) bool {
	return a.delegate.Admit(ctx, currentCluster, task)
}

func removalTestTask(removed bool) *replicationspb.NamespaceTaskAttributes {
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 "namespace-id",
		Info:               &namespacepb.NamespaceInfo{Name: "payments", State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config:             &namespacepb.NamespaceConfig{},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: "cellA",
			Clusters:          []*replicationpb.ClusterReplicationConfig{{ClusterName: "cellA"}},
		},
		ConfigVersion: 11, FailoverVersion: 1,
	}
	if !removed {
		task.ConfigVersion = 10
		task.ReplicationConfig.Clusters = append(task.ReplicationConfig.Clusters, &replicationpb.ClusterReplicationConfig{ClusterName: "cellC"})
	}
	return task
}

func TestAuthoritativeRemovalConvergesUnderReorderingAndContention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		removalFirst    bool
		createRace      bool
		updateConflict  bool
		commitTimeout   bool
		customReject    bool
		delegateDefault bool
	}{
		{name: "removal before addition", removalFirst: true},
		{name: "addition before removal"},
		{name: "older addition wins create race", removalFirst: true, createRace: true},
		{name: "notification counter contention", updateConflict: true},
		{name: "write commits before timeout", commitTimeout: true},
		{name: "custom policy still rejects", removalFirst: true, customReject: true},
		{name: "custom policy delegates default removal admission", removalFirst: true, delegateDefault: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := persistence.NewMockMetadataManager(ctrl)
			clusters := cluster.NewMockMetadata(ctrl)
			clusters.EXPECT().GetCurrentClusterName().Return("cellC").AnyTimes()
			clusters.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
			var stored *persistencespb.NamespaceDetail
			version := int64(1)
			metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
				if stored == nil {
					return nil, serviceerror.NewNamespaceNotFound("payments")
				}
				return &persistence.GetNamespaceResponse{Namespace: proto.Clone(stored).(*persistencespb.NamespaceDetail), IsGlobalNamespace: true}, nil
			}).AnyTimes()
			metadata.EXPECT().GetMetadata(gomock.Any()).DoAndReturn(func(context.Context) (*persistence.GetMetadataResponse, error) {
				return &persistence.GetMetadataResponse{NotificationVersion: version}, nil
			}).AnyTimes()
			metadata.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
				if tc.createRace && stored == nil {
					// An older concurrent CREATE wins after the UPDATE read no row.
					stored = proto.Clone(req.Namespace).(*persistencespb.NamespaceDetail)
					stored.ConfigVersion = 10
					stored.ReplicationConfig.Clusters = []string{"cellA", "cellC"}
					version++
				}
				if stored != nil {
					return nil, serviceerror.NewNamespaceAlreadyExists("payments")
				}
				stored = proto.Clone(req.Namespace).(*persistencespb.NamespaceDetail)
				version++
				return &persistence.CreateNamespaceResponse{ID: req.Namespace.GetInfo().GetId()}, nil
			}).AnyTimes()
			var attemptedVersions []int64
			metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *persistence.UpdateNamespaceRequest) error {
				attemptedVersions = append(attemptedVersions, req.NotificationVersion)
				if tc.updateConflict && len(attemptedVersions) == 1 {
					version++ // An unrelated namespace write consumes the shared counter.
					return serviceerror.NewUnavailable("CAS conflict")
				}
				require.Equal(t, version, req.NotificationVersion, "each retry must read a fresh counter")
				stored = proto.Clone(req.Namespace).(*persistencespb.NamespaceDetail)
				version++
				if tc.commitTimeout && len(attemptedVersions) == 1 {
					return serviceerror.NewUnavailable("write committed but response timed out")
				}
				return nil
			}).AnyTimes()
			admitter := nsreplication.NewDefaultAdmitter()
			if tc.customReject {
				admitter = rejectRemovalAdmitter{}
			} else if tc.delegateDefault {
				admitter = delegatingRemovalAdmitter{delegate: admitter}
			}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: clusters,
				namespaceMutationExecutor: nsreplication.NewMutationTaskExecutor("cellC", metadata, nsreplication.NewNoopDataMerger(), admitter, log.NewNoopLogger(), testhooks.TestHooks{}),
			}
			for _, removed := range []bool{tc.removalFirst, !tc.removalFirst} {
				payload, err := nsreplication.MarshalNamespaceTask(removalTestTask(removed))
				require.NoError(t, err)
				response, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload, RemovedPeer: removed})
				require.NoError(t, err)
				if tc.customReject {
					require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED, response.GetOutcome())
				}
				if tc.commitTimeout && removed {
					require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE, response.GetOutcome())
				}
			}
			if tc.customReject {
				require.Nil(t, stored)
			} else {
				require.NotNil(t, stored)
				require.Equal(t, int64(11), stored.GetConfigVersion())
				require.Equal(t, []string{"cellA"}, stored.GetReplicationConfig().GetClusters())
			}
			if tc.updateConflict {
				require.Equal(t, []int64{2, 3}, attemptedVersions)
			}
			if tc.commitTimeout {
				require.Equal(t, []int64{2}, attemptedVersions, "retry must recognize the committed snapshot without another write")
				require.Equal(t, int64(3), version)
			}
		})
	}
}

func TestRemovedPeerIntentValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*replicationspb.NamespaceTaskAttributes)
		shadow bool
	}{
		{name: "create", mutate: func(task *replicationspb.NamespaceTaskAttributes) {
			task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_CREATE
		}},
		{name: "receiver still included", mutate: func(task *replicationspb.NamespaceTaskAttributes) {
			task.ReplicationConfig.Clusters = append(task.ReplicationConfig.Clusters, &replicationpb.ClusterReplicationConfig{ClusterName: "cellC"})
		}},
		{name: "empty cluster list", mutate: func(task *replicationspb.NamespaceTaskAttributes) { task.ReplicationConfig.Clusters = nil }},
		{name: "shadow", shadow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			clusters := cluster.NewMockMetadata(ctrl)
			clusters.EXPECT().GetCurrentClusterName().Return("cellC").AnyTimes()
			clusters.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
			executor := nsreplication.NewMockMutationTaskExecutor(ctrl)
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: clusters, namespaceMutationExecutor: executor}
			task := removalTestTask(true)
			if tc.mutate != nil {
				tc.mutate(task)
			}
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			_, err = handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload, ShadowLegacyTaskPayload: payload, RemovedPeer: true, Shadow: tc.shadow})
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalid)
		})
	}
}
