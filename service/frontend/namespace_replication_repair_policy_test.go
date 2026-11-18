package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// refusingRepairPolicy refuses namespaces whose data carries the given key and
// records what it was asked about.
type refusingRepairPolicy struct {
	key   string
	calls []string
}

func (p *refusingRepairPolicy) policy() nsreplication.RepairPolicy {
	return func(_ context.Context, namespaceID, namespaceName string, data map[string]string) error {
		p.calls = append(p.calls, namespaceID+"/"+namespaceName)
		if _, ok := data[p.key]; ok {
			return serviceerror.NewFailedPrecondition("repair does not support this namespace")
		}
		return nil
	}
}

func repairPolicyTestMutation(t *testing.T, data map[string]string) *adminservice.ApplyNamespaceMutationRequest {
	t.Helper()
	task := shadowObservationTask()
	task.Info.Data = data
	payload, err := nsreplication.MarshalNamespaceTask(task)
	require.NoError(t, err)
	return &adminservice.ApplyNamespaceMutationRequest{
		NamespaceTaskPayload: payload, SourceCluster: "cellA",
		Attribution: &adminservice.NamespaceMutationAttribution{
			Origin: adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR, RepairId: "repair-id", Reason: "operator repair",
		},
	}
}

func TestNamespaceRepairPolicyGuardsOperatorRepairApply(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		data      map[string]string
		repair    bool
		wantApply bool
	}{
		{name: "refused repair writes nothing", data: map[string]string{"membership": "elsewhere"}, repair: true},
		{name: "allowed repair applies", repair: true, wantApply: true},
		// Ordinary CHASM deliveries never consult the repair policy.
		{name: "replication delivery is not checked", data: map[string]string{"membership": "elsewhere"}, wantApply: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			executor := nsreplication.NewMockMutationTaskExecutor(ctrl)
			policy := &refusingRepairPolicy{key: "membership"}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t),
				metricsHandler: metrics.NoopMetricsHandler, namespaceMutationExecutor: executor, repairPolicy: policy.policy()}
			request := repairPolicyTestMutation(t, tc.data)
			if !tc.repair {
				request.Attribution = nil
			}
			if tc.wantApply {
				executor.EXPECT().ExecuteWithOutcome(gomock.Any(), gomock.Any(), gomock.Any()).Return(nsreplication.ApplyOutcomeApplied, nil)
			}
			response, err := handler.ApplyNamespaceMutation(t.Context(), request)
			if !tc.wantApply {
				var refused *serviceerror.FailedPrecondition
				require.ErrorAs(t, err, &refused)
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED, response.GetOutcome())
			}
			if tc.repair {
				require.Equal(t, []string{"namespace-id/payments"}, policy.calls)
			} else {
				require.Empty(t, policy.calls)
			}
		})
	}
}

func TestNamespaceRepairPolicyRefusesRelayBeforeContactingPeer(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	// No cluster lookup or remote client is expected: the refusal comes first.
	policy := &refusingRepairPolicy{key: "membership"}
	handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: cluster.NewMockMetadata(ctrl), repairPolicy: policy.policy()}
	_, err := handler.ApplyRemoteClusterNamespaceRepair(t.Context(), &adminservice.ApplyRemoteClusterNamespaceRepairRequest{
		ClusterName: "peer", Mutation: repairPolicyTestMutation(t, map[string]string{"membership": "elsewhere"}),
	})
	var refused *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &refused)
	require.ErrorContains(t, err, "repair does not support this namespace")
}

func TestNamespaceRepairPolicyLeavesUndecodablePayloadToPeer(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	metadata := cluster.NewMockMetadata(ctrl)
	metadata.EXPECT().GetCurrentClusterName().Return("cellA")
	metadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{})
	policy := &refusingRepairPolicy{key: "membership"}
	handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: metadata, repairPolicy: policy.policy()}
	_, err := handler.ApplyRemoteClusterNamespaceRepair(t.Context(), &adminservice.ApplyRemoteClusterNamespaceRepairRequest{
		ClusterName: "peer", Mutation: remoteRepairTestMutation(),
	})
	// The relay continues to the peer lookup, as before this policy existed.
	require.ErrorContains(t, err, `unknown remote cluster "peer"`)
	require.Empty(t, policy.calls)
}

func TestNamespaceRepairPolicyRefusesReconcile(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	metadata := persistence.NewMockMetadataManager(ctrl)
	clusters := cluster.NewMockMetadata(ctrl)
	clusters.EXPECT().GetCurrentClusterName().Return("cluster-a")
	metadata.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "namespace-id"}).Return(&persistence.GetNamespaceResponse{
		IsGlobalNamespace: true,
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments", State: enumspb.NAMESPACE_STATE_REGISTERED,
				Data: map[string]string{"membership": "elsewhere"}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cluster-a", Clusters: []string{"cluster-a", "cluster-b"}},
		},
	}, nil)
	policy := &refusingRepairPolicy{key: "membership"}
	// No visibility listing or CHASM reconcile is expected after the refusal.
	handler := &AdminHandler{logger: log.NewNoopLogger(), persistenceMetadataManager: metadata, clusterMetadata: clusters,
		chasmVisibilityManager: chasm.NewMockVisibilityManager(ctrl), chasmNsReplClient: &testNamespaceReplicationRepairClient{},
		repairPolicy: policy.policy()}
	response, err := handler.ReconcileNamespaceMutationComponents(t.Context(), &adminservice.ReconcileNamespaceMutationComponentsRequest{
		NamespaceId: "namespace-id", RepairId: "repair-id", Reason: "repair", CutoffTime: timestamppb.New(time.Unix(10, 0)),
		VerifiedConfigVersion: wrapperspb.Int64(0), VerifiedFailoverVersion: wrapperspb.Int64(0),
		AuthoritativePeerClusters: []string{"cluster-b"}, VerifiedPeerClusters: []string{"cluster-b"},
	})
	var refused *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &refused)
	require.ErrorContains(t, err, "repair does not support this namespace")
	require.Nil(t, response)
	require.Equal(t, []string{"namespace-id/payments"}, policy.calls)
}
