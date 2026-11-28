package frontend

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
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

// Only well-formed operator-repair attribution reaches the operator-repair path;
// anything else is rejected before the payload is read or applied.
func TestNamespaceMutationRejectsInvalidAttribution(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		shadow      bool
		attribution *adminservice.NamespaceMutationAttribution
		wantError   string
	}{
		{name: "audit fields without an origin", attribution: &adminservice.NamespaceMutationAttribution{RepairId: "repair-id"},
			wantError: "attribution origin is required"},
		{name: "operator repair as shadow", shadow: true, attribution: &adminservice.NamespaceMutationAttribution{
			Origin: adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR, RepairId: "repair-id", Reason: "repair",
		}, wantError: "cannot use shadow apply"},
		{name: "operator repair without repair id", attribution: &adminservice.NamespaceMutationAttribution{
			Origin: adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR, Reason: "repair",
		}, wantError: "requires repair_id and reason"},
		{name: "operator repair without reason", attribution: &adminservice.NamespaceMutationAttribution{
			Origin: adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR, RepairId: "repair-id",
		}, wantError: "requires repair_id and reason"},
		{name: "unknown origin", attribution: &adminservice.NamespaceMutationAttribution{
			Origin: adminservice.NamespaceMutationAttribution_Origin(99), RepairId: "repair-id", Reason: "repair",
		}, wantError: "unknown namespace mutation attribution origin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			// No executor expectations: a rejected request must not be applied.
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t),
				metricsHandler: metrics.NoopMetricsHandler, namespaceMutationExecutor: nsreplication.NewMockMutationTaskExecutor(ctrl)}
			payload, err := nsreplication.MarshalNamespaceTask(shadowObservationTask())
			require.NoError(t, err)
			_, err = handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{
				NamespaceTaskPayload: payload, SourceCluster: "cellA", Shadow: tc.shadow, Attribution: tc.attribution,
			})
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalid)
			require.ErrorContains(t, err, tc.wantError)
		})
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

// TestNamespaceRepairPolicyChecksStoredNamespace sends a repair payload that
// omits what the policy refuses. The stored namespace still carries it, so the
// receiver refuses the repair before writing.
func TestNamespaceRepairPolicyChecksStoredNamespace(t *testing.T) {
	t.Parallel()
	for _, storedRefused := range []bool{true, false} {
		t.Run(fmt.Sprintf("stored refused=%v", storedRefused), func(t *testing.T) {
			t.Parallel()
			metadata := persistence.NewMockMetadataManager(gomock.NewController(t))
			storedData := map[string]string{"team": "payments"}
			if storedRefused {
				storedData["membership"] = "elsewhere"
			}
			stored := &persistencespb.NamespaceDetail{
				Info:   &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments", State: enumspb.NAMESPACE_STATE_REGISTERED, Data: storedData},
				Config: &persistencespb.NamespaceConfig{}, ConfigVersion: 16, FailoverVersion: 23,
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cellA", Clusters: []string{"cellA", "cellB"}},
			}
			metadata.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{Name: "payments"}).DoAndReturn(
				func(context.Context, *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
					return &persistence.GetNamespaceResponse{Namespace: proto.CloneOf(stored), IsGlobalNamespace: true}, nil
				}).AnyTimes()
			metadata.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 30}, nil)
			if !storedRefused {
				metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
			}
			policy := &refusingRepairPolicy{key: "membership"}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t),
				metricsHandler: metrics.NoopMetricsHandler, repairPolicy: policy.policy(),
				namespaceMutationExecutor: nsreplication.NewMutationTaskExecutor("cellB", metadata, nsreplication.NewDefaultMutationDataMerger(nsreplication.NewNoopDataMerger()),
					nsreplication.NewDefaultAdmitter(), log.NewNoopLogger(), testhooks.TestHooks{}, nsreplication.WithOperatorRepairPolicy(policy.policy())),
			}
			task := shadowObservationTask()
			task.Info.State = enumspb.NAMESPACE_STATE_REGISTERED
			task.Info.Data = map[string]string{"team": "payments"}
			task.Config = &namespacepb.NamespaceConfig{}
			task.ReplicationConfig = &replicationpb.NamespaceReplicationConfig{ActiveClusterName: "cellA",
				Clusters: []*replicationpb.ClusterReplicationConfig{{ClusterName: "cellA"}, {ClusterName: "cellB"}}}
			request := repairPolicyTestMutation(t, nil)
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			request.NamespaceTaskPayload = payload

			response, err := handler.ApplyNamespaceMutation(t.Context(), request)
			// The payload and then the stored namespace are checked.
			require.Equal(t, []string{"namespace-id/payments", "namespace-id/payments"}, policy.calls)
			if storedRefused {
				var refused *serviceerror.FailedPrecondition
				require.ErrorAs(t, err, &refused)
				require.Nil(t, response)
				return
			}
			require.NoError(t, err)
			require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED, response.GetOutcome())
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

func TestNamespaceRepairPolicyReportedByGetNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		data       map[string]string
		noPolicy   bool
		wantReason string
	}{
		{name: "refused", data: map[string]string{"membership": "elsewhere"}, wantReason: "repair does not support this namespace"},
		{name: "allowed", data: map[string]string{"other": "value"}},
		{name: "no policy", data: map[string]string{"membership": "elsewhere"}, noPolicy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := persistence.NewMockMetadataManager(ctrl)
			metadata.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "namespace-id"}).Return(&persistence.GetNamespaceResponse{
				IsGlobalNamespace: true,
				Namespace: &persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments", State: enumspb.NAMESPACE_STATE_REGISTERED,
						Data: tc.data},
					Config:            &persistencespb.NamespaceConfig{},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cluster-a", Clusters: []string{"cluster-a", "cluster-b"}},
				},
			}, nil)
			handler := &AdminHandler{logger: log.NewNoopLogger(), persistenceMetadataManager: metadata}
			if !tc.noPolicy {
				handler.repairPolicy = (&refusingRepairPolicy{key: "membership"}).policy()
			}
			response, err := handler.GetNamespace(t.Context(), &adminservice.GetNamespaceRequest{
				Attributes: &adminservice.GetNamespaceRequest_Id{Id: "namespace-id"},
			})
			// GetNamespace serves other callers, so a refusal is reported, not returned.
			require.NoError(t, err)
			require.Equal(t, "payments", response.GetInfo().GetName())
			// The scope is always set, so repair can tell this server from an older one.
			require.NotNil(t, response.GetRepairScope())
			if tc.wantReason == "" {
				require.Empty(t, response.GetRepairScope().GetRefusalReason())
				return
			}
			require.Contains(t, response.GetRepairScope().GetRefusalReason(), tc.wantReason)
		})
	}
}
