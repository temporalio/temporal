package frontend

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
)

// dataMembershipResolver models a deployment whose replica membership lives in
// namespace data rather than ReplicationConfig.Clusters.
type dataMembershipResolver struct {
	namespace.ReplicationResolver
	members []string
}

func (r dataMembershipResolver) ClusterNames(string) []string { return slices.Clone(r.members) }

func dataMembershipFactory() namespace.ReplicationResolverFactory {
	defaultFactory := namespace.NewDefaultReplicationResolverFactory()
	return func(detail *persistencespb.NamespaceDetail) namespace.ReplicationResolver {
		resolver := defaultFactory(detail)
		if members, ok := detail.GetInfo().GetData()["members"]; ok {
			return dataMembershipResolver{ReplicationResolver: resolver, members: strings.Split(members, ",")}
		}
		return resolver
	}
}

func TestNamespaceReplicationUsesResolverMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		mode           string
		factory        namespace.ReplicationResolverFactory
		newMembers     string
		wantPeers      []string
		wantRemoved    []string
		wantPublish    bool
		wantLocalWrite bool
	}{
		// ReplicationConfig.Clusters is only cell-a, so stock OSS never replicates.
		{name: "chasm/default factory", mode: dynamicconfig.NamespaceReplicationTransportModeCHASM,
			factory: namespace.NewDefaultReplicationResolverFactory(), wantLocalWrite: true},
		{name: "chasm/membership factory", mode: dynamicconfig.NamespaceReplicationTransportModeCHASM,
			factory: dataMembershipFactory(), wantPeers: []string{"cell-b", "cell-c"}},
		{name: "chasm/membership factory, removed member stays a peer", mode: dynamicconfig.NamespaceReplicationTransportModeCHASM,
			factory: dataMembershipFactory(), newMembers: "cell-a,cell-c", wantPeers: []string{"cell-b", "cell-c"}, wantRemoved: []string{"cell-b"}},
		{name: "legacy/default factory", mode: dynamicconfig.NamespaceReplicationTransportModeLegacy,
			factory: namespace.NewDefaultReplicationResolverFactory(), wantLocalWrite: true},
		// The legacy path keeps its own ReplicationConfig.Clusters decision.
		{name: "legacy/membership factory", mode: dynamicconfig.NamespaceReplicationTransportModeLegacy,
			factory: dataMembershipFactory(), wantLocalWrite: true},
		// Shadow validates the membership peer list without changing the legacy path.
		{name: "shadow/membership factory", mode: dynamicconfig.NamespaceReplicationTransportModeShadow,
			factory: dataMembershipFactory(), wantLocalWrite: true, wantPeers: []string{"cell-b", "cell-c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := persistence.NewMockMetadataManager(ctrl)
			clusters := cluster.NewMockMetadata(ctrl)
			queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			logger := log.NewNoopLogger()
			dc := dynamicconfig.NewNoopCollection()
			cfg := NewConfig(dc, 32)
			// Only the updated namespace uses the mode under test.
			cfg.NamespaceReplicationTransportMode = func(namespace string) string {
				if namespace == "ns" {
					return tc.mode
				}
				return dynamicconfig.NamespaceReplicationTransportModeLegacy
			}
			client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
			handler := newNamespaceHandler(logger, wideevents.NoopLogger(), metadata, nil, clusters,
				nsreplication.NewReplicator(queue, logger, nil, dynamicconfig.GetBoolPropertyFn(false), "cell-a"),
				archiver.NewArchivalMetadata(dc, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
				nil, clock.NewEventTimeSource(), cfg, client, metrics.NoopMetricsHandler, tc.factory)
			clusters.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
			clusters.EXPECT().IsMasterCluster().Return(true).AnyTimes()
			clusters.EXPECT().GetCurrentClusterName().Return("cell-a").AnyTimes()
			clusters.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
				"cell-a": {Enabled: true}, "cell-b": {Enabled: true}, "cell-c": {Enabled: true},
			}).AnyTimes()
			metadata.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 7}, nil)
			metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED,
						Data: map[string]string{"members": "cell-a,cell-b,cell-c"}},
					Config:            &persistencespb.NamespaceConfig{BadBinaries: &namespacepb.BadBinaries{}},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cell-a", Clusters: []string{"cell-a"}},
					FailoverVersion:   1,
				}, IsGlobalNamespace: true,
			}, nil)
			if tc.wantLocalWrite {
				metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
			}
			if tc.wantPublish {
				queue.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, task *replicationspb.ReplicationTask) error {
					require.Equal(t, "updated", task.GetNamespaceTaskAttributes().GetInfo().GetDescription())
					return nil
				})
			}
			updateInfo := &namespacepb.UpdateNamespaceInfo{Description: "updated"}
			if tc.newMembers != "" {
				updateInfo.Data = map[string]string{"members": tc.newMembers}
			}
			_, err := handler.UpdateNamespace(t.Context(), &workflowservice.UpdateNamespaceRequest{Namespace: "ns", UpdateInfo: updateInfo})
			require.NoError(t, err)

			// Shadow triggers asynchronously, so wait briefly either way.
			select {
			case request := <-client.requests:
				require.NotNil(t, tc.wantPeers, "unexpected CHASM trigger")
				require.Equal(t, tc.mode == dynamicconfig.NamespaceReplicationTransportModeShadow, request.GetMutation().GetShadow())
				peers := request.GetMutation().GetPeerCells()
				slices.Sort(peers)
				require.Equal(t, tc.wantPeers, peers)
				require.NotNil(t, request.GetMutation().GetRemovedPeers(), "the source always records the removed set")
				require.Equal(t, tc.wantRemoved, request.GetMutation().GetRemovedPeers().GetCells())
			case <-time.After(2 * time.Second):
				require.Nil(t, tc.wantPeers, "expected a CHASM trigger")
			}
		})
	}
}

func TestRemovedPeerValidationUsesResolverMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		factory namespace.ReplicationResolverFactory
		wantErr bool
	}{
		// The receiver is still in the legacy cluster list, so the default policy rejects the removal.
		{name: "default factory", wantErr: true},
		// Membership says the receiver was removed, matching what the source computed.
		{name: "membership factory", factory: dataMembershipFactory()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			clusters := cluster.NewMockMetadata(ctrl)
			clusters.EXPECT().GetCurrentClusterName().Return("cellC").AnyTimes()
			clusters.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
			executor := nsreplication.NewMockMutationTaskExecutor(ctrl)
			if !tc.wantErr {
				executor.EXPECT().ExecuteWithOutcome(gomock.Any(), gomock.Any(), gomock.Any()).Return(nsreplication.ApplyOutcomeApplied, nil)
			}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: clusters, namespaceMutationExecutor: executor,
				metricsHandler: metrics.NoopMetricsHandler, eventLogger: wideevents.NoopLogger(),
				config:                     &Config{EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(false)},
				replicationResolverFactory: tc.factory}
			task := removalTestTask(true)
			task.ReplicationConfig.Clusters = append(task.ReplicationConfig.Clusters, &replicationpb.ClusterReplicationConfig{ClusterName: "cellC"})
			task.Info.Data = map[string]string{"members": "cellA"}
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			resp, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload, RemovedPeer: true})
			if tc.wantErr {
				var invalid *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED, resp.GetOutcome())
		})
	}
}

func TestDeliveryPolicy(t *testing.T) {
	t.Parallel()
	trustAll := func(context.Context, *replicationspb.NamespaceTaskAttributes) (bool, error) { return true, nil }
	deferAll := func(context.Context, *replicationspb.NamespaceTaskAttributes) (bool, error) {
		return true, serviceerror.NewFailedPrecondition("not ready")
	}
	for _, tc := range []struct {
		name          string
		policy        nsreplication.DeliveryPolicy
		target        string
		removedPeer   bool
		wantCode      codes.Code
		wantAddressed bool
	}{
		{name: "delivery addressed to another cluster is rejected", policy: trustAll, target: "cellB", wantCode: codes.FailedPrecondition},
		// The receiver is still in the payload's cluster list, so it disagrees with the source.
		{name: "default policy keeps the removed-peer check", target: "cellC", removedPeer: true, wantCode: codes.InvalidArgument},
		{name: "trusted delivery follows the source", policy: trustAll, target: "cellC", removedPeer: true, wantAddressed: true},
		// Older sources do not address deliveries, so the receiver checks membership itself.
		{name: "unaddressed delivery is not trusted", policy: trustAll, removedPeer: true, wantCode: codes.InvalidArgument},
		{name: "default policy admits through the admission policy", target: "cellC"},
		// A deferral wins over the removed-peer check, which would end the delivery for good.
		{name: "deferred delivery returns before the removed-peer check", policy: deferAll, target: "cellC", removedPeer: true, wantCode: codes.FailedPrecondition},
		{name: "unaddressed delivery can be deferred", policy: deferAll, wantCode: codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			clusters := cluster.NewMockMetadata(ctrl)
			clusters.EXPECT().GetCurrentClusterName().Return("cellC").AnyTimes()
			clusters.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
			executor := nsreplication.NewMockMutationTaskExecutor(ctrl)
			if tc.wantCode == codes.OK {
				executor.EXPECT().ExecuteWithOutcome(gomock.Any(), gomock.Any(), nsreplication.MutationApplyOptions{
					Admission: nsreplication.AdmissionOptions{RemovedPeer: tc.removedPeer, Addressed: tc.wantAddressed},
				}).Return(nsreplication.ApplyOutcomeApplied, nil)
			}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: clusters, namespaceMutationExecutor: executor,
				metricsHandler: metrics.NoopMetricsHandler, eventLogger: wideevents.NoopLogger(),
				config:         &Config{EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(false)},
				deliveryPolicy: tc.policy}
			task := removalTestTask(true)
			task.ReplicationConfig.Clusters = append(task.ReplicationConfig.Clusters, &replicationpb.ClusterReplicationConfig{ClusterName: "cellC"})
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			resp, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{
				NamespaceTaskPayload: payload, RemovedPeer: tc.removedPeer, TargetCluster: tc.target,
			})
			require.Equal(t, tc.wantCode, serviceerror.ToStatus(err).Code(), "error: %v", err)
			if tc.wantCode != codes.OK {
				return
			}
			require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED, resp.GetOutcome())
		})
	}
}
