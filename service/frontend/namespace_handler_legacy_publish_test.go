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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
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
	"google.golang.org/protobuf/types/known/durationpb"
)

// membersResolver models a deployment whose replica membership lives in
// namespace data rather than ReplicationConfig.Clusters.
type membersResolver struct {
	namespace.ReplicationResolver
	members []string
}

func (r membersResolver) ClusterNames(string) []string { return slices.Clone(r.members) }

func membersResolverFactory() namespace.ReplicationResolverFactory {
	defaultFactory := namespace.NewDefaultReplicationResolverFactory()
	return func(detail *persistencespb.NamespaceDetail) namespace.ReplicationResolver {
		resolver := defaultFactory(detail)
		if members, ok := detail.GetInfo().GetData()["members"]; ok {
			return membersResolver{ReplicationResolver: resolver, members: strings.Split(members, ",")}
		}
		return resolver
	}
}

func newLegacyPublishTestHandler(
	ctrl *gomock.Controller,
	metadataMgr persistence.MetadataManager,
	queue persistence.NamespaceReplicationQueue,
	factory namespace.ReplicationResolverFactory,
) *namespaceHandler {
	logger := log.NewNoopLogger()
	dc := dynamicconfig.NewNoopCollection()
	cfg := NewConfig(dc, 32)
	cfg.NamespaceReplicationTransportMode = dynamicconfig.GetStringPropertyFn(dynamicconfig.NamespaceReplicationTransportModeLegacy)
	clusterMetadata := cluster.NewMetadata(
		true,
		10,
		"cell-a",
		"cell-a",
		map[string]cluster.ClusterInformation{
			"cell-a": {Enabled: true, InitialFailoverVersion: 1, RPCAddress: "cell-a:7233"},
			"cell-b": {Enabled: true, InitialFailoverVersion: 2, RPCAddress: "cell-b:7233"},
			"cell-c": {Enabled: true, InitialFailoverVersion: 3, RPCAddress: "cell-c:7233"},
		},
		nil,
		nil,
		logger,
	)
	return newNamespaceHandler(
		logger,
		wideevents.NoopLogger(),
		metadataMgr,
		namespace.NewMockRegistry(ctrl),
		clusterMetadata,
		nsreplication.NewReplicator(queue, logger, nil, dynamicconfig.GetBoolPropertyFn(false), "cell-a"),
		archiver.NewArchivalMetadata(dc, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
		nil,
		clock.NewEventTimeSource(),
		cfg,
		nil,
		metrics.NoopMetricsHandler,
		factory,
	)
}

// The legacy queue publishes a namespace update when the replication resolver
// reports a peer, even when ReplicationConfig.Clusters lists only the source.
// The published task still carries ReplicationConfig.Clusters as stored.
func TestUpdateNamespace_LegacyPublishFollowsResolverMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		factory     namespace.ReplicationResolverFactory
		clusters    []string
		members     string
		newMembers  string
		wantPublish bool
	}{
		{name: "default factory ignores members", factory: namespace.NewDefaultReplicationResolverFactory(),
			clusters: []string{"cell-a"}, members: "cell-a,cell-b,cell-c"},
		{name: "default factory follows the cluster list", factory: namespace.NewDefaultReplicationResolverFactory(),
			clusters: []string{"cell-a", "cell-b"}, members: "cell-a", wantPublish: true},
		{name: "members beyond the cluster list", factory: membersResolverFactory(),
			clusters: []string{"cell-a"}, members: "cell-a,cell-b,cell-c", wantPublish: true},
		{name: "members narrower than the cluster list", factory: membersResolverFactory(),
			clusters: []string{"cell-a", "cell-b"}, members: "cell-a"},
		{name: "members shrink to the source", factory: membersResolverFactory(),
			clusters: []string{"cell-a"}, members: "cell-a,cell-b", newMembers: "cell-a", wantPublish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadataMgr := persistence.NewMockMetadataManager(ctrl)
			queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			handler := newLegacyPublishTestHandler(ctrl, metadataMgr, queue, tc.factory)

			metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 7}, nil)
			metadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					Info: &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED,
						Data: map[string]string{"members": tc.members}},
					Config:            &persistencespb.NamespaceConfig{Retention: durationpb.New(24 * time.Hour), BadBinaries: &namespacepb.BadBinaries{}},
					ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cell-a", Clusters: slices.Clone(tc.clusters)},
					FailoverVersion:   1,
				},
				IsGlobalNamespace: true,
			}, nil)
			metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
			if tc.wantPublish {
				queue.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, task *replicationspb.ReplicationTask) error {
					attrs := task.GetNamespaceTaskAttributes()
					require.Equal(t, "updated", attrs.GetInfo().GetDescription())
					var clusters []string
					for _, c := range attrs.GetReplicationConfig().GetClusters() {
						clusters = append(clusters, c.GetClusterName())
					}
					require.Equal(t, tc.clusters, clusters)
					return nil
				})
			}

			updateInfo := &namespacepb.UpdateNamespaceInfo{Description: "updated"}
			if tc.newMembers != "" {
				updateInfo.Data = map[string]string{"members": tc.newMembers}
			}
			_, err := handler.UpdateNamespace(t.Context(), &workflowservice.UpdateNamespaceRequest{Namespace: "ns", UpdateInfo: updateInfo})
			require.NoError(t, err)
		})
	}
}

// Registration publishes the same way: the resolver's members decide.
func TestRegisterNamespace_LegacyPublishFollowsResolverMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		factory     namespace.ReplicationResolverFactory
		wantPublish bool
	}{
		{name: "default factory", factory: namespace.NewDefaultReplicationResolverFactory()},
		{name: "members beyond the cluster list", factory: membersResolverFactory(), wantPublish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadataMgr := persistence.NewMockMetadataManager(ctrl)
			queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			handler := newLegacyPublishTestHandler(ctrl, metadataMgr, queue, tc.factory)

			metadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("ns"))
			metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{ID: "namespace-id"}, nil)
			if tc.wantPublish {
				queue.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
			}

			_, err := handler.RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
				Namespace:                        "ns",
				WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
				Clusters:                         []*replicationpb.ClusterReplicationConfig{{ClusterName: "cell-a"}},
				ActiveClusterName:                "cell-a",
				Data:                             map[string]string{"members": "cell-a,cell-b"},
				IsGlobalNamespace:                true,
			})
			require.NoError(t, err)
		})
	}
}
