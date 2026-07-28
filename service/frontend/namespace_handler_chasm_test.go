package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	nsreplpb "go.temporal.io/server/chasm/lib/nsrepl/gen/nsreplpb/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// mockNamespaceReplicationServiceClient is a capturing stand-in for the
// history-side NamespaceReplicationService client. It records every
// TriggerNamespaceMutation request so tests can assert the fan-out (operation +
// peer cells) the frontend computed. Hand-rolled (rather than gomock) because the
// proto mock generator intentionally skips chasm/lib services, matching the
// hand-rolled test-double convention in the CHASM libraries. Calls are
// synchronous within the handler, so no locking is needed.
type mockNamespaceReplicationServiceClient struct {
	requests []*nsreplpb.TriggerNamespaceMutationRequest
}

func (m *mockNamespaceReplicationServiceClient) TriggerNamespaceMutation(
	_ context.Context,
	in *nsreplpb.TriggerNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*nsreplpb.TriggerNamespaceMutationResponse, error) {
	m.requests = append(m.requests, in)
	return &nsreplpb.TriggerNamespaceMutationResponse{
		NewVersion: in.GetMutation().GetExpectedVersion() + 1,
	}, nil
}

// namespaceHandlerCHASMSuite exercises the frontend namespace handler with the
// CHASM replication transport enabled, focusing on the peer fan-out computed for
// globalization / de-globalization and cluster add/remove. It asserts against the
// captured TriggerNamespaceMutation requests rather than reaching into history.
type namespaceHandlerCHASMSuite struct {
	suite.Suite

	controller          *gomock.Controller
	mockMetadataMgr     *persistence.MockMetadataManager
	mockClusterMetadata *cluster.MockMetadata
	mockProducer        *persistence.MockNamespaceReplicationQueue
	mockNsReplClient    *mockNamespaceReplicationServiceClient
	config              *Config
	handler             *namespaceHandler
}

func TestNamespaceHandlerCHASMSuite(t *testing.T) {
	suite.Run(t, new(namespaceHandlerCHASMSuite))
}

func (s *namespaceHandlerCHASMSuite) SetupTest() {
	logger := log.NewNoopLogger()
	dcCollection := dc.NewNoopCollection()
	s.controller = gomock.NewController(s.T())
	s.mockMetadataMgr = persistence.NewMockMetadataManager(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.mockNsReplClient = &mockNamespaceReplicationServiceClient{}
	s.config = NewConfig(dcCollection, 1024)
	// Enable the CHASM transport for the whole suite.
	s.config.UseCHASMNamespaceReplication = func() bool { return true }

	s.handler = newNamespaceHandler(
		logger,
		s.mockMetadataMgr,
		namespace.NewMockRegistry(s.controller),
		s.mockClusterMetadata,
		nsreplication.NewReplicator(s.mockProducer, logger),
		archiver.NewArchivalMetadata(dcCollection, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
		provider.NewMockArchiverProvider(s.controller),
		clock.NewEventTimeSource(),
		s.config,
		s.mockNsReplClient,
	)
}

func (s *namespaceHandlerCHASMSuite) TearDownTest() {
	s.controller.Finish()
}

// mockClusterInfo wires the cluster-metadata reads the handler + validators make.
// GetNextFailoverVersion is stubbed AnyTimes so tests that do (promotion) and
// don't (pure cluster-list change) trigger a failover-version bump both pass.
func (s *namespaceHandlerCHASMSuite) mockClusterInfo(current string, clusters ...string) {
	info := make(map[string]cluster.ClusterInformation, len(clusters))
	for i, c := range clusters {
		info[c] = cluster.ClusterInformation{Enabled: true, InitialFailoverVersion: int64(i + 1)}
	}
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(info).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(current).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsMasterCluster().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetNextFailoverVersion(gomock.Any(), gomock.Any()).Return(int64(100)).AnyTimes()
}

func chasmTestNamespaceDetail(id, name, active string, clusters []string) *persistencespb.NamespaceDetail {
	return &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: id, Name: name, State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config: &persistencespb.NamespaceConfig{Retention: durationpb.New(24 * time.Hour)},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: active,
			Clusters:          clusters,
		},
		ConfigVersion:   1,
		FailoverVersion: 1,
	}
}

func chasmTestClusterList(names ...string) []*replicationpb.ClusterReplicationConfig {
	out := make([]*replicationpb.ClusterReplicationConfig, 0, len(names))
	for _, n := range names {
		out = append(out, &replicationpb.ClusterReplicationConfig{ClusterName: n})
	}
	return out
}

// lastMutation returns the single captured mutation, failing if the fan-out
// count is not exactly one.
func (s *namespaceHandlerCHASMSuite) lastMutation() *nsreplpb.NamespaceMutation {
	s.Require().Len(s.mockNsReplClient.requests, 1, "expected exactly one TriggerNamespaceMutation call")
	return s.mockNsReplClient.requests[0].GetMutation()
}

// Globalization at create: registering a global namespace with two clusters must
// fan out to the peer (not the origin), and must NOT write metadata or publish to
// the legacy queue directly — the CHASM component owns both.
func (s *namespaceHandlerCHASMSuite) TestRegisterNamespace_GlobalMultiCluster_FansOutToPeer() {
	current := "cluster1"
	s.mockClusterInfo(current, "cluster1", "cluster2")
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Times(0)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "global-ns",
		Description:                      "global-ns",
		WorkflowExecutionRetentionPeriod: durationpb.New(10 * 24 * time.Hour),
		ActiveClusterName:                current,
		Clusters:                         chasmTestClusterList("cluster1", "cluster2"),
		IsGlobalNamespace:                true,
	})
	s.NoError(err)

	m := s.lastMutation()
	s.Equal(nsreplpb.NAMESPACE_OPERATION_CREATE, m.GetOperation())
	s.ElementsMatch([]string{"cluster2"}, m.GetPeerCells())
}

// Registering a single-cluster global namespace has no peer, so the CHASM
// transport is not engaged (falls through to the direct write + legacy path).
func (s *namespaceHandlerCHASMSuite) TestRegisterNamespace_GlobalSingleCluster_NoFanOut() {
	current := "cluster1"
	s.mockClusterInfo(current, "cluster1")
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, &serviceerror.NamespaceNotFound{})
	s.mockMetadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{ID: "id"}, nil)

	_, err := s.handler.RegisterNamespace(context.Background(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "global-ns",
		Description:                      "global-ns",
		WorkflowExecutionRetentionPeriod: durationpb.New(10 * 24 * time.Hour),
		ActiveClusterName:                current,
		Clusters:                         chasmTestClusterList("cluster1"),
		IsGlobalNamespace:                true,
	})
	s.NoError(err)
	s.Empty(s.mockNsReplClient.requests, "single-cluster global namespace should not use the CHASM transport")
}

// Adding a cluster ([c1,c2] -> [c1,c2,c3]) fans out to both the retained and the
// newly added peer.
func (s *namespaceHandlerCHASMSuite) TestUpdateNamespace_AddCluster_FansOutToNewPeer() {
	current := "cluster1"
	s.mockClusterInfo(current, "cluster1", "cluster2", "cluster3")
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 10}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		IsGlobalNamespace: true,
		Namespace:         chasmTestNamespaceDetail("ns-id", "global-ns", current, []string{"cluster1", "cluster2"}),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: "global-ns",
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: chasmTestClusterList("cluster1", "cluster2", "cluster3"),
		},
	})
	s.NoError(err)

	m := s.lastMutation()
	s.Equal(nsreplpb.NAMESPACE_OPERATION_UPDATE, m.GetOperation())
	s.ElementsMatch([]string{"cluster2", "cluster3"}, m.GetPeerCells())
}

// De-globalization / remove peer ([c1,c2,c3] -> [c1,c2]): the removed cluster
// (c3) must STILL receive the final mutation so it learns it is no longer a
// participant. This is the regression the previous-cluster union guards against.
func (s *namespaceHandlerCHASMSuite) TestUpdateNamespace_RemoveCluster_StillNotifiesRemovedPeer() {
	current := "cluster1"
	s.mockClusterInfo(current, "cluster1", "cluster2", "cluster3")
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 10}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		IsGlobalNamespace: true,
		Namespace:         chasmTestNamespaceDetail("ns-id", "global-ns", current, []string{"cluster1", "cluster2", "cluster3"}),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: "global-ns",
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: chasmTestClusterList("cluster1", "cluster2"),
		},
	})
	s.NoError(err)

	m := s.lastMutation()
	s.ElementsMatch([]string{"cluster2", "cluster3"}, m.GetPeerCells(),
		"removed cluster3 must still receive the final mutation")
}

// Remove peer down to a single cluster ([c1,c2] -> [c1]): before the previous-
// cluster union, this produced zero peers and silently stranded c2. It must still
// notify c2.
func (s *namespaceHandlerCHASMSuite) TestUpdateNamespace_RemoveToSingleCluster_StillNotifiesRemovedPeer() {
	current := "cluster1"
	s.mockClusterInfo(current, "cluster1", "cluster2")
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 10}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		IsGlobalNamespace: true,
		Namespace:         chasmTestNamespaceDetail("ns-id", "global-ns", current, []string{"cluster1", "cluster2"}),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace: "global-ns",
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: chasmTestClusterList("cluster1"),
		},
	})
	s.NoError(err)

	m := s.lastMutation()
	s.ElementsMatch([]string{"cluster2"}, m.GetPeerCells(),
		"removed cluster2 must still be notified even when the new list is single-cluster")
}

// Globalization via promotion (local [c1] -> global [c1,c2]) routes through the
// CHASM transport as an UPDATE and fans out to the new peer.
func (s *namespaceHandlerCHASMSuite) TestUpdateNamespace_PromoteLocalToGlobal_FansOutToPeer() {
	current := "cluster1"
	s.mockClusterInfo(current, "cluster1", "cluster2")
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 10}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		IsGlobalNamespace: false,
		Namespace:         chasmTestNamespaceDetail("ns-id", "local-ns", current, []string{"cluster1"}),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Times(0)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace:        "local-ns",
		PromoteNamespace: true,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			Clusters: chasmTestClusterList("cluster1", "cluster2"),
		},
	})
	s.NoError(err)

	m := s.lastMutation()
	s.Equal(nsreplpb.NAMESPACE_OPERATION_UPDATE, m.GetOperation())
	s.ElementsMatch([]string{"cluster2"}, m.GetPeerCells())
}

// A local (non-global) namespace update must NOT use the CHASM transport even
// with the flag on — it writes directly and never fans out.
func (s *namespaceHandlerCHASMSuite) TestUpdateNamespace_LocalNamespace_DoesNotUseCHASM() {
	current := "cluster1"
	s.mockClusterInfo(current, "cluster1")
	s.mockMetadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 10}, nil).AnyTimes()
	s.mockMetadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
		IsGlobalNamespace: false,
		Namespace:         chasmTestNamespaceDetail("ns-id", "local-ns", current, []string{"cluster1"}),
	}, nil)
	s.mockMetadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	s.mockProducer.EXPECT().Publish(gomock.Any(), gomock.Any()).Times(0)

	_, err := s.handler.UpdateNamespace(context.Background(), &workflowservice.UpdateNamespaceRequest{
		Namespace:  "local-ns",
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{Description: "new description"},
	})
	s.NoError(err)
	s.Empty(s.mockNsReplClient.requests, "local namespace update must not engage the CHASM transport")
}
