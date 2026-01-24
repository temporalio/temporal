package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

const mockCurrentCuster = "current_cluster_1"

type (
	EagerNamespaceRefresherSuite struct {
		suite.Suite
		*require.Assertions

		controller                  *gomock.Controller
		mockShard                   *shard.ContextTest
		mockMetadataManager         *persistence.MockMetadataManager
		mockNamespaceRegistry       *namespace.MockRegistry
		eagerNamespaceRefresher     EagerNamespaceRefresher
		logger                      log.Logger
		clientBean                  *client.MockBean
		mockReplicationTaskExecutor *nsreplication.MockTaskExecutor
		currentCluster              string
		mockMetricsHandler          metrics.Handler
		remoteAdminClient           *adminservicemock.MockAdminServiceClient
	}
)

func (s *EagerNamespaceRefresherSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
	s.mockMetadataManager = persistence.NewMockMetadataManager(s.controller)
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.remoteAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(s.remoteAdminClient, nil).AnyTimes()
	scope := tally.NewTestScope("test", nil)
	s.mockReplicationTaskExecutor = nsreplication.NewMockTaskExecutor(s.controller)
	s.mockMetricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(
		metrics.ServiceNameTag("serviceName"))
	s.eagerNamespaceRefresher = NewEagerNamespaceRefresher(
		s.mockMetadataManager,
		s.mockNamespaceRegistry,
		s.logger,
		s.clientBean,
		s.mockReplicationTaskExecutor,
		mockCurrentCuster,
		s.mockMetricsHandler,
	)
}

func TestEagerNamespaceRefresherSuite(t *testing.T) {
	suite.Run(t, new(EagerNamespaceRefresherSuite))
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_CreateSuccess() {
	namespaceId := namespace.ID("abc")
	nsName := "another-random-namespace-name"
	nsResponse := adminservice.GetNamespaceResponse_builder{
		Info: namespacepb.NamespaceInfo_builder{
			Id:    namespaceId.String(),
			Name:  nsName,
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				replicationpb.ClusterReplicationConfig_builder{ClusterName: mockCurrentCuster}.Build(),
				replicationpb.ClusterReplicationConfig_builder{ClusterName: "not_current_cluster_1"}.Build(),
			},
		}.Build(),
		IsGlobalNamespace: true,
	}.Build()
	task := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Id:                 nsResponse.GetInfo().GetId(),
		Info:               nsResponse.GetInfo(),
		Config:             nsResponse.GetConfig(),
		ReplicationConfig:  nsResponse.GetReplicationConfig(),
		ConfigVersion:      nsResponse.GetConfigVersion(),
		FailoverVersion:    nsResponse.GetFailoverVersion(),
		FailoverHistory:    nsResponse.GetFailoverHistory(),
	}.Build()
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), adminservice.GetNamespaceRequest_builder{
		Id: proto.String(namespaceId.String()),
	}.Build()).Return(nsResponse, nil)
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task).Return(nil).Times(1)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)
	nsFromResponse, err := fromAdminClientAPIResponse(nsResponse)
	s.NoError(err)
	s.mockNamespaceRegistry.EXPECT().RefreshNamespaceById(namespaceId).Return(nsFromResponse, nil).Times(1)
	ns, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Nil(err)
	s.Equal(namespaceId, ns.ID())
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_UpdateSuccess() {
	namespaceId := namespace.ID("abc")
	nsName := "another-random-namespace-name"
	nsResponse := adminservice.GetNamespaceResponse_builder{
		Info: namespacepb.NamespaceInfo_builder{
			Id:    namespaceId.String(),
			Name:  nsName,
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				replicationpb.ClusterReplicationConfig_builder{ClusterName: mockCurrentCuster}.Build(),
				replicationpb.ClusterReplicationConfig_builder{ClusterName: "not_current_cluster_1"}.Build(),
			},
		}.Build(),
		IsGlobalNamespace: true,
	}.Build()
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), adminservice.GetNamespaceRequest_builder{
		Id: proto.String(namespaceId.String()),
	}.Build()).Return(nsResponse, nil)
	task := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 nsResponse.GetInfo().GetId(),
		Info:               nsResponse.GetInfo(),
		Config:             nsResponse.GetConfig(),
		ReplicationConfig:  nsResponse.GetReplicationConfig(),
		ConfigVersion:      nsResponse.GetConfigVersion(),
		FailoverVersion:    nsResponse.GetFailoverVersion(),
		FailoverHistory:    nsResponse.GetFailoverHistory(),
	}.Build()
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task).Return(nil).Times(1)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, nil).Times(1)
	nsFromResponse, err := fromAdminClientAPIResponse(nsResponse)
	s.NoError(err)
	s.mockNamespaceRegistry.EXPECT().RefreshNamespaceById(namespaceId).Return(nsFromResponse, nil).Times(1)
	ns, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Nil(err)
	s.Equal(namespaceId, ns.ID())
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_NamespaceNotBelongsToCurrentCluster() {
	namespaceId := namespace.ID("abc")

	nsResponse := adminservice.GetNamespaceResponse_builder{
		Info: namespacepb.NamespaceInfo_builder{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				replicationpb.ClusterReplicationConfig_builder{ClusterName: "not_current_cluster_1"}.Build(),
				replicationpb.ClusterReplicationConfig_builder{ClusterName: "not_current_cluster_2"}.Build(),
			},
		}.Build(),
		IsGlobalNamespace: true,
	}.Build()
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), adminservice.GetNamespaceRequest_builder{
		Id: proto.String(namespaceId.String()),
	}.Build()).Return(nsResponse, nil).Times(1)

	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.IsType(&serviceerror.FailedPrecondition{}, err)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_ExecutorReturnsError() {
	namespaceId := namespace.ID("abc")

	nsResponse := adminservice.GetNamespaceResponse_builder{
		Info: namespacepb.NamespaceInfo_builder{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				replicationpb.ClusterReplicationConfig_builder{ClusterName: mockCurrentCuster}.Build(),
				replicationpb.ClusterReplicationConfig_builder{ClusterName: "not_current_cluster_2"}.Build(),
			},
		}.Build(),
		IsGlobalNamespace: true,
	}.Build()
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), adminservice.GetNamespaceRequest_builder{
		Id: proto.String(namespaceId.String()),
	}.Build()).Return(nsResponse, nil).Times(1)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)

	expectedError := errors.New("some error")
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(expectedError)
	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.Equal(expectedError, err)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_NamespaceIsNotGlobalNamespace() {
	namespaceId := namespace.ID("abc")

	nsResponse := adminservice.GetNamespaceResponse_builder{
		Info: namespacepb.NamespaceInfo_builder{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)}.Build(),
		IsGlobalNamespace: false,
	}.Build()
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), adminservice.GetNamespaceRequest_builder{
		Id: proto.String(namespaceId.String()),
	}.Build()).Return(nsResponse, nil).Times(1)

	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.IsType(&serviceerror.FailedPrecondition{}, err)
}

func fromAdminClientAPIResponse(response *adminservice.GetNamespaceResponse) (*namespace.Namespace, error) {
	info := persistencespb.NamespaceInfo_builder{
		Id:          response.GetInfo().GetId(),
		Name:        response.GetInfo().GetName(),
		State:       response.GetInfo().GetState(),
		Description: response.GetInfo().GetDescription(),
		Owner:       response.GetInfo().GetOwnerEmail(),
		Data:        response.GetInfo().GetData(),
	}.Build()
	config := persistencespb.NamespaceConfig_builder{
		Retention:                    response.GetConfig().GetWorkflowExecutionRetentionTtl(),
		HistoryArchivalState:         response.GetConfig().GetHistoryArchivalState(),
		HistoryArchivalUri:           response.GetConfig().GetHistoryArchivalUri(),
		VisibilityArchivalState:      response.GetConfig().GetVisibilityArchivalState(),
		VisibilityArchivalUri:        response.GetConfig().GetVisibilityArchivalUri(),
		CustomSearchAttributeAliases: response.GetConfig().GetCustomSearchAttributeAliases(),
	}.Build()
	replicationConfig := persistencespb.NamespaceReplicationConfig_builder{
		ActiveClusterName: response.GetReplicationConfig().GetActiveClusterName(),
		State:             response.GetReplicationConfig().GetState(),
		Clusters:          nsreplication.ConvertClusterReplicationConfigFromProto(response.GetReplicationConfig().GetClusters()),
		FailoverHistory:   nsreplication.ConvertFailoverHistoryToPersistenceProto(response.GetFailoverHistory()),
	}.Build()

	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := persistencespb.NamespaceDetail_builder{
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,
		ConfigVersion:     response.GetConfigVersion(),
		FailoverVersion:   response.GetFailoverVersion(),
	}.Build()
	ns, err := namespace.FromPersistentState(
		detail,
		factory(detail),
		namespace.WithGlobalFlag(response.GetIsGlobalNamespace()))
	return ns, err
}
