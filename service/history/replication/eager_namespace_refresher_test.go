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
	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespaceId.String(),
			Name:  nsName,
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_1"},
			},
		},
		IsGlobalNamespace: true,
	}
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Id:                 nsResponse.GetInfo().Id,
		Info:               nsResponse.GetInfo(),
		Config:             nsResponse.GetConfig(),
		ReplicationConfig:  nsResponse.GetReplicationConfig(),
		ConfigVersion:      nsResponse.GetConfigVersion(),
		FailoverVersion:    nsResponse.GetFailoverVersion(),
		FailoverHistory:    nsResponse.GetFailoverHistory(),
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil)
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
	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespaceId.String(),
			Name:  nsName,
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_1"},
			},
		},
		IsGlobalNamespace: true,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil)
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 nsResponse.GetInfo().Id,
		Info:               nsResponse.GetInfo(),
		Config:             nsResponse.GetConfig(),
		ReplicationConfig:  nsResponse.GetReplicationConfig(),
		ConfigVersion:      nsResponse.GetConfigVersion(),
		FailoverVersion:    nsResponse.GetFailoverVersion(),
		FailoverHistory:    nsResponse.GetFailoverHistory(),
	}
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

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: "not_current_cluster_1"},
				{ClusterName: "not_current_cluster_2"},
			},
		},
		IsGlobalNamespace: true,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)

	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.IsType(&serviceerror.FailedPrecondition{}, err)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_ExecutorReturnsError() {
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: mockCurrentCuster},
				{ClusterName: "not_current_cluster_2"},
			},
		},
		IsGlobalNamespace: true,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)

	expectedError := errors.New("some error")
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(expectedError)
	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.Equal(expectedError, err)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_NamespaceIsNotGlobalNamespace() {
	namespaceId := namespace.ID("abc")

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespace.NewID().String(),
			Name:  "another random namespace name",
			State: enumspb.NAMESPACE_STATE_DELETED,
			Data:  make(map[string]string)},
		IsGlobalNamespace: false,
	}
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceId.String(),
		},
	}).Return(nsResponse, nil).Times(1)

	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.Error(err)
	s.IsType(&serviceerror.FailedPrecondition{}, err)
}

func fromAdminClientAPIResponse(response *adminservice.GetNamespaceResponse) (*namespace.Namespace, error) {
	info := &persistencespb.NamespaceInfo{
		Id:          response.GetInfo().GetId(),
		Name:        response.GetInfo().GetName(),
		State:       response.GetInfo().GetState(),
		Description: response.GetInfo().GetDescription(),
		Owner:       response.GetInfo().GetOwnerEmail(),
		Data:        response.GetInfo().GetData(),
	}
	config := &persistencespb.NamespaceConfig{
		Retention:                    response.GetConfig().GetWorkflowExecutionRetentionTtl(),
		HistoryArchivalState:         response.GetConfig().GetHistoryArchivalState(),
		HistoryArchivalUri:           response.GetConfig().GetHistoryArchivalUri(),
		VisibilityArchivalState:      response.GetConfig().GetVisibilityArchivalState(),
		VisibilityArchivalUri:        response.GetConfig().GetVisibilityArchivalUri(),
		CustomSearchAttributeAliases: response.GetConfig().GetCustomSearchAttributeAliases(),
	}
	replicationConfig := &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: response.GetReplicationConfig().GetActiveClusterName(),
		State:             response.GetReplicationConfig().GetState(),
		Clusters:          nsreplication.ConvertClusterReplicationConfigFromProto(response.GetReplicationConfig().GetClusters()),
		FailoverHistory:   nsreplication.ConvertFailoverHistoryToPersistenceProto(response.GetFailoverHistory()),
	}

	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,
		ConfigVersion:     response.ConfigVersion,
		FailoverVersion:   response.GetFailoverVersion(),
	}
	ns, err := namespace.FromPersistentState(
		detail,
		factory(detail),
		namespace.WithGlobalFlag(response.IsGlobalNamespace))
	return ns, err
}
