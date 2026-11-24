package replication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	otellog "go.opentelemetry.io/otel/log"
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
	"go.temporal.io/server/common/wideevents"
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
		eventCapture                *eventCaptureLogger
	}

	// fixedClustersResolver reports a fixed cluster list regardless of the namespace's replication config.
	fixedClustersResolver struct {
		namespace.ReplicationResolver
		clusters []string
	}
)

func (r fixedClustersResolver) ClusterNames(string) []string {
	return r.clusters
}

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
	s.eventCapture = &eventCaptureLogger{}
	s.eagerNamespaceRefresher = s.newRefresher(namespace.NewDefaultReplicationResolverFactory())
}

func (s *EagerNamespaceRefresherSuite) newRefresher(resolverFactory namespace.ReplicationResolverFactory) EagerNamespaceRefresher {
	return NewEagerNamespaceRefresher(
		s.mockMetadataManager,
		s.mockNamespaceRegistry,
		s.logger,
		s.clientBean,
		s.mockReplicationTaskExecutor,
		resolverFactory,
		mockCurrentCuster,
		s.mockMetricsHandler,
		s.eventCapture,
	)
}

// requireRefreshEvent asserts that exactly one eager refresh event was emitted and returns its details.
func (s *EagerNamespaceRefresherSuite) requireRefreshEvent(outcome string) map[string]any {
	s.Len(s.eventCapture.records, 1)
	attrs := map[string]string{}
	s.eventCapture.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value.AsString()
		return true
	})
	s.Equal(wideevents.NamespaceLifecycleEventName, s.eventCapture.records[0].EventName())
	s.Equal(wideevents.PhaseNamespaceEagerRefresh, attrs["phase"])
	var details map[string]any
	s.NoError(json.Unmarshal([]byte(attrs["details"]), &details))
	s.Equal(outcome, details["outcome"])
	s.Equal(mockCurrentCuster, details["current_cluster"])
	return details
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
	s.NoError(err)
	s.Equal(namespaceId, ns.ID())
	details := s.requireRefreshEvent(wideevents.NamespaceEagerRefreshRefreshed)
	s.Equal(enumsspb.NAMESPACE_OPERATION_CREATE.String(), details["operation"])
	s.Equal("currentCluster", details["source_cluster"])
	s.NotContains(details, "error")
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_UpdateSuccess() {
	namespaceId := namespace.ID("abc")
	nsName := "another-random-namespace-name"
	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Id:    namespaceId.String(),
			Name:  nsName,
			State: enumspb.NAMESPACE_STATE_REGISTERED,
			Data:  make(map[string]string)},
		RestoreInfo: &persistencespb.NamespaceRestoreInfo{
			RestoreId:   "restore-id",
			HostCluster: cluster.TestAlternativeClusterName,
			State:       enumspb.NAMESPACE_RESTORE_STATE_RUNNING,
		},
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
		RestoreInfo:        nsResponse.GetRestoreInfo(),
	}
	s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), task).Return(nil).Times(1)
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceId).Return(nil, nil).Times(1)
	nsFromResponse, err := fromAdminClientAPIResponse(nsResponse)
	s.NoError(err)
	s.mockNamespaceRegistry.EXPECT().RefreshNamespaceById(namespaceId).Return(nsFromResponse, nil).Times(1)
	ns, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceId, "currentCluster")
	s.NoError(err)
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
	s.ErrorAs(err, new(*serviceerror.FailedPrecondition))
	details := s.requireRefreshEvent(wideevents.NamespaceEagerRefreshNotMember)
	s.Contains(details["error"], "Namespace does not belong to current cluster")
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
	s.Equal(expectedError, err)
	s.requireRefreshEvent(wideevents.NamespaceEagerRefreshApplyFailed)
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
	s.ErrorAs(err, new(*serviceerror.FailedPrecondition))
	s.requireRefreshEvent(wideevents.NamespaceEagerRefreshNotGlobal)
}

func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_FetchFails() {
	namespaceID := namespace.ID("abc")
	expectedError := serviceerror.NewUnavailable("source unavailable")
	s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, expectedError).Times(1)

	_, err := s.eagerNamespaceRefresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceID, "currentCluster")
	s.Equal(expectedError, err)
	s.requireRefreshEvent(wideevents.NamespaceEagerRefreshFetchFailed)
}

// Membership is the cluster list reported by the namespace's replication resolver, which may differ
// from the replication config's cluster list.
func (s *EagerNamespaceRefresherSuite) TestSyncNamespaceFromSourceCluster_ResolverDecidesMembership() {
	namespaceID := namespace.ID("abc")
	testCases := []struct {
		name             string
		configClusters   []string
		resolverClusters []string
		member           bool
	}{
		{
			name:             "member although config cluster list excludes current",
			configClusters:   []string{"not_current_cluster_1"},
			resolverClusters: []string{"not_current_cluster_1", mockCurrentCuster},
			member:           true,
		},
		{
			name:             "not a member although config cluster list includes current",
			configClusters:   []string{mockCurrentCuster},
			resolverClusters: []string{"not_current_cluster_1"},
			member:           false,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.eventCapture.records = nil
			var clusters []*replicationpb.ClusterReplicationConfig
			for _, c := range tc.configClusters {
				clusters = append(clusters, &replicationpb.ClusterReplicationConfig{ClusterName: c})
			}
			nsResponse := &adminservice.GetNamespaceResponse{
				Info: &namespacepb.NamespaceInfo{
					Id:    namespaceID.String(),
					Name:  "ns-name",
					State: enumspb.NAMESPACE_STATE_REGISTERED,
				},
				ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
					ActiveClusterName: cluster.TestAlternativeClusterName,
					Clusters:          clusters,
				},
				IsGlobalNamespace: true,
			}
			s.remoteAdminClient.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nsResponse, nil).Times(1)
			refresher := s.newRefresher(func(detail *persistencespb.NamespaceDetail) namespace.ReplicationResolver {
				return fixedClustersResolver{
					ReplicationResolver: namespace.NewDefaultReplicationResolverFactory()(detail),
					clusters:            tc.resolverClusters,
				}
			})

			if !tc.member {
				_, err := refresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceID, "currentCluster")
				s.ErrorAs(err, new(*serviceerror.FailedPrecondition))
				s.requireRefreshEvent(wideevents.NamespaceEagerRefreshNotMember)
				return
			}
			s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nil, serviceerror.NewNamespaceNotFound("namespace not found")).Times(1)
			s.mockReplicationTaskExecutor.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil).Times(1)
			nsFromResponse, err := fromAdminClientAPIResponse(nsResponse)
			s.NoError(err)
			s.mockNamespaceRegistry.EXPECT().RefreshNamespaceById(namespaceID).Return(nsFromResponse, nil).Times(1)
			ns, err := refresher.SyncNamespaceFromSourceCluster(context.Background(), namespaceID, "currentCluster")
			s.NoError(err)
			s.Equal(namespaceID, ns.ID())
			s.requireRefreshEvent(wideevents.NamespaceEagerRefreshRefreshed)
		})
	}
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
