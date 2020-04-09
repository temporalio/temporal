package resource

import (
	"net"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	"go.temporal.io/temporal-proto/workflowservicemock"
	sdkclient "go.temporal.io/temporal/client"
	sdkmocks "go.temporal.io/temporal/mocks"
	"go.uber.org/zap"

	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/matchingservicemock"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/client/frontend"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
)

type (
	// Test is the test implementation used for testing
	Test struct {
		MetricsScope    tally.Scope
		ClusterMetadata *cluster.MockMetadata

		// other common resources

		NamespaceCache    *cache.MockNamespaceCache
		TimeSource        clock.TimeSource
		PayloadSerializer persistence.PayloadSerializer
		MetricsClient     metrics.Client
		ArchivalMetadata  *archiver.MockArchivalMetadata
		ArchiverProvider  *provider.MockArchiverProvider

		// membership infos

		MembershipMonitor       *membership.MockMonitor
		FrontendServiceResolver *membership.MockServiceResolver
		MatchingServiceResolver *membership.MockServiceResolver
		HistoryServiceResolver  *membership.MockServiceResolver
		WorkerServiceResolver   *membership.MockServiceResolver

		// internal services clients

		SDKClient            sdkclient.Client
		FrontendClient       *workflowservicemock.MockWorkflowServiceClient
		MatchingClient       *matchingservicemock.MockMatchingServiceClient
		HistoryClient        *historyservicemock.MockHistoryServiceClient
		RemoteAdminClient    *adminservicemock.MockAdminServiceClient
		RemoteFrontendClient *workflowservicemock.MockWorkflowServiceClient
		ClientBean           *client.MockBean

		// persistence clients

		MetadataMgr               *mocks.MetadataManager
		TaskMgr                   *mocks.TaskManager
		VisibilityMgr             *mocks.VisibilityManager
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		ShardMgr                  *mocks.ShardManager
		HistoryMgr                *mocks.HistoryV2Manager
		ExecutionMgr              *mocks.ExecutionManager
		PersistenceBean           *persistenceClient.MockBean

		Logger log.Logger
	}
)

var _ Resource = (*Test)(nil)

const (
	testHostName = "test_host"
)

var (
	testHostInfo = membership.NewHostInfo(testHostName, nil)
)

// NewTest returns a new test resource instance
func NewTest(
	controller *gomock.Controller,
	serviceMetricsIndex metrics.ServiceIdx,
) *Test {

	zapLogger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger := loggerimpl.NewLogger(zapLogger)

	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(controller)
	matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	remoteFrontendClient := workflowservicemock.NewMockWorkflowServiceClient(controller)
	remoteAdminClient := adminservicemock.NewMockAdminServiceClient(controller)
	clientBean := client.NewMockBean(controller)
	clientBean.EXPECT().GetFrontendClient().Return(frontendClient).AnyTimes()
	clientBean.EXPECT().GetMatchingClient(gomock.Any()).Return(matchingClient, nil).AnyTimes()
	clientBean.EXPECT().GetHistoryClient().Return(historyClient).AnyTimes()
	clientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(remoteAdminClient).AnyTimes()
	clientBean.EXPECT().GetRemoteFrontendClient(gomock.Any()).Return(remoteFrontendClient).AnyTimes()

	metadataMgr := &mocks.MetadataManager{}
	taskMgr := &mocks.TaskManager{}
	visibilityMgr := &mocks.VisibilityManager{}
	shardMgr := &mocks.ShardManager{}
	historyMgr := &mocks.HistoryV2Manager{}
	executionMgr := &mocks.ExecutionManager{}
	namespaceReplicationQueue := persistence.NewMockNamespaceReplicationQueue(controller)
	namespaceReplicationQueue.EXPECT().Start().AnyTimes()
	namespaceReplicationQueue.EXPECT().Stop().AnyTimes()
	persistenceBean := persistenceClient.NewMockBean(controller)
	persistenceBean.EXPECT().GetMetadataManager().Return(metadataMgr).AnyTimes()
	persistenceBean.EXPECT().GetTaskManager().Return(taskMgr).AnyTimes()
	persistenceBean.EXPECT().GetVisibilityManager().Return(visibilityMgr).AnyTimes()
	persistenceBean.EXPECT().GetHistoryManager().Return(historyMgr).AnyTimes()
	persistenceBean.EXPECT().GetShardManager().Return(shardMgr).AnyTimes()
	persistenceBean.EXPECT().GetExecutionManager(gomock.Any()).Return(executionMgr, nil).AnyTimes()
	persistenceBean.EXPECT().GetNamespaceReplicationQueue().Return(namespaceReplicationQueue).AnyTimes()

	membershipMonitor := membership.NewMockMonitor(controller)
	frontendServiceResolver := membership.NewMockServiceResolver(controller)
	matchingServiceResolver := membership.NewMockServiceResolver(controller)
	historyServiceResolver := membership.NewMockServiceResolver(controller)
	workerServiceResolver := membership.NewMockServiceResolver(controller)
	membershipMonitor.EXPECT().GetResolver(common.FrontendServiceName).Return(frontendServiceResolver, nil).AnyTimes()
	membershipMonitor.EXPECT().GetResolver(common.MatchingServiceName).Return(matchingServiceResolver, nil).AnyTimes()
	membershipMonitor.EXPECT().GetResolver(common.HistoryServiceName).Return(historyServiceResolver, nil).AnyTimes()
	membershipMonitor.EXPECT().GetResolver(common.WorkerServiceName).Return(workerServiceResolver, nil).AnyTimes()

	scope := tally.NewTestScope("test", nil)

	return &Test{
		MetricsScope:    scope,
		ClusterMetadata: cluster.NewMockMetadata(controller),

		// other common resources

		NamespaceCache:    cache.NewMockNamespaceCache(controller),
		TimeSource:        clock.NewRealTimeSource(),
		PayloadSerializer: persistence.NewPayloadSerializer(),
		MetricsClient:     metrics.NewClient(scope, serviceMetricsIndex),
		ArchivalMetadata:  &archiver.MockArchivalMetadata{},
		ArchiverProvider:  &provider.MockArchiverProvider{},

		// membership infos

		MembershipMonitor:       membershipMonitor,
		FrontendServiceResolver: frontendServiceResolver,
		MatchingServiceResolver: matchingServiceResolver,
		HistoryServiceResolver:  historyServiceResolver,
		WorkerServiceResolver:   workerServiceResolver,

		// internal services clients

		SDKClient:            &sdkmocks.Client{},
		FrontendClient:       frontendClient,
		MatchingClient:       matchingClient,
		HistoryClient:        historyClient,
		RemoteAdminClient:    remoteAdminClient,
		RemoteFrontendClient: remoteFrontendClient,
		ClientBean:           clientBean,

		// persistence clients

		MetadataMgr:               metadataMgr,
		TaskMgr:                   taskMgr,
		VisibilityMgr:             visibilityMgr,
		NamespaceReplicationQueue: namespaceReplicationQueue,
		ShardMgr:                  shardMgr,
		HistoryMgr:                historyMgr,
		ExecutionMgr:              executionMgr,
		PersistenceBean:           persistenceBean,

		// logger

		Logger: logger,
	}
}

// Start for testing
func (s *Test) Start() {

}

// Stop for testing
func (s *Test) Stop() {

}

// static infos

// GetServiceName for testing
func (s *Test) GetServiceName() string {
	panic("user should implement this method for test")
}

// GetHostName for testing
func (s *Test) GetHostName() string {
	return testHostInfo.Identity()
}

// GetHostInfo for testing
func (s *Test) GetHostInfo() *membership.HostInfo {
	return testHostInfo
}

// GetClusterMetadata for testing
func (s *Test) GetClusterMetadata() cluster.Metadata {
	return s.ClusterMetadata
}

// other common resources

// GetNamespaceCache for testing
func (s *Test) GetNamespaceCache() cache.NamespaceCache {
	return s.NamespaceCache
}

// GetTimeSource for testing
func (s *Test) GetTimeSource() clock.TimeSource {
	return s.TimeSource
}

// GetPayloadSerializer for testing
func (s *Test) GetPayloadSerializer() persistence.PayloadSerializer {
	return s.PayloadSerializer
}

// GetMetricsClient for testing
func (s *Test) GetMetricsClient() metrics.Client {
	return s.MetricsClient
}

// GetMessagingClient for testing
func (s *Test) GetMessagingClient() messaging.Client {
	panic("user should implement this method for test")
}

// GetArchivalMetadata for testing
func (s *Test) GetArchivalMetadata() archiver.ArchivalMetadata {
	return s.ArchivalMetadata
}

// GetArchiverProvider for testing
func (s *Test) GetArchiverProvider() provider.ArchiverProvider {
	return s.ArchiverProvider
}

// membership infos

// GetMembershipMonitor for testing
func (s *Test) GetMembershipMonitor() membership.Monitor {
	return s.MembershipMonitor
}

// GetFrontendServiceResolver for testing
func (s *Test) GetFrontendServiceResolver() membership.ServiceResolver {
	return s.FrontendServiceResolver
}

// GetMatchingServiceResolver for testing
func (s *Test) GetMatchingServiceResolver() membership.ServiceResolver {
	return s.MatchingServiceResolver
}

// GetHistoryServiceResolver for testing
func (s *Test) GetHistoryServiceResolver() membership.ServiceResolver {
	return s.HistoryServiceResolver
}

// GetWorkerServiceResolver for testing
func (s *Test) GetWorkerServiceResolver() membership.ServiceResolver {
	return s.WorkerServiceResolver
}

// internal services clients

// GetSDKClient for testing
func (s *Test) GetSDKClient() sdkclient.Client {
	return s.SDKClient
}

// GetFrontendRawClient for testing
func (s *Test) GetFrontendRawClient() frontend.Client {
	return s.FrontendClient
}

// GetFrontendClient for testing
func (s *Test) GetFrontendClient() frontend.Client {
	return s.FrontendClient
}

// GetMatchingRawClient for testing
func (s *Test) GetMatchingRawClient() matching.Client {
	return s.MatchingClient
}

// GetMatchingClient for testing
func (s *Test) GetMatchingClient() matching.Client {
	return s.MatchingClient
}

// GetHistoryRawClient for testing
func (s *Test) GetHistoryRawClient() history.Client {
	return s.HistoryClient
}

// GetHistoryClient for testing
func (s *Test) GetHistoryClient() history.Client {
	return s.HistoryClient
}

// GetRemoteAdminClient for testing
func (s *Test) GetRemoteAdminClient(
	cluster string,
) admin.Client {

	return s.RemoteAdminClient
}

// GetRemoteFrontendClient for testing
func (s *Test) GetRemoteFrontendClient(
	cluster string,
) frontend.Client {

	return s.RemoteFrontendClient
}

// GetClientBean for testing
func (s *Test) GetClientBean() client.Bean {
	return s.ClientBean
}

// persistence clients

// GetMetadataManager for testing
func (s *Test) GetMetadataManager() persistence.MetadataManager {
	return s.MetadataMgr
}

// GetTaskManager for testing
func (s *Test) GetTaskManager() persistence.TaskManager {
	return s.TaskMgr
}

// GetVisibilityManager for testing
func (s *Test) GetVisibilityManager() persistence.VisibilityManager {
	return s.VisibilityMgr
}

// GetNamespaceReplicationQueue for testing
func (s *Test) GetNamespaceReplicationQueue() persistence.NamespaceReplicationQueue {
	// user should implement this method for test
	return s.NamespaceReplicationQueue
}

// GetShardManager for testing
func (s *Test) GetShardManager() persistence.ShardManager {
	return s.ShardMgr
}

// GetHistoryManager for testing
func (s *Test) GetHistoryManager() persistence.HistoryManager {
	return s.HistoryMgr
}

// GetExecutionManager for testing
func (s *Test) GetExecutionManager(
	shardID int,
) (persistence.ExecutionManager, error) {

	return s.ExecutionMgr, nil
}

// GetPersistenceBean for testing
func (s *Test) GetPersistenceBean() persistenceClient.Bean {
	return s.PersistenceBean
}

// loggers

// GetLogger for testing
func (s *Test) GetLogger() log.Logger {
	return s.Logger
}

// GetThrottledLogger for testing
func (s *Test) GetThrottledLogger() log.Logger {
	return s.Logger
}

// GetGRPCListener for testing
func (s *Test) GetGRPCListener() net.Listener {
	panic("user should implement this method for test")
}

// Finish checks whether expectations are met
func (s *Test) Finish(
	t mock.TestingT,
) {
	s.ArchivalMetadata.AssertExpectations(t)
	s.ArchiverProvider.AssertExpectations(t)

	s.MetadataMgr.AssertExpectations(t)
	s.TaskMgr.AssertExpectations(t)
	s.VisibilityMgr.AssertExpectations(t)
	s.ShardMgr.AssertExpectations(t)
	s.HistoryMgr.AssertExpectations(t)
	s.ExecutionMgr.AssertExpectations(t)
}
