// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package resource

import (
	"net"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	"go.temporal.io/api/workflowservicemock/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkmocks "go.temporal.io/sdk/mocks"
	"go.uber.org/zap"

	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
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
		ClusterMetadataMgr        *mocks.MockClusterMetadataManager
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
	clusterMetadataManager := mocks.NewMockClusterMetadataManager(controller)
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
	persistenceBean.EXPECT().GetClusterMetadataManager().Return(clusterMetadataManager).AnyTimes()

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
		ClusterMetadataMgr:        clusterMetadataManager,
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

// GetClusterMetadata for testing
func (s *Test) GetClusterMetadataManager() persistence.ClusterMetadataManager {
	return s.ClusterMetadataMgr
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
	shardID int32,
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
