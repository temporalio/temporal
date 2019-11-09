// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	publicservicetest "go.uber.org/cadence/.gen/go/cadence/workflowservicetest"

	"github.com/uber/cadence/.gen/go/cadence/workflowservicetest"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/matching/matchingservicetest"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/client/admin"
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	persistenceClient "github.com/uber/cadence/common/persistence/client"

	"go.uber.org/yarpc"
	"go.uber.org/zap"
)

type (
	// Test is the test implementation used for testing
	Test struct {
		MetricsScope    tally.Scope
		ClusterMetadata *cluster.MockMetadata

		// other common resources

		DomainCache       *cache.MockDomainCache
		TimeSource        clock.TimeSource
		PayloadSerializer persistence.PayloadSerializer
		MetricsClient     metrics.Client
		ArchivalMetadata  *archiver.MockArchivalMetadata
		ArchiverProvider  *provider.MockArchiverProvider

		// membership infos

		MembershipMonitor       membership.Monitor
		FrontendServiceResolver membership.ServiceResolver
		MatchingServiceResolver membership.ServiceResolver
		HistoryServiceResolver  membership.ServiceResolver
		WorkerServiceResolver   membership.ServiceResolver

		// internal services clients

		PublicClient   *publicservicetest.MockClient
		FrontendClient *workflowservicetest.MockClient
		MatchingClient *matchingservicetest.MockClient
		HistoryClient  *historyservicetest.MockClient
		ClientBean     *client.MockBean

		// persistence clients

		MetadataMgr            *mocks.MetadataManager
		TaskMgr                *mocks.TaskManager
		VisibilityMgr          *mocks.VisibilityManager
		DomainReplicationQueue persistence.DomainReplicationQueue
		ShardMgr               *mocks.ShardManager
		HistoryMgr             *mocks.HistoryV2Manager
		ExecutionMgr           *mocks.ExecutionManager
		PersistenceBean        *persistenceClient.MockBean

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

	frontendClient := workflowservicetest.NewMockClient(controller)
	matchingClient := matchingservicetest.NewMockClient(controller)
	historyClient := historyservicetest.NewMockClient(controller)
	clientBean := client.NewMockBean(controller)
	clientBean.EXPECT().GetFrontendClient().Return(frontendClient).AnyTimes()
	clientBean.EXPECT().GetMatchingClient(gomock.Any()).Return(matchingClient, nil).AnyTimes()
	clientBean.EXPECT().GetHistoryClient().Return(historyClient).AnyTimes()

	metadataMgr := &mocks.MetadataManager{}
	taskMgr := &mocks.TaskManager{}
	visibilityMgr := &mocks.VisibilityManager{}
	shardMgr := &mocks.ShardManager{}
	historyMgr := &mocks.HistoryV2Manager{}
	executionMgr := &mocks.ExecutionManager{}
	persistenceBean := persistenceClient.NewMockBean(controller)
	persistenceBean.EXPECT().GetMetadataManager().Return(metadataMgr).AnyTimes()
	persistenceBean.EXPECT().GetTaskManager().Return(taskMgr).AnyTimes()
	persistenceBean.EXPECT().GetVisibilityManager().Return(visibilityMgr).AnyTimes()
	persistenceBean.EXPECT().GetHistoryManager().Return(historyMgr).AnyTimes()
	persistenceBean.EXPECT().GetShardManager().Return(shardMgr).AnyTimes()
	persistenceBean.EXPECT().GetExecutionManager(gomock.Any()).Return(executionMgr, nil).AnyTimes()

	return &Test{
		MetricsScope:    tally.NoopScope,
		ClusterMetadata: cluster.NewMockMetadata(controller),

		// other common resources

		DomainCache:       cache.NewMockDomainCache(controller),
		TimeSource:        clock.NewRealTimeSource(),
		PayloadSerializer: persistence.NewPayloadSerializer(),
		MetricsClient:     metrics.NewClient(tally.NoopScope, serviceMetricsIndex),
		ArchivalMetadata:  &archiver.MockArchivalMetadata{},
		ArchiverProvider:  &provider.MockArchiverProvider{},

		// membership infos

		MembershipMonitor:       nil,
		FrontendServiceResolver: nil,
		MatchingServiceResolver: nil,
		HistoryServiceResolver:  nil,
		WorkerServiceResolver:   nil,

		// internal services clients

		PublicClient:   publicservicetest.NewMockClient(controller),
		FrontendClient: frontendClient,
		MatchingClient: matchingClient,
		HistoryClient:  historyClient,
		ClientBean:     clientBean,

		// persistence clients

		MetadataMgr:            metadataMgr,
		TaskMgr:                taskMgr,
		VisibilityMgr:          visibilityMgr,
		DomainReplicationQueue: nil,
		ShardMgr:               shardMgr,
		HistoryMgr:             historyMgr,
		ExecutionMgr:           executionMgr,
		PersistenceBean:        persistenceBean,

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
func (s *Test) GetHostInfo() (*membership.HostInfo, error) {
	return testHostInfo, nil
}

// GetClusterMetadata for testing
func (s *Test) GetClusterMetadata() cluster.Metadata {
	return s.ClusterMetadata
}

// other common resources

// GetDomainCache for testing
func (s *Test) GetDomainCache() cache.DomainCache {
	return s.DomainCache
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
	panic("user should implement this method for test")
}

// GetFrontendServiceResolver for testing
func (s *Test) GetFrontendServiceResolver() membership.ServiceResolver {
	panic("user should implement this method for test")
}

// GetMatchingServiceResolver for testing
func (s *Test) GetMatchingServiceResolver() membership.ServiceResolver {
	panic("user should implement this method for test")
}

// GetHistoryServiceResolver for testing
func (s *Test) GetHistoryServiceResolver() membership.ServiceResolver {
	panic("user should implement this method for test")
}

// GetWorkerServiceResolver for testing
func (s *Test) GetWorkerServiceResolver() membership.ServiceResolver {
	panic("user should implement this method for test")
}

// internal services clients

// GetPublicClient for testing
func (s *Test) GetPublicClient() workflowserviceclient.Interface {
	return s.PublicClient
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
	return s.ClientBean.GetHistoryClient()
}

// GetHistoryClient for testing
func (s *Test) GetHistoryClient() history.Client {
	return s.ClientBean.GetHistoryClient()
}

// GetRemoteAdminClient for testing
func (s *Test) GetRemoteAdminClient(
	cluster string,
) admin.Client {

	return s.ClientBean.GetRemoteAdminClient(cluster)
}

// GetRemoteFrontendClient for testing
func (s *Test) GetRemoteFrontendClient(
	cluster string,
) frontend.Client {

	return s.ClientBean.GetRemoteFrontendClient(cluster)
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

// GetDomainReplicationQueue for testing
func (s *Test) GetDomainReplicationQueue() persistence.DomainReplicationQueue {
	// user should implement this method for test
	return nil
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

// GetDispatcher for testing
func (s *Test) GetDispatcher() *yarpc.Dispatcher {
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
