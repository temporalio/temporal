package replication

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	taskFetcherSuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		mockResource   *resourcetest.Test
		frontendClient *adminservicemock.MockAdminServiceClient

		config *configs.Config
		logger log.Logger

		replicationTaskFetcher *taskFetcherImpl
	}

	getReplicationMessagesRequestMatcher struct {
		clusterName string
		tokens      map[int32]*replicationspb.ReplicationToken
	}
)

func TestTaskFetcherSuite(t *testing.T) {
	s := new(taskFetcherSuite)
	suite.Run(t, s)
}

func (s *taskFetcherSuite) SetupSuite() {

}

func (s *taskFetcherSuite) TearDownSuite() {

}

func (s *taskFetcherSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockResource = resourcetest.NewTest(s.controller, primitives.HistoryService)
	s.frontendClient = s.mockResource.RemoteAdminClient
	s.logger = log.NewNoopLogger()
	s.config = tests.NewDynamicConfig()
	s.config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(1)

	s.replicationTaskFetcher = newReplicationTaskFetcher(
		s.logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		s.config,
		s.mockResource.ClientBean,
	)
}

func (s *taskFetcherSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *taskFetcherSuite) TestBufferRequests_NoDuplicate() {
	shardID := int32(1)

	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan,
	}

	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest)

	select {
	case <-respChan:
		s.Fail("new request channel should not be closed")
	default:
		// noop
	}

	s.Equal(map[int32]*replicationTaskRequest{
		shardID: shardRequest,
	}, s.replicationTaskFetcher.workers[0].requestByShard)
}

func (s *taskFetcherSuite) TestBufferRequests_Duplicate() {
	shardID := int32(1)

	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}

	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}

	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest1)
	s.replicationTaskFetcher.workers[0].bufferRequests(shardRequest2)

	_, ok := <-respChan1
	s.False(ok)

	select {
	case <-respChan2:
		s.Fail("new request channel should not be closed")
	default:
		// noop
	}

	s.Equal(map[int32]*replicationTaskRequest{
		shardID: shardRequest2,
	}, s.replicationTaskFetcher.workers[0].requestByShard)
}

func (s *taskFetcherSuite) TestGetMessages_All() {
	shardID := int32(1)
	respChan := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID: shardRequest,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	responseByShard := map[int32]*replicationspb.ReplicationMessages{
		shardID: {},
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(&adminservice.GetReplicationMessagesResponse{ShardMessages: responseByShard}, nil)
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	s.NoError(err)
	s.Equal(responseByShard[shardID], <-respChan)
}

func (s *taskFetcherSuite) TestGetMessages_Partial() {
	shardID1 := int32(1)
	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID1,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}
	shardID2 := int32(2)
	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID2,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID1: shardRequest1,
		shardID2: shardRequest2,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest1.token,
			shardRequest2.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	responseByShard := map[int32]*replicationspb.ReplicationMessages{
		shardID1: {},
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(&adminservice.GetReplicationMessagesResponse{ShardMessages: responseByShard}, nil)
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	s.NoError(err)
	s.Equal(responseByShard[shardID1], <-respChan1)
	s.Equal((*replicationspb.ReplicationMessages)(nil), <-respChan2)
}

func (s *taskFetcherSuite) TestGetMessages_Error() {
	shardID1 := int32(1)
	respChan1 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest1 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID1,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan1,
	}
	shardID2 := int32(2)
	respChan2 := make(chan *replicationspb.ReplicationMessages, 1)
	shardRequest2 := &replicationTaskRequest{
		token: &replicationspb.ReplicationToken{
			ShardId:                shardID2,
			LastProcessedMessageId: 1,
			LastRetrievedMessageId: 2,
		},
		respChan: respChan2,
	}
	requestByShard := map[int32]*replicationTaskRequest{
		shardID1: shardRequest1,
		shardID2: shardRequest2,
	}

	replicationMessageRequest := &adminservice.GetReplicationMessagesRequest{
		Tokens: []*replicationspb.ReplicationToken{
			shardRequest1.token,
			shardRequest2.token,
		},
		ClusterName: cluster.TestCurrentClusterName,
	}
	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		newGetReplicationMessagesRequestMatcher(replicationMessageRequest),
	).Return(nil, errors.New("random error"))
	s.replicationTaskFetcher.workers[0].requestByShard = requestByShard
	err := s.replicationTaskFetcher.workers[0].getMessages()
	s.Error(err)
	s.Equal((*replicationspb.ReplicationMessages)(nil), <-respChan1)
	s.Equal((*replicationspb.ReplicationMessages)(nil), <-respChan2)
}

func (s *taskFetcherSuite) TestConcurrentFetchAndProcess_Success() {
	numShards := 1024

	s.config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(8)

	s.replicationTaskFetcher = newReplicationTaskFetcher(
		s.logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		s.config,
		s.mockResource.ClientBean,
	)

	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		gomock.Any(),
	).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	s.replicationTaskFetcher.Start()
	defer s.replicationTaskFetcher.Stop()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numShards)
	for i := range numShards {
		shardID := int32(i)
		go func() {
			defer waitGroup.Done()
			respChan := make(chan *replicationspb.ReplicationMessages, 1)
			shardRequest := &replicationTaskRequest{
				token: &replicationspb.ReplicationToken{
					ShardId:                shardID,
					LastProcessedMessageId: 1,
					LastRetrievedMessageId: 2,
				},
				respChan: respChan,
			}

			s.replicationTaskFetcher.getRequestChan() <- shardRequest
			<-respChan
		}()
	}
	waitGroup.Wait()
}

func (s *taskFetcherSuite) TestConcurrentFetchAndProcess_Error() {
	numShards := 1024

	s.config.ReplicationTaskFetcherParallelism = dynamicconfig.GetIntPropertyFn(8)

	s.replicationTaskFetcher = newReplicationTaskFetcher(
		s.logger,
		cluster.TestAlternativeClusterName,
		cluster.TestCurrentClusterName,
		s.config,
		s.mockResource.ClientBean,
	)

	s.frontendClient.EXPECT().GetReplicationMessages(
		gomock.Any(),
		gomock.Any(),
	).Return(nil, errors.New("random error")).AnyTimes()

	s.replicationTaskFetcher.Start()
	defer s.replicationTaskFetcher.Stop()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numShards)
	for i := range numShards {
		shardID := int32(i)
		go func() {
			defer waitGroup.Done()
			respChan := make(chan *replicationspb.ReplicationMessages, 1)
			shardRequest := &replicationTaskRequest{
				token: &replicationspb.ReplicationToken{
					ShardId:                shardID,
					LastProcessedMessageId: 1,
					LastRetrievedMessageId: 2,
				},
				respChan: respChan,
			}

			s.replicationTaskFetcher.getRequestChan() <- shardRequest
			<-respChan
		}()
	}
	waitGroup.Wait()
}

func (s *taskFetcherSuite) TestGetSourceClusterAndRateLimiter() {
	s.Equal(cluster.TestAlternativeClusterName, s.replicationTaskFetcher.getSourceCluster())
	s.NotNil(s.replicationTaskFetcher.getRateLimiter())
	s.Equal(s.replicationTaskFetcher.rateLimiter, s.replicationTaskFetcher.getRateLimiter())
}

func (s *taskFetcherSuite) TestFetcher_StartStop_Idempotent() {
	s.replicationTaskFetcher.Start()
	// Second Start is a no-op (status already started).
	s.replicationTaskFetcher.Start()

	s.replicationTaskFetcher.Stop()
	// Second Stop is a no-op (status already stopped).
	s.replicationTaskFetcher.Stop()
}

func (s *taskFetcherSuite) TestFetcherWorker_StartStop_Idempotent() {
	worker := s.replicationTaskFetcher.workers[0]
	worker.Start()
	worker.Start() // no-op
	worker.Stop()
	worker.Stop() // no-op
}

func (s *taskFetcherSuite) taskFetchNewFactory() *taskFetcherFactoryImpl {
	return NewTaskFetcherFactory(
		s.logger,
		s.config,
		s.mockResource.ClusterMetadata,
		s.mockResource.ClientBean,
	).(*taskFetcherFactoryImpl)
}

func (s *taskFetcherSuite) TestFactory_StartStop_Idempotent() {
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockResource.ClusterMetadata.EXPECT().RegisterMetadataChangeCallback(gomock.Any(), gomock.Any()).Times(1)
	s.mockResource.ClusterMetadata.EXPECT().UnRegisterMetadataChangeCallback(gomock.Any()).Times(1)
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	factory := s.taskFetchNewFactory()
	factory.Start()
	factory.Start() // no-op
	factory.Stop()
	factory.Stop() // no-op
}

func (s *taskFetcherSuite) TestFactory_GetOrCreateFetcher() {
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	factory := s.taskFetchNewFactory()
	defer func() {
		factory.fetchersLock.Lock()
		defer factory.fetchersLock.Unlock()
		for _, f := range factory.fetchers {
			f.Stop()
		}
	}()

	fetcher := factory.GetOrCreateFetcher(cluster.TestAlternativeClusterName)
	s.NotNil(fetcher)
	s.Equal(cluster.TestAlternativeClusterName, fetcher.getSourceCluster())

	// Second call returns the cached fetcher (no new fetcher created).
	fetcher2 := factory.GetOrCreateFetcher(cluster.TestAlternativeClusterName)
	s.Equal(fetcher, fetcher2)
	s.Len(factory.fetchers, 1)
}

func (s *taskFetcherSuite) TestFactory_ListenClusterMetadataChange_RemovesDisabledFetcher() {
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	var taskFetchCallback cluster.CallbackFn
	s.mockResource.ClusterMetadata.EXPECT().RegisterMetadataChangeCallback(gomock.Any(), gomock.Any()).
		Do(func(_ any, cb cluster.CallbackFn) {
			taskFetchCallback = cb
		}).Times(1)

	factory := s.taskFetchNewFactory()
	factory.Start()
	defer func() {
		s.mockResource.ClusterMetadata.EXPECT().UnRegisterMetadataChangeCallback(gomock.Any()).Times(1)
		factory.Stop()
	}()

	// Create a fetcher to remove later.
	fetcher := factory.GetOrCreateFetcher(cluster.TestAlternativeClusterName)
	s.NotNil(fetcher)
	s.Len(factory.fetchers, 1)

	s.NotNil(taskFetchCallback)

	// Callback with current cluster: skipped (continue branch).
	// Callback with disabled remote cluster: fetcher removed.
	// Callback with an unknown cluster: ignored (not in fetchers map).
	taskFetchCallback(
		map[string]*cluster.ClusterInformation{},
		map[string]*cluster.ClusterInformation{
			cluster.TestCurrentClusterName:     {Enabled: true},
			cluster.TestAlternativeClusterName: {Enabled: false},
			"some-unknown-cluster":             {Enabled: true},
		},
	)

	factory.fetchersLock.Lock()
	_, ok := factory.fetchers[cluster.TestAlternativeClusterName]
	factory.fetchersLock.Unlock()
	s.False(ok)
}

func (s *taskFetcherSuite) TestFactory_ListenClusterMetadataChange_NilClusterInfoRemovesFetcher() {
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	var taskFetchCallback cluster.CallbackFn
	s.mockResource.ClusterMetadata.EXPECT().RegisterMetadataChangeCallback(gomock.Any(), gomock.Any()).
		Do(func(_ any, cb cluster.CallbackFn) {
			taskFetchCallback = cb
		}).Times(1)

	factory := s.taskFetchNewFactory()
	factory.Start()
	defer func() {
		s.mockResource.ClusterMetadata.EXPECT().UnRegisterMetadataChangeCallback(gomock.Any()).Times(1)
		factory.Stop()
	}()

	fetcher := factory.GetOrCreateFetcher(cluster.TestAlternativeClusterName)
	s.NotNil(fetcher)
	s.Len(factory.fetchers, 1)
	s.NotNil(taskFetchCallback)

	// nil cluster info also removes the fetcher.
	taskFetchCallback(
		map[string]*cluster.ClusterInformation{},
		map[string]*cluster.ClusterInformation{
			cluster.TestAlternativeClusterName: nil,
		},
	)

	factory.fetchersLock.Lock()
	_, ok := factory.fetchers[cluster.TestAlternativeClusterName]
	factory.fetchersLock.Unlock()
	s.False(ok)
}

func (s *taskFetcherSuite) TestFactory_ListenClusterMetadataChange_StillEnabledKeepsFetcher() {
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.frontendClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(&adminservice.GetReplicationMessagesResponse{}, nil).AnyTimes()

	var taskFetchCallback cluster.CallbackFn
	s.mockResource.ClusterMetadata.EXPECT().RegisterMetadataChangeCallback(gomock.Any(), gomock.Any()).
		Do(func(_ any, cb cluster.CallbackFn) {
			taskFetchCallback = cb
		}).Times(1)

	factory := s.taskFetchNewFactory()
	factory.Start()
	defer func() {
		s.mockResource.ClusterMetadata.EXPECT().UnRegisterMetadataChangeCallback(gomock.Any()).Times(1)
		factory.Stop()
	}()

	fetcher := factory.GetOrCreateFetcher(cluster.TestAlternativeClusterName)
	s.NotNil(fetcher)
	s.NotNil(taskFetchCallback)

	// Remote cluster still enabled -> fetcher kept.
	taskFetchCallback(
		map[string]*cluster.ClusterInformation{},
		map[string]*cluster.ClusterInformation{
			cluster.TestAlternativeClusterName: {Enabled: true},
		},
	)

	factory.fetchersLock.Lock()
	_, ok := factory.fetchers[cluster.TestAlternativeClusterName]
	factory.fetchersLock.Unlock()
	s.True(ok)
}

func newGetReplicationMessagesRequestMatcher(
	req *adminservice.GetReplicationMessagesRequest,
) *getReplicationMessagesRequestMatcher {
	tokens := make(map[int32]*replicationspb.ReplicationToken)
	for _, token := range req.Tokens {
		tokens[token.ShardId] = token
	}
	return &getReplicationMessagesRequestMatcher{
		clusterName: req.ClusterName,
		tokens:      tokens,
	}
}

func (m *getReplicationMessagesRequestMatcher) Matches(x any) bool {
	req, ok := x.(*adminservice.GetReplicationMessagesRequest)
	if !ok {
		return false
	}
	return reflect.DeepEqual(m, newGetReplicationMessagesRequestMatcher(req))
}

func (m *getReplicationMessagesRequestMatcher) String() string {
	// noop, not used
	return ""
}
