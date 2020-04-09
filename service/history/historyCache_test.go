package history

import (
	"errors"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	executionpb "go.temporal.io/temporal-proto/execution"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	historyCacheSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shardContextTest

		cache *historyCache
	}
)

func TestHistoryCacheSuite(t *testing.T) {
	s := new(historyCacheSuite)
	suite.Run(t, s)
}

func (s *historyCacheSuite) SetupSuite() {
}

func (s *historyCacheSuite) TearDownSuite() {
}

func (s *historyCacheSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockShard.resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
}

func (s *historyCacheSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *historyCacheSuite) TestHistoryCacheBasic() {
	s.cache = newHistoryCache(s.mockShard)

	namespaceID := "test_namespace_id"
	execution1 := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := NewMockmutableState(s.controller)
	context, release, err := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, execution1)
	s.Nil(err)
	context.(*workflowExecutionContextImpl).mutableState = mockMS1
	release(nil)
	context, release, err = s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, execution1)
	s.Nil(err)
	s.Equal(mockMS1, context.(*workflowExecutionContextImpl).mutableState)
	release(nil)

	execution2 := executionpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	context, release, err = s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, execution2)
	s.Nil(err)
	s.NotEqual(mockMS1, context.(*workflowExecutionContextImpl).mutableState)
	release(nil)
}

func (s *historyCacheSuite) TestHistoryCachePinning() {
	s.mockShard.GetConfig().HistoryCacheMaxSize = dynamicconfig.GetIntPropertyFn(2)
	namespaceID := "test_namespace_id"
	s.cache = newHistoryCache(s.mockShard)
	we := executionpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	context, release, err := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
	s.Nil(err)

	we2 := executionpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	// Cache is full because context is pinned, should get an error now
	_, _, err2 := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we2)
	s.NotNil(err2)

	// Now release the context, this should unpin it.
	release(err2)

	_, release2, err3 := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we2)
	s.Nil(err3)
	release2(err3)

	// Old context should be evicted.
	newContext, release, err4 := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
	s.Nil(err4)
	s.False(context == newContext)
	release(err4)
}

func (s *historyCacheSuite) TestHistoryCacheClear() {
	s.mockShard.GetConfig().HistoryCacheMaxSize = dynamicconfig.GetIntPropertyFn(20)
	namespaceID := "test_namespace_id"
	s.cache = newHistoryCache(s.mockShard)
	we := executionpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-clear",
		RunId:      uuid.New(),
	}

	context, release, err := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
	s.Nil(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake msBuilder
	context.(*workflowExecutionContextImpl).mutableState = &mutableStateBuilder{}
	release(nil)

	// since last time, the release function receive a nil error
	// the ms builder will not be cleared
	context, release, err = s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
	s.Nil(err)
	s.NotNil(context.(*workflowExecutionContextImpl).mutableState)
	release(errors.New("some random error message"))

	// since last time, the release function receive a non-nil error
	// the ms builder will be cleared
	context, release, err = s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
	s.Nil(err)
	s.Nil(context.(*workflowExecutionContextImpl).mutableState)
	release(nil)
}

func (s *historyCacheSuite) TestHistoryCacheConcurrentAccess() {
	s.mockShard.GetConfig().HistoryCacheMaxSize = dynamicconfig.GetIntPropertyFn(20)
	namespaceID := "test_namespace_id"
	s.cache = newHistoryCache(s.mockShard)
	we := executionpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	coroutineCount := 50
	waitGroup := &sync.WaitGroup{}
	stopChan := make(chan struct{})
	testFn := func() {
		<-stopChan
		context, release, err := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
		s.Nil(err)
		// since each time the builder is reset to nil
		s.Nil(context.(*workflowExecutionContextImpl).mutableState)
		// since we are just testing whether the release function will clear the cache
		// all we need is a fake msBuilder
		context.(*workflowExecutionContextImpl).mutableState = &mutableStateBuilder{}
		release(errors.New("some random error message"))
		waitGroup.Done()
	}

	for i := 0; i < coroutineCount; i++ {
		waitGroup.Add(1)
		go testFn()
	}
	close(stopChan)
	waitGroup.Wait()

	context, release, err := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
	s.Nil(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake msBuilder
	s.Nil(context.(*workflowExecutionContextImpl).mutableState)
	release(nil)
}
