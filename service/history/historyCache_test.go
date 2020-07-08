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

package history

import (
	"errors"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common/v1"

	"github.com/temporalio/temporal/api/persistenceblobs/v1"
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
	execution1 := commonpb.WorkflowExecution{
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

	execution2 := commonpb.WorkflowExecution{
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
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	context, release, err := s.cache.getOrCreateWorkflowExecutionForBackground(namespaceID, we)
	s.Nil(err)

	we2 := commonpb.WorkflowExecution{
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
	we := commonpb.WorkflowExecution{
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
	we := commonpb.WorkflowExecution{
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
