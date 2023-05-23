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

package cache

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	workflowCacheSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		cache Cache
	}
)

func TestWorkflowCacheSuite(t *testing.T) {
	s := new(workflowCacheSuite)
	suite.Run(t, s)
}

func (s *workflowCacheSuite) SetupSuite() {
}

func (s *workflowCacheSuite) TearDownSuite() {
}

func (s *workflowCacheSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
}

func (s *workflowCacheSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *workflowCacheSuite) TestHistoryCacheBasic() {
	s.cache = NewCache(s.mockShard)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := workflow.NewMockMutableState(s.controller)
	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		execution1,
		workflow.LockPriorityHigh,
	)
	s.Nil(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	release(nil)
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		execution1,
		workflow.LockPriorityHigh,
	)
	s.Nil(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)

	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		execution2,
		workflow.LockPriorityHigh,
	)
	s.Nil(err)
	s.NotEqual(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCachePinning() {
	s.mockShard.GetConfig().HistoryCacheMaxSize = dynamicconfig.GetIntPropertyFn(1)
	namespaceID := namespace.ID("test_namespace_id")
	s.cache = NewCache(s.mockShard)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		we,
		workflow.LockPriorityHigh,
	)
	s.Nil(err)

	we2 := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	// Cache is full because context is pinned, should get an error now
	_, _, err2 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		we2,
		workflow.LockPriorityHigh,
	)
	s.NotNil(err2)

	// Now release the context, this should unpin it.
	release(err2)

	_, release2, err3 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		we2,
		workflow.LockPriorityHigh,
	)
	s.Nil(err3)
	release2(err3)

	// Old context should be evicted.
	newContext, release, err4 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		we,
		workflow.LockPriorityHigh,
	)
	s.Nil(err4)
	s.False(ctx == newContext)
	release(err4)
}

func (s *workflowCacheSuite) TestHistoryCacheClear() {
	s.mockShard.GetConfig().HistoryCacheMaxSize = dynamicconfig.GetIntPropertyFn(20)
	namespaceID := namespace.ID("test_namespace_id")
	s.cache = NewCache(s.mockShard)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-clear",
		RunId:      uuid.New(),
	}

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		we,
		workflow.LockPriorityHigh,
	)
	s.Nil(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	mock := workflow.NewMockMutableState(s.controller)
	ctx.(*workflow.ContextImpl).MutableState = mock

	release(nil)

	// since last time, the release function receive a nil error
	// the ms will not be cleared
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		we,
		workflow.LockPriorityHigh,
	)
	s.Nil(err)

	s.NotNil(ctx.(*workflow.ContextImpl).MutableState)
	mock.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
	release(errors.New("some random error message"))

	// since last time, the release function receive a non-nil error
	// the ms will be cleared
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		we,
		workflow.LockPriorityHigh,
	)
	s.Nil(err)
	s.Nil(ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCacheConcurrentAccess_Release() {
	cacheMaxSize := 16
	coroutineCount := 50

	s.mockShard.GetConfig().HistoryCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheMaxSize)
	s.cache = NewCache(s.mockShard)

	startGroup := &sync.WaitGroup{}
	stopGroup := &sync.WaitGroup{}
	startGroup.Add(coroutineCount)
	stopGroup.Add(coroutineCount)

	namespaceID := namespace.ID("test_namespace_id")
	workflowId := "wf-cache-test-pinning"
	runID := uuid.New()

	testFn := func() {
		defer stopGroup.Done()
		startGroup.Done()

		startGroup.Wait()
		ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
			context.Background(),
			namespaceID,
			commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runID,
			},
			workflow.LockPriorityHigh,
		)
		s.Nil(err)
		// since each time the is reset to nil
		s.Nil(ctx.(*workflow.ContextImpl).MutableState)
		// since we are just testing whether the release function will clear the cache
		// all we need is a fake MutableState
		mock := workflow.NewMockMutableState(s.controller)
		mock.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
		ctx.(*workflow.ContextImpl).MutableState = mock
		release(errors.New("some random error message"))
	}

	for i := 0; i < coroutineCount; i++ {
		go testFn()
	}
	stopGroup.Wait()

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runID,
		},
		workflow.LockPriorityHigh,
	)
	s.Nil(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	s.Nil(ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCacheConcurrentAccess_Pin() {
	cacheMaxSize := 16
	runIDCount := cacheMaxSize * 4
	coroutineCount := runIDCount * 64

	s.mockShard.GetConfig().HistoryCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheMaxSize)
	s.mockShard.GetConfig().HistoryCacheTTL = dynamicconfig.GetDurationPropertyFn(time.Nanosecond)
	s.cache = NewCache(s.mockShard)

	startGroup := &sync.WaitGroup{}
	stopGroup := &sync.WaitGroup{}
	startGroup.Add(coroutineCount)
	stopGroup.Add(coroutineCount)

	namespaceID := namespace.ID("test_namespace_id")
	workflowID := "wf-cache-test-pinning"
	runIDs := make([]string, runIDCount)
	runIDRefCounter := make([]int32, runIDCount)
	for i := 0; i < runIDCount; i++ {
		runIDs[i] = uuid.New()
		runIDRefCounter[i] = 0
	}

	testFn := func(id int, runID string, refCounter *int32) {
		defer stopGroup.Done()
		startGroup.Done()
		startGroup.Wait()

		var releaseFn ReleaseCacheFunc
		var err error
		for {
			_, releaseFn, err = s.cache.GetOrCreateWorkflowExecution(
				context.Background(),
				namespaceID,
				commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				workflow.LockPriorityHigh,
			)
			if err == nil {
				break
			}
		}
		if !atomic.CompareAndSwapInt32(refCounter, 0, 1) {
			s.Fail("unable to assert lock uniqueness")
		}
		// randomly sleep few nanoseconds
		time.Sleep(time.Duration(rand.Int63n(10)))
		if !atomic.CompareAndSwapInt32(refCounter, 1, 0) {
			s.Fail("unable to assert lock uniqueness")
		}
		releaseFn(nil)
	}

	for i := 0; i < coroutineCount; i++ {
		go testFn(i, runIDs[i%runIDCount], &runIDRefCounter[i%runIDCount])
	}
	stopGroup.Wait()
}

func (s *workflowCacheSuite) TestHistoryCache_CacheLatencyMetricContext() {
	s.cache = NewCache(s.mockShard)

	ctx := metrics.AddMetricsContext(context.Background())
	_, currentRelease, err := s.cache.GetOrCreateCurrentWorkflowExecution(
		ctx,
		tests.NamespaceID,
		tests.WorkflowID,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	defer currentRelease(nil)

	latency1, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.GetMetricName())
	s.True(ok)
	s.NotZero(latency1)

	_, release, err := s.cache.GetOrCreateWorkflowExecution(
		ctx,
		tests.NamespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		workflow.LockPriorityHigh,
	)
	s.Nil(err)
	defer release(nil)

	latency2, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.GetMetricName())
	s.True(ok)
	s.Greater(latency2, latency1)

}

func (s *workflowCacheSuite) TestCacheImpl_lockWorkflowExecution() {

	testSets := []struct {
		name             string
		shouldLockBefore bool
		callerType       string
		withTimeout      bool
		wantErr          bool
	}{

		{
			name:       "API context without timeout without locking beforehand should not return an error",
			callerType: headers.CallerTypeAPI,
		},
		{
			name:             "API context without timeout with locking beforehand should not return an error",
			shouldLockBefore: true,
			callerType:       headers.CallerTypeAPI,
			wantErr:          true,
		},

		{
			name:       "API context with timeout without locking beforehand should not return an error",
			callerType: headers.CallerTypeAPI,
		},
		{
			name:             "API context with timeout and locking beforehand should return an error",
			shouldLockBefore: true,
			callerType:       headers.CallerTypeAPI,
			wantErr:          true,
		},
		{
			name:       "Non API context with timeout without locking beforehand should return an error",
			callerType: headers.CallerTypeBackground,
		},
		{
			name:             "Non API context with timeout and locking beforehand should return an error",
			shouldLockBefore: true,
			callerType:       headers.CallerTypeBackground,
			wantErr:          true,
		},
	}
	for _, tt := range testSets {
		s.Run(tt.name, func() {
			c := NewCache(s.mockShard).(*CacheImpl)
			namespaceID := namespace.ID("test_namespace_id")
			execution := commonpb.WorkflowExecution{
				WorkflowId: "some random workflow id",
				RunId:      uuid.New(),
			}
			key := definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId())
			workflowCtx := workflow.NewContext(c.shard, key, c.logger)
			ctx := headers.SetCallerType(context.Background(), tt.callerType)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if tt.shouldLockBefore {
				// lock the workflow to allow it to time out
				err := workflowCtx.Lock(ctx, workflow.LockPriorityHigh)
				s.Nil(err)
			}

			if err := c.lockWorkflowExecution(ctx, workflowCtx, key, workflow.LockPriorityHigh); (err != nil) != tt.wantErr {
				s.T().Errorf("CacheImpl.lockWorkflowExecution() error = %v, wantErr %v", err, tt.wantErr)
			}

		})
	}
}
