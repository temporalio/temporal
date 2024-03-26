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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/persistence"

	enumsspb "go.temporal.io/server/api/enums/v1"
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
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := workflow.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	release(nil)
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)

	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution2,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.NotEqual(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCachePanic() {
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := workflow.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(true).AnyTimes()
	mockMS1.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry()).AnyTimes()
	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1

	defer func() {
		if recover() != nil {
			ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
				context.Background(),
				s.mockShard,
				namespaceID,
				&execution1,
				workflow.LockPriorityHigh,
			)
			s.NoError(err)
			s.Nil(ctx.(*workflow.ContextImpl).MutableState)
			release(nil)
		} else {
			s.Fail("test should panic")
		}
	}()
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCachePinning() {
	s.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(1)
	namespaceID := namespace.ID("test_namespace_id")
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)

	we2 := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.New(),
	}

	// Cache is full because context is pinned, should get an error now
	_, _, err2 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we2,
		workflow.LockPriorityHigh,
	)
	s.Error(err2)

	// Now release the context, this should unpin it.
	release(err2)

	_, release2, err3 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we2,
		workflow.LockPriorityHigh,
	)
	s.NoError(err3)
	release2(err3)

	// Old context should be evicted.
	newContext, release, err4 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		workflow.LockPriorityHigh,
	)
	s.NoError(err4)
	s.False(ctx == newContext)
	release(err4)
}

func (s *workflowCacheSuite) TestHistoryCacheClear() {
	s.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(20)
	namespaceID := namespace.ID("test_namespace_id")
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-clear",
		RunId:      uuid.New(),
	}

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	mock := workflow.NewMockMutableState(s.controller)
	mock.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx.(*workflow.ContextImpl).MutableState = mock

	release(nil)

	// since last time, the release function receive a nil error
	// the ms will not be cleared
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)

	s.NotNil(ctx.(*workflow.ContextImpl).MutableState)
	mock.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
	release(errors.New("some random error message"))

	// since last time, the release function receive a non-nil error
	// the ms will be cleared
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Nil(ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCacheConcurrentAccess_Release() {
	cacheMaxSize := 16
	coroutineCount := 50

	s.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheMaxSize)
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

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
			s.mockShard,
			namespaceID,
			&commonpb.WorkflowExecution{
				WorkflowId: workflowId,
				RunId:      runID,
			},
			workflow.LockPriorityHigh,
		)
		s.NoError(err)
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
		s.mockShard,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runID,
		},
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	s.Nil(ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCacheConcurrentAccess_Pin() {
	cacheMaxSize := 16
	runIDCount := cacheMaxSize * 4
	coroutineCount := runIDCount * 64

	s.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheMaxSize)
	s.mockShard.GetConfig().HistoryCacheTTL = dynamicconfig.GetDurationPropertyFn(time.Nanosecond)
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

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
				s.mockShard,
				namespaceID,
				&commonpb.WorkflowExecution{
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
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

	ctx := metrics.AddMetricsContext(context.Background())
	currentRelease, err := s.cache.GetOrCreateCurrentWorkflowExecution(
		ctx,
		s.mockShard,
		tests.NamespaceID,
		tests.WorkflowID,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	defer currentRelease(nil)

	latency1, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name())
	s.True(ok)
	s.NotZero(latency1)

	_, release, err := s.cache.GetOrCreateWorkflowExecution(
		ctx,
		s.mockShard,
		tests.NamespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	defer release(nil)

	latency2, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name())
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
			c := NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler).(*CacheImpl)
			namespaceID := namespace.ID("test_namespace_id")
			execution := commonpb.WorkflowExecution{
				WorkflowId: "some random workflow id",
				RunId:      uuid.New(),
			}
			cacheKey := Key{
				WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
				ShardUUID:   uuid.New(),
			}
			workflowCtx := workflow.NewContext(s.mockShard.GetConfig(), cacheKey.WorkflowKey, s.mockShard.GetLogger(), s.mockShard.GetThrottledLogger(), s.mockShard.GetMetricsHandler())
			ctx := headers.SetCallerType(context.Background(), tt.callerType)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if tt.shouldLockBefore {
				// lock the workflow to allow it to time out
				err := workflowCtx.Lock(ctx, workflow.LockPriorityHigh)
				s.NoError(err)
			}

			if err := c.lockWorkflowExecution(ctx, workflowCtx, cacheKey, workflow.LockPriorityHigh); (err != nil) != tt.wantErr {
				s.T().Errorf("CacheImpl.lockWorkflowExecution() error = %v, wantErr %v", err, tt.wantErr)
			}

		})
	}
}

func (s *workflowCacheSuite) TestCacheImpl_RejectsRequestWhenAtLimitSimple() {
	config := tests.NewDynamicConfig()
	config.HistoryCacheLimitSizeBased = true
	config.HistoryHostLevelCacheMaxSizeBytes = dynamicconfig.GetIntPropertyFn(1000)
	s.cache = NewHostLevelCache(config, metrics.NoopMetricsHandler)
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := workflow.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release1, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	// MockMS1 should fill the entire cache. The total size of the context object will be the size of MutableState
	// plus the size of commonpb.WorkflowExecution in this case. Even though we are returning a size 900 from
	// MutableState, the size of workflow.Context object in the cache will be slightly higher (~972bytes).
	mockMS1.EXPECT().GetApproximatePersistedSize().Return(900).Times(1)
	release1(nil)
	ctx, _, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)

	// Try to insert another entry before releasing previous.
	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	_, _, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		workflow.LockPriorityHigh,
	)
	s.Error(err)
	s.ErrorIs(err, cache.ErrCacheFull)
}

func (s *workflowCacheSuite) TestCacheImpl_RejectsRequestWhenAtLimitMultiple() {
	// This test does the following;
	//   1. Try inserting 3 entries of size 400bytes. Last insert should fail as max size is 1000 bytes.
	//   2. Make the size of second entry 1000 and release it. This should make the cache size > max limit.
	//      Cache should evict this entry to maintain its size under limit.
	//   3. Insert another entry of size 400 bytes successfully.
	config := tests.NewDynamicConfig()
	config.HistoryCacheLimitSizeBased = true
	config.HistoryHostLevelCacheMaxSizeBytes = dynamicconfig.GetIntPropertyFn(1000)
	s.cache = NewHostLevelCache(config, metrics.NoopMetricsHandler)
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := workflow.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()

	ctx, release1, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1

	// Make mockMS1's size 400.
	mockMS1.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release1(nil)
	ctx, release1, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)

	// Insert another 400byte entry.
	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS2 := workflow.NewMockMutableState(s.controller)
	mockMS2.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release2, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS2
	mockMS2.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release2(nil)
	ctx, release2, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS2, ctx.(*workflow.ContextImpl).MutableState)

	// Insert another entry. This should fail as cache has ~800bytes pinned.
	execution3 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS3 := workflow.NewMockMutableState(s.controller)
	mockMS3.EXPECT().IsDirty().Return(false).AnyTimes()
	_, _, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution3,
		workflow.LockPriorityHigh,
	)
	s.Error(err)
	s.ErrorIs(err, cache.ErrCacheFull)

	// Now there are two entries pinned in the cache. Their total size is 800bytes.
	// Make mockMS1 grow to 1000 bytes. Cache should be able to handle this. Now the cache size will be more than its
	// limit. Cache will evict this entry and make more space.
	mockMS1.EXPECT().GetApproximatePersistedSize().Return(1000).Times(1)
	release1(nil)
	ctx, release1, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	// Make sure execution 3 was evicted by checking if mutable state is nil.
	s.Nil(ctx.(*workflow.ContextImpl).MutableState, nil)
	release1(nil)

	// Insert execution3 again with size 400bytes.
	ctx, release3, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution3,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS3

	mockMS3.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release3(nil)
	ctx, release3, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution3,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS3, ctx.(*workflow.ContextImpl).MutableState)

	// Release all remaining entries.
	mockMS2.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release2(nil)
	mockMS3.EXPECT().GetApproximatePersistedSize().Return(400).Times(1)
	release3(nil)
}

func (s *workflowCacheSuite) TestCacheImpl_CheckCacheLimitSizeBasedFlag() {
	config := tests.NewDynamicConfig()
	// HistoryCacheLimitSizeBased is set to false. Cache limit should be based on entry count.
	config.HistoryCacheLimitSizeBased = false
	config.HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(1)
	s.cache = NewHostLevelCache(config, metrics.NoopMetricsHandler)
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.New(),
	}
	mockMS1 := workflow.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release1, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	// GetApproximatePersistedSize() should not be called, since we disabled HistoryHostLevelCacheMaxSize flag.
	mockMS1.EXPECT().GetApproximatePersistedSize().Times(0)
	release1(nil)
	ctx, release1, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release1(nil)
}

func (s *workflowCacheSuite) TestCacheImpl_GetCurrentRunID_CurrentRunExists() {
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      "",
	}

	currentRunID := uuid.New()

	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
	}).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.New(),
		RunID:          currentRunID,
		State:          enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil).Times(1)

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution,
		workflow.LockPriorityHigh,
	)
	s.NoError(err)

	s.Equal(currentRunID, ctx.GetWorkflowKey().RunID)
	release(nil)
}

func (s *workflowCacheSuite) TestCacheImpl_GetCurrentRunID_NoCurrentRun() {
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      "",
	}

	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
	}).Return(nil, serviceerror.NewNotFound("current worflow not found")).Times(1)
	mockShardManager := s.mockShard.Resource.ShardMgr
	mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: s.mockShard.GetShardID(),
		RangeID: s.mockShard.GetRangeID(),
	}).Return(nil).Times(1)

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution,
		workflow.LockPriorityHigh,
	)
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
	s.Nil(ctx)
	s.Nil(release)
}
