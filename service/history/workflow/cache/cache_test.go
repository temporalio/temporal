package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
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
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mockMS1 := historyi.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	s.NoError(err)
	ctx.(*workflow.ContextImpl).MutableState = mockMS1
	release(nil)
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)

	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution2,
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.NotEqual(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCachePanic() {
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mockMS1 := historyi.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(true).AnyTimes()
	mockMS1.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry()).AnyTimes()
	mockMS1.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask().AnyTimes()
	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
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
				locks.PriorityHigh,
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
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.NewString(),
	}

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	s.NoError(err)

	we2 := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-pinning",
		RunId:      uuid.NewString(),
	}

	// Cache is full because context is pinned, should get an error now
	_, _, err2 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we2,
		locks.PriorityHigh,
	)
	s.Error(err2)

	// Now release the context, this should unpin it.
	release(err2)

	_, release2, err3 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we2,
		locks.PriorityHigh,
	)
	s.NoError(err3)
	release2(err3)

	// Old context should be evicted.
	newContext, release, err4 := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	s.NoError(err4)
	s.False(ctx == newContext)
	release(err4)
}

func (s *workflowCacheSuite) TestHistoryCacheClear() {
	s.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(20)
	namespaceID := namespace.ID("test_namespace_id")
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	we := commonpb.WorkflowExecution{
		WorkflowId: "wf-cache-test-clear",
		RunId:      uuid.NewString(),
	}

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
	)
	s.NoError(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	mock := historyi.NewMockMutableState(s.controller)
	mock.EXPECT().IsDirty().Return(false).AnyTimes()
	mock.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask().AnyTimes()
	ctx.(*workflow.ContextImpl).MutableState = mock

	release(nil)

	// since last time, the release function receive a nil error
	// the ms will not be cleared
	ctx, release, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&we,
		locks.PriorityHigh,
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
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Nil(ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

func (s *workflowCacheSuite) TestHistoryCacheConcurrentAccess_Release() {
	cacheMaxSize := 16
	coroutineCount := 50

	s.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheMaxSize)
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	startGroup := &sync.WaitGroup{}
	stopGroup := &sync.WaitGroup{}
	startGroup.Add(coroutineCount)
	stopGroup.Add(coroutineCount)

	namespaceID := namespace.ID("test_namespace_id")
	workflowId := "wf-cache-test-pinning"
	runID := uuid.NewString()

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
			locks.PriorityHigh,
		)
		s.NoError(err)
		// since each time the is reset to nil
		s.Nil(ctx.(*workflow.ContextImpl).MutableState)
		// since we are just testing whether the release function will clear the cache
		// all we need is a fake MutableState
		mock := historyi.NewMockMutableState(s.controller)
		mock.EXPECT().GetQueryRegistry().Return(workflow.NewQueryRegistry())
		mock.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask()
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
		locks.PriorityHigh,
	)
	s.NoError(err)
	// since we are just testing whether the release function will clear the cache
	// all we need is a fake MutableState
	s.Nil(ctx.(*workflow.ContextImpl).MutableState)
	release(nil)
}

/*
this test not just failing, it also stuck the test suite (at least once)
func (s *workflowCacheSuite) TestHistoryCacheConcurrentAccess_Pin() {
	cacheMaxSize := 16
	runIDCount := cacheMaxSize * 4
	coroutineCount := runIDCount * 64

	s.mockShard.GetConfig().HistoryHostLevelCacheMaxSize = dynamicconfig.GetIntPropertyFn(cacheMaxSize)
	s.mockShard.GetConfig().HistoryCacheTTL = dynamicconfig.GetDurationPropertyFn(time.Nanosecond)
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	startGroup := &sync.WaitGroup{}
	stopGroup := &sync.WaitGroup{}
	startGroup.Add(coroutineCount)
	stopGroup.Add(coroutineCount)

	namespaceID := namespace.ID("test_namespace_id")
	workflowID := "wf-cache-test-pinning"
	runIDs := make([]string, runIDCount)
	runIDRefCounter := make([]int32, runIDCount)
	for i := 0; i < runIDCount; i++ {
		runIDs[i] = uuid.NewString()
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
				locks.PriorityHigh,
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
}*/

func (s *workflowCacheSuite) TestHistoryCache_CacheLatencyMetricContext() {
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	ctx := metrics.AddMetricsContext(context.Background())
	currentRelease, err := s.cache.GetOrCreateCurrentExecution(
		ctx,
		s.mockShard,
		tests.NamespaceID,
		tests.WorkflowID,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
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
		locks.PriorityHigh,
	)
	s.NoError(err)
	defer release(nil)

	latency2, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name())
	s.True(ok)
	s.Greater(latency2, latency1)
}

func (s *workflowCacheSuite) TestHistoryCache_CacheHoldTimeMetricContext() {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()

	s.mockShard.SetMetricsHandler(metricsHandler)
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metricsHandler)

	release1, err := s.cache.GetOrCreateCurrentExecution(
		context.Background(),
		s.mockShard,
		tests.NamespaceID,
		tests.WorkflowID,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Eventually(func() bool {
		release1(nil)
		snapshot := capture.Snapshot()
		s.Greater(snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Value, 100*time.Millisecond)
		return tests.NamespaceID.String() == snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Tags["namespace_id"]
	}, 150*time.Millisecond, 100*time.Millisecond)

	capture = metricsHandler.StartCapture()
	release2, err := s.cache.GetOrCreateCurrentExecution(
		context.Background(),
		s.mockShard,
		tests.NamespaceID,
		tests.WorkflowID,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Eventually(func() bool {
		release2(nil)
		snapshot := capture.Snapshot()
		s.Greater(snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Value, 200*time.Millisecond)
		return tests.NamespaceID.String() == snapshot[metrics.HistoryWorkflowExecutionCacheLockHoldDuration.Name()][0].Tags["namespace_id"]
	}, 300*time.Millisecond, 200*time.Millisecond)
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
			callerType: headers.CallerTypeBackgroundHigh,
		},
		{
			name:             "Non API context with timeout and locking beforehand should return an error",
			shouldLockBefore: true,
			callerType:       headers.CallerTypeBackgroundHigh,
			wantErr:          true,
		},
	}
	for _, tt := range testSets {
		s.Run(tt.name, func() {
			c := NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

			namespaceID := namespace.ID("test_namespace_id")
			execution := commonpb.WorkflowExecution{
				WorkflowId: "some random workflow id",
				RunId:      uuid.NewString(),
			}
			cacheKey := Key{
				WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
				ArchetypeID: chasm.WorkflowArchetypeID,
				ShardUUID:   uuid.NewString(),
			}
			workflowCtx := workflow.NewContext(
				s.mockShard.GetConfig(),
				cacheKey.WorkflowKey,
				chasm.WorkflowArchetypeID,
				s.mockShard.GetLogger(),
				s.mockShard.GetThrottledLogger(),
				s.mockShard.GetMetricsHandler(),
			)
			ctx := headers.SetCallerType(context.Background(), tt.callerType)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if tt.shouldLockBefore {
				// lock the workflow to allow it to time out
				err := workflowCtx.Lock(ctx, locks.PriorityHigh)
				s.NoError(err)
			}

			if err := c.(*cacheImpl).lockWorkflowExecution(ctx, workflowCtx, cacheKey, locks.PriorityHigh); (err != nil) != tt.wantErr {
				s.T().Errorf("cacheImpl.lockWorkflowExecution() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func (s *workflowCacheSuite) TestCacheImpl_RejectsRequestWhenAtLimitSimple() {
	config := tests.NewDynamicConfig()
	config.HistoryCacheLimitSizeBased = true
	config.HistoryHostLevelCacheMaxSizeBytes = dynamicconfig.GetIntPropertyFn(1000)
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	s.cache = NewHostLevelCache(config, s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mockMS1 := historyi.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release1, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
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
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)

	// Try to insert another entry before releasing previous.
	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	_, _, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		locks.PriorityHigh,
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
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	s.cache = NewHostLevelCache(config, s.mockShard.GetLogger(), metrics.NoopMetricsHandler)
	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mockMS1 := historyi.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()

	ctx, release1, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
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
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)

	// Insert another 400byte entry.
	execution2 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mockMS2 := historyi.NewMockMutableState(s.controller)
	mockMS2.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release2, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution2,
		locks.PriorityHigh,
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
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS2, ctx.(*workflow.ContextImpl).MutableState)

	// Insert another entry. This should fail as cache has ~800bytes pinned.
	execution3 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mockMS3 := historyi.NewMockMutableState(s.controller)
	mockMS3.EXPECT().IsDirty().Return(false).AnyTimes()
	_, _, err = s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution3,
		locks.PriorityHigh,
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
		locks.PriorityHigh,
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
		locks.PriorityHigh,
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
		locks.PriorityHigh,
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
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		config,
	)
	s.cache = NewHostLevelCache(config, s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution1 := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      uuid.NewString(),
	}
	mockMS1 := historyi.NewMockMutableState(s.controller)
	mockMS1.EXPECT().IsDirty().Return(false).AnyTimes()
	ctx, release1, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		mockShard,
		namespaceID,
		&execution1,
		locks.PriorityHigh,
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
		locks.PriorityHigh,
	)
	s.NoError(err)
	s.Equal(mockMS1, ctx.(*workflow.ContextImpl).MutableState)
	release1(nil)
}

func (s *workflowCacheSuite) TestCacheImpl_GetCurrentRunID_CurrentRunExists() {
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	namespaceID := namespace.ID("test_namespace_id")
	execution := commonpb.WorkflowExecution{
		WorkflowId: "some random workflow ID",
		RunId:      "",
	}

	currentRunID := uuid.NewString()

	mockExecutionManager := s.mockShard.Resource.ExecutionMgr
	mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.GetWorkflowId(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.NewString(),
		RunID:          currentRunID,
		State:          enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil).Times(1)

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution,
		locks.PriorityHigh,
	)
	s.NoError(err)

	s.Equal(currentRunID, ctx.GetWorkflowKey().RunID)
	release(nil)
}

func (s *workflowCacheSuite) TestCacheImpl_GetCurrentRunID_NoCurrentRun() {
	s.cache = NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

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
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(nil, serviceerror.NewNotFound("current worflow not found")).Times(1)

	ctx, release, err := s.cache.GetOrCreateWorkflowExecution(
		context.Background(),
		s.mockShard,
		namespaceID,
		&execution,
		locks.PriorityHigh,
	)
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)
	s.Nil(ctx)
	s.Nil(release)
}
