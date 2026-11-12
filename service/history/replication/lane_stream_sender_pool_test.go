package replication

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type lanePoolFixture struct {
	pool      *lanePool
	engine    *historyi.MockEngine
	shard     *historyi.MockShardContext
	converter *MockSourceTaskConverter
	server    *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer
	flow      *MockSenderFlowController
}

func newLanePoolFixture(t *testing.T) lanePoolFixture {
	t.Helper()
	controller := gomock.NewController(t)
	fixture := lanePoolFixture{
		engine: historyi.NewMockEngine(controller), shard: historyi.NewMockShardContext(controller),
		converter: NewMockSourceTaskConverter(controller), server: historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesServer(controller),
		flow: NewMockSenderFlowController(controller),
	}
	fixture.server.EXPECT().Context().Return(t.Context()).AnyTimes()
	fixture.shard.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()
	fixture.shard.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()
	registry, err := newSenderLaneRegistry(100, nil)
	require.NoError(t, err)
	sender := &laneStreamSender{
		server: fixture.server, historyEngine: fixture.engine, shardContext: fixture.shard, taskConverter: fixture.converter,
		config: tests.NewDynamicConfig(), laneRegistry: registry, flowController: fixture.flow,
		clientClusterName: "target", clientClusterShardCount: 1,
		clientShardKey: ClusterShardKey{ClusterID: 2, ShardID: 1}, serverShardKey: ClusterShardKey{ClusterID: 1, ShardID: 1},
		metrics: metrics.NoopMetricsHandler, logger: log.NewNoopLogger(), shutdownChan: channel.NewShutdownOnce(),
		requestedLanes: make(map[string]struct{}), laneLimiters: make(map[string]LaneLimiter), laneRefusedAt: make(map[string]time.Time),
	}
	sender.lanesConfirmed.Store(true)
	sender.config.EmitReplicationLifecycleEvents = dynamicconfig.GetBoolPropertyFn(false)
	fixture.pool = newLanePool(sender)
	t.Cleanup(fixture.pool.cancel)
	return fixture
}

func (f lanePoolFixture) addLane(t *testing.T, key string, class int) senderLaneSnapshot {
	t.Helper()
	lane, created, err := f.pool.sender.laneRegistry.Create(key, LaneFilter{NamespaceIDs: []namespace.ID{namespace.ID(key)}}, 100, nil)
	require.NoError(t, err)
	require.True(t, created)
	f.pool.sender.requestedLanes[key] = struct{}{}
	f.pool.sender.laneRegistry.SetSchedulingClass(key, class)
	return lane
}

func TestLanePoolPreparationBoundsAndClassChange(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110)).AnyTimes()
	for i := range 8 {
		f.addLane(t, fmt.Sprintf("namespace-%d", i), i%2)
	}
	p := f.pool
	p.reconcile()
	for range 2 {
		p.prepareMore()
		require.Equal(t, 2, p.preparing)
		for range 2 {
			job := await.Rcv(t, p.prepareJobs)
			require.NoError(t, p.complete(lanePoolResult{lane: job.lane, stage: lanePoolPreparing,
				payload: &replicationspb.ReplicationTask{}, nextCursor: 101, controlTime: p.now()}))
		}
	}
	p.prepareMore()
	require.Zero(t, p.preparing)
	require.Equal(t, 2, p.slots[0])
	require.Equal(t, 2, p.slots[1])
	lane := p.lanes[0]
	oldClass := lane.lane.schedulingClass
	p.sender.laneRegistry.SetSchedulingClass(lane.lane.logicalKey, 2)
	p.reconcile()
	require.False(t, lane.held)
	require.False(t, lane.slot)
	require.Nil(t, lane.payload)
	require.Equal(t, 1, p.slots[oldClass])
	require.Equal(t, 2, lane.lane.schedulingClass)
	p.prepareMore()
	require.LessOrEqual(t, p.preparing, 2)
	for _, entry := range p.lanes {
		p.release(entry)
	}
	require.Equal(t, [laneSchedulingClassCount]int{}, p.slots)
}

func TestLanePoolRestoredRetirementKeepsFiniteScope(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	lane := f.addLane(t, "namespace", 0)
	registry := f.pool.sender.laneRegistry
	_, acquired := registry.Acquire(lane.id)
	require.True(t, acquired)
	_, acquired = registry.AcquireDefault(105)
	require.True(t, acquired)
	registry.ReleaseDefault(105, true)
	require.True(t, registry.RequestRetirement(lane.logicalKey))
	registry.AdvanceLaneCursor(lane.id, 105)
	registry.Release(lane.id)
	snapshot, exists := registry.SnapshotByID(lane.id)
	require.True(t, exists)
	require.Equal(t, int64(105), snapshot.scope.Range.ExclusiveMax.TaskID)
	// Recovery replays the unacknowledged prefix without expanding its fixed end.
	state := registry.BuildReaderState(syncReplicationState(100, 100, 100))
	restored, err := newSenderLaneRegistry(105, state.Lanes)
	require.NoError(t, err)
	f.pool.sender.laneRegistry = restored
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110))
	f.pool.reconcile()
	f.pool.prepareMore()
	job := await.Rcv(t, f.pool.prepareJobs)
	require.Equal(t, int64(105), job.end)
	f.pool.release(job.lane)
}

func TestLanePoolPauseDoesNotFixRetirementBoundary(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	lane := f.addLane(t, "namespace", 0)
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110))
	f.pool.reconcile()
	f.pool.prepareMore()
	job := await.Rcv(t, f.pool.prepareJobs)
	require.NoError(t, f.pool.complete(lanePoolResult{lane: job.lane, stage: lanePoolPreparing, payload: &replicationspb.ReplicationTask{}}))
	registry := f.pool.sender.laneRegistry
	registry.RequestRetirement(lane.logicalKey)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: 100, FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE},
	})
	f.pool.reconcile()
	require.False(t, job.lane.held)
	require.Zero(t, f.pool.slots[0])
	snapshot, exists := registry.SnapshotByID(lane.id)
	require.True(t, exists)
	require.Equal(t, int64(math.MaxInt64), snapshot.scope.Range.ExclusiveMax.TaskID)
	require.Empty(t, registry.ReadyRetirements())
}

func TestLanePoolCommittedWriteSurvivesConcurrentPolicyChange(t *testing.T) {
	t.Parallel()
	for _, paused := range []bool{false, true} {
		t.Run(fmt.Sprintf("paused=%t", paused), func(t *testing.T) {
			t.Parallel()
			f := newLanePoolFixture(t)
			lane := f.addLane(t, "namespace", 0)
			registry := f.pool.sender.laneRegistry
			f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110))
			f.pool.reconcile()
			f.pool.prepareMore()
			job := await.Rcv(t, f.pool.prepareJobs)
			require.NoError(t, f.pool.complete(lanePoolResult{lane: job.lane, stage: lanePoolPreparing,
				payload: &replicationspb.ReplicationTask{SourceTaskId: 100}, nextCursor: 101, controlTime: f.pool.now()}))
			f.pool.startWrite(job.lane)
			write := await.Rcv(t, f.pool.writeJobs)
			f.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(*historyservice.StreamWorkflowReplicationMessagesResponse) error {
				registry.SetSchedulingClass(lane.logicalKey, 1)
				registry.RequestRetirement(lane.logicalKey)
				if paused {
					registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
						lane.id: {InclusiveLowWatermark: 100, FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE},
					})
				}
				return nil
			})
			require.NoError(t, f.pool.complete(f.pool.write(write)))
			snapshot, exists := registry.SnapshotByID(lane.id)
			require.True(t, exists)
			require.Equal(t, int64(101), snapshot.cursor)
			if paused {
				require.Equal(t, int64(math.MaxInt64), snapshot.scope.Range.ExclusiveMax.TaskID)
				require.Empty(t, registry.ReadyRetirements())
				registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
					lane.id: {InclusiveLowWatermark: 100, FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME},
				})
				snapshot, _ = registry.SnapshotByID(lane.id)
			}
			require.Equal(t, int64(101), snapshot.scope.Range.ExclusiveMax.TaskID)
			require.False(t, job.lane.held)
			require.Zero(t, f.pool.slots[0])
		})
	}
}

func TestLanePoolSlowConversionKeepsOtherClassMoving(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.addLane(t, "noisy", 0)
	f.addLane(t, "healthy", 1)
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(102)).AnyTimes()
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(namespace.NewGlobalNamespaceForTest(nil, nil,
		&persistencespb.NamespaceReplicationConfig{Clusters: []string{"source", "target"}}, 100), nil).AnyTimes()
	f.shard.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
	items := []tasks.Task{
		&tasks.SyncWorkflowStateTask{WorkflowKey: definition.WorkflowKey{NamespaceID: "noisy", WorkflowID: "workflow"}, TaskID: 100, Priority: enumsspb.TASK_PRIORITY_HIGH},
		&tasks.SyncWorkflowStateTask{WorkflowKey: definition.WorkflowKey{NamespaceID: "healthy", WorkflowID: "workflow"}, TaskID: 101, Priority: enumsspb.TASK_PRIORITY_HIGH},
	}
	f.engine.EXPECT().GetReplicationTasksIter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, start, end int64) (collection.Iterator[tasks.Task], error) {
			return collection.NewPagingIterator[tasks.Task](func([]byte) ([]tasks.Task, []byte, error) {
				var selected []tasks.Task
				for _, item := range items {
					if item.GetTaskID() >= start && item.GetTaskID() < end {
						selected = append(selected, item)
					}
				}
				return selected, nil, nil
			}), nil
		}).AnyTimes()
	notifications := make(chan struct{})
	f.engine.EXPECT().SubscribeReplicationNotification("target").Return(notifications, "subscriber")
	f.engine.EXPECT().UnsubscribeReplicationNotification("subscriber")
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	f.converter.EXPECT().Convert(gomock.Any(), int32(2), enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow).DoAndReturn(
		func(item tasks.Task, _ int32, _ enumsspb.TaskPriority, _ locks.Priority) (*replicationspb.ReplicationTask, error) {
			if item.GetNamespaceID() == "noisy" {
				close(blocked)
				select {
				case <-release:
				case <-f.pool.ctx.Done():
					return nil, context.Canceled
				}
			}
			return &replicationspb.ReplicationTask{SourceTaskId: item.GetTaskID()}, nil
		}).AnyTimes()
	f.flow.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_HIGH).Return(nil).AnyTimes()
	sent := make(chan int64, 8)
	f.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(response *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		for _, task := range response.GetMessages().GetReplicationTasks() {
			sent <- task.SourceTaskId
		}
		return nil
	}).AnyTimes()
	done := make(chan error, 1)
	go func() { done <- f.pool.run() }()
	t.Cleanup(func() {
		f.pool.cancel()
		unblock()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("lane pool did not join its workers")
		}
	})
	await.Rcv(t, blocked)
	require.Equal(t, int64(101), await.Rcv(t, sent))
	unblock()
	require.Equal(t, int64(100), await.Rcv(t, sent))
}

func TestLaneLimiterReadinessDoesNotSpend(t *testing.T) {
	t.Parallel()
	limiter := newRateLaneLimiter(func() float64 { return 1 })
	now := time.Now()
	for range 10 {
		ready, delay := limiter.Ready(now)
		require.True(t, ready)
		require.Zero(t, delay)
	}
	allowed, _ := limiter.Reserve(now)
	require.True(t, allowed)
	for range 10 {
		ready, delay := limiter.Ready(now)
		require.False(t, ready)
		require.Equal(t, time.Second, delay)
	}
	allowed, _ = limiter.Reserve(now.Add(time.Second))
	require.True(t, allowed)
}

func TestLanePoolClassChangePreservesCredit(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.addLane(t, "namespace", 0)
	f.pool.sender.laneLimiters["namespace"] = newRateLaneLimiter(func() float64 { return 1 })
	f.pool.reconcile()
	lane := f.pool.lanes[0]
	allowed, _ := f.pool.admission(lane, true)
	require.True(t, allowed)
	f.pool.sender.laneRegistry.SetSchedulingClass("namespace", 1)
	f.pool.reconcile()
	require.Equal(t, 1, lane.lane.schedulingClass)
	allowed, delay := f.pool.admission(lane, true)
	require.False(t, allowed)
	require.Positive(t, delay)
}

func TestLanePoolCanceledWorkerDoesNotStartQueuedJob(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.pool.prepareJobs <- lanePoolJob{}
	f.pool.cancel()
	called := false
	f.pool.worker(f.pool.prepareJobs, func(lanePoolJob) lanePoolResult {
		called = true
		return lanePoolResult{}
	})
	require.False(t, called)
}

func TestLanePoolFailureStopsStreamBeforeJoiningWriter(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.addLane(t, "prepared", 0)
	f.addLane(t, "failing", 1)
	p := f.pool
	p.sender.status = common.DaemonStatusStarted
	p.reconcile()
	for _, lane := range p.lanes {
		if lane.lane.logicalKey == "prepared" {
			snapshot, acquired := p.sender.laneRegistry.Acquire(lane.lane.id)
			require.True(t, acquired)
			lane.lane, lane.held, lane.phase = snapshot, true, lanePoolPrepared
			lane.nextCursor, lane.end = 101, 110
		}
	}
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110)).AnyTimes()
	notifications := make(chan struct{})
	f.engine.EXPECT().SubscribeReplicationNotification("target").Return(notifications, "subscriber")
	f.engine.EXPECT().UnsubscribeReplicationNotification("subscriber")
	started := make(chan struct{})
	f.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(*historyservice.StreamWorkflowReplicationMessagesResponse) error {
		close(started)
		<-p.sender.shutdownChan.Channel()
		return context.Canceled
	})
	f.engine.EXPECT().GetReplicationTasksIter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, int64, int64) (collection.Iterator[tasks.Task], error) {
			select {
			case <-started:
				return nil, errors.New("persistence unavailable")
			case <-p.ctx.Done():
				return nil, p.ctx.Err()
			}
		})
	done := make(chan error, 1)
	go func() { done <- p.run() }()
	t.Cleanup(func() {
		p.sender.Stop()
		p.cancel()
	})
	select {
	case err := <-done:
		require.ErrorContains(t, err, "persistence unavailable")
	case <-time.After(5 * time.Second):
		t.Fatal("pool failed to stop the stream before joining its writer")
	}
}

func TestLanePoolSchedulerSelectionAndClassValidation(t *testing.T) {
	t.Parallel()
	config := tests.NewDynamicConfig()
	for _, name := range []string{"per-lane", "pooled", "invalid"} {
		config.ReplicationStreamSenderLaneScheduler = dynamicconfig.GetStringPropertyFn(name)
		require.Equal(t, name == "pooled", usePooledLaneScheduling(config))
	}
	lane := DesiredLane{Key: "lane", Filter: LaneFilter{NamespaceIDs: []namespace.ID{"namespace"}}}
	for _, class := range []int{0, 15} {
		lane.SchedulingClass = class
		require.NoError(t, validateDesiredLane(lane))
	}
	for _, class := range []int{-1, 16} {
		lane.SchedulingClass = class
		require.ErrorContains(t, validateDesiredLane(lane), "scheduling class")
	}
	f := newLanePoolFixture(t)
	f.pool.sender.status = common.DaemonStatusStarted
	f.pool.sender.lanesEnabledSetting = f.pool.sender.config.ReplicationStreamSenderLanesEnabled()
	require.True(t, f.pool.sender.IsValid())
	f.pool.sender.config.ReplicationStreamSenderLaneScheduler = dynamicconfig.GetStringPropertyFn("pooled")
	require.False(t, f.pool.sender.IsValid())
	f.pool.sender.pooledLaneScheduling = true
	require.True(t, f.pool.sender.IsValid())
	f.pool.sender.config.ReplicationStreamSenderLaneScheduler = dynamicconfig.GetStringPropertyFn("per-lane")
	require.False(t, f.pool.sender.IsValid())
}

func TestLanePoolLimiterPanicAndHandoverStillRespectPause(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"readiness panic", "reservation panic", "handover"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newLanePoolFixture(t)
			lane := f.addLane(t, "namespace", 0)
			f.pool.reconcile()
			f.pool.sender.laneLimiters["namespace"] = lanePoolTestLimiter{
				ready:   func(time.Time) (bool, time.Duration) { panic("readiness failure") },
				reserve: func(time.Time) (bool, time.Duration) { panic("reservation failure") },
			}
			f.pool.sender.handoverInProgress.Store(mode == "handover")
			ready, _ := f.pool.admission(f.pool.lanes[0], mode == "reservation panic")
			require.True(t, ready)
			require.Equal(t, mode != "handover", f.pool.sender.lanePolicyDisabled.Load())
			f.pool.sender.laneRegistry.ObserveAcks(map[string]*replicationspb.ReplicationState{
				lane.id: {FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE},
			})
			ready, _ = f.pool.admission(f.pool.lanes[0], true)
			require.False(t, ready)
		})
	}
}

func TestLanePoolRetryReleasesPreparationCapacity(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.addLane(t, "namespace", 0)
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110)).AnyTimes()
	f.pool.sender.config.ReplicationTaskConverterLowPriorityLockMaxAttempts = dynamicconfig.GetIntPropertyFn(1)
	f.pool.reconcile()
	f.pool.prepareMore()
	job := await.Rcv(t, f.pool.prepareJobs)
	raw := &lanePoolRaw{item: &tasks.SyncWorkflowStateTask{TaskID: 100}, lockPriority: locks.PriorityLow,
		retry:    backoff.NewRetrier(backoff.NewExponentialRetryPolicy(time.Second), clock.NewRealTimeSource()),
		throttle: backoff.NewRetrier(backoff.NewExponentialRetryPolicy(time.Second), clock.NewRealTimeSource())}
	require.NoError(t, f.pool.complete(lanePoolResult{lane: job.lane, stage: lanePoolPreparing, raw: raw,
		nextCursor: 101, err: consts.ErrResourceExhaustedBusyWorkflow}))
	require.Zero(t, f.pool.preparing)
	require.Zero(t, f.pool.slots[0])
	require.True(t, job.lane.held)
	require.Equal(t, lanePoolRetry, job.lane.phase)
	require.Equal(t, locks.PriorityHigh, raw.lockPriority)
	f.pool.now = func() time.Time { return job.lane.due }
	f.pool.prepareMore()
	retry := await.Rcv(t, f.pool.prepareJobs)
	require.Same(t, raw, retry.raw)
	require.Equal(t, int64(100), retry.snapshot.cursor)
	f.pool.release(job.lane)
}

func TestLanePoolOnlySkipsIdentifiedConversionFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		err      error
		skip     bool
		wantSkip bool
	}{
		{name: "conversion opted in", err: &convertError{err: errUnknownReplicationTask}, skip: true, wantSkip: true},
		{name: "conversion opted out", err: &convertError{err: errUnknownReplicationTask}},
		{name: "send failure", err: errors.New("send failed"), skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newLanePoolFixture(t)
			lane := f.addLane(t, "namespace", 0)
			f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110))
			f.pool.sender.config.ReplicationStreamSenderSkipStuckTask = dynamicconfig.GetBoolPropertyFn(tc.skip)
			f.pool.reconcile()
			f.pool.prepareMore()
			job := await.Rcv(t, f.pool.prepareJobs)
			raw := &lanePoolRaw{item: &tasks.SyncWorkflowStateTask{TaskID: 100}, attempt: 1,
				retry: backoff.NewRetrier(backoff.NewExponentialRetryPolicy(time.Second).WithMaximumAttempts(1), clock.NewRealTimeSource())}
			err := f.pool.complete(lanePoolResult{lane: job.lane, stage: lanePoolPreparing, raw: raw, nextCursor: 101, controlTime: f.pool.now(), err: tc.err})
			snapshot, exists := f.pool.sender.laneRegistry.SnapshotByID(lane.id)
			require.True(t, exists)
			require.Equal(t, int64(100), snapshot.cursor)
			if !tc.wantSkip {
				require.Error(t, err)
				f.pool.release(job.lane)
				return
			}
			require.NoError(t, err)
			require.Nil(t, job.lane.raw)
			require.Nil(t, job.lane.payload)
			require.Equal(t, lanePoolPrepared, job.lane.phase)
			require.NoError(t, f.pool.dispatch())
			write := await.Rcv(t, f.pool.writeJobs)
			f.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(response *historyservice.StreamWorkflowReplicationMessagesResponse) error {
				require.Empty(t, response.GetMessages().ReplicationTasks)
				require.Equal(t, int64(101), response.GetMessages().ExclusiveHighWatermark)
				return nil
			})
			require.NoError(t, f.pool.complete(f.pool.write(write)))
			snapshot, _ = f.pool.sender.laneRegistry.SnapshotByID(lane.id)
			require.Equal(t, int64(101), snapshot.cursor)
		})
	}
}

type lanePoolTestLimiter struct {
	ready   func(time.Time) (bool, time.Duration)
	reserve func(time.Time) (bool, time.Duration)
}

func (l lanePoolTestLimiter) Ready(now time.Time) (bool, time.Duration)   { return l.ready(now) }
func (l lanePoolTestLimiter) Reserve(now time.Time) (bool, time.Duration) { return l.reserve(now) }

func TestLanePoolReadinessAndDebitAreSeparate(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.addLane(t, "namespace", 0)
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110))
	ready, accepted := false, false
	debits := 0
	f.pool.sender.laneLimiters["namespace"] = lanePoolTestLimiter{
		ready: func(time.Time) (bool, time.Duration) { return ready, time.Second },
		reserve: func(time.Time) (bool, time.Duration) {
			if accepted {
				debits++
			}
			return accepted, time.Second
		},
	}
	f.pool.reconcile()
	f.pool.prepareMore()
	job := await.Rcv(t, f.pool.prepareJobs)
	require.NoError(t, f.pool.complete(lanePoolResult{lane: job.lane, stage: lanePoolPreparing, payload: &replicationspb.ReplicationTask{}, nextCursor: 101}))
	require.NoError(t, f.pool.dispatch())
	require.Empty(t, f.pool.flowJobs)
	require.Zero(t, debits)
	ready = true
	f.pool.wake()
	require.NoError(t, f.pool.dispatch())
	await.Rcv(t, f.pool.flowJobs)
	require.NoError(t, f.pool.complete(lanePoolResult{lane: job.lane, stage: lanePoolFlow}))
	require.NoError(t, f.pool.dispatch())
	require.Empty(t, f.pool.writeJobs)
	require.Zero(t, debits)
	accepted = true
	f.pool.wake()
	require.NoError(t, f.pool.dispatch())
	await.Rcv(t, f.pool.flowJobs)
	require.NoError(t, f.pool.complete(lanePoolResult{lane: job.lane, stage: lanePoolFlow}))
	require.NoError(t, f.pool.dispatch())
	await.Rcv(t, f.pool.writeJobs)
	require.Equal(t, 1, debits)
	f.pool.release(job.lane)
}

func TestLanePolicyRateOnlyPublicationWakesExecutor(t *testing.T) {
	t.Parallel()
	f := newLanePoolFixture(t)
	f.addLane(t, "a", 0)
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(110))
	f.shard.EXPECT().GetReplicationStatus([]string{"target"}).Return(nil, nil, nil)
	policy := NewMockLaneStreamPolicy(gomock.NewController(t))
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{{Key: "a", Filter: LaneFilter{NamespaceIDs: []namespace.ID{"a"}}}}, nil)
	f.pool.sender.lanePolicy = policy
	f.pool.sender.laneLimiters["a"] = newRateLaneLimiter(func() float64 { return 10 })
	changed := f.pool.sender.laneRegistry.Changed()
	state := syncReplicationState(100, 100, 100)
	state.MaxLanes = 4
	require.NoError(t, f.pool.sender.reconcileLanes(state, false))
	require.False(t, f.pool.sender.laneRegistry.Snapshots()[0].retiring)
	select {
	case <-changed:
	default:
		t.Fatal("policy publication with unchanged membership did not wake executor")
	}
}
