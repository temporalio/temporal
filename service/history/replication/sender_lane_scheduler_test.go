package replication

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/await"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type laneSenderTest struct {
	sender       *StreamSenderImpl
	server       *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer
	engine       *historyi.MockEngine
	shard        *historyi.MockShardContext
	converter    *MockSourceTaskConverter
	sent         chan *replicationspb.WorkflowReplicationMessages
	subscribed   chan struct{}
	unsubscribed chan struct{}
}

func newLaneSenderTest(t *testing.T, classCount int, scanTasks []tasks.Task) *laneSenderTest {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &laneSenderTest{
		server:       historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesServer(ctrl),
		engine:       historyi.NewMockEngine(ctrl),
		shard:        historyi.NewMockShardContext(ctrl),
		converter:    NewMockSourceTaskConverter(ctrl),
		sent:         make(chan *replicationspb.WorkflowReplicationMessages, 100),
		subscribed:   make(chan struct{}, 100),
		unsubscribed: make(chan struct{}, 100),
	}
	f.server.EXPECT().Context().Return(context.Background()).AnyTimes()
	f.shard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	f.shard.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()
	f.shard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	config := tests.NewDynamicConfig()
	config.EnableReplicationTaskTieredProcessing = func() bool { return false }
	config.ReplicationStreamSendEmptyTaskDuration = func() time.Duration { return time.Hour }
	config.ReplicationStreamSenderErrorRetryMaxAttempts = func() int { return 1 }
	config.ReplicationStreamSenderErrorRetryWait = func() time.Duration { return time.Nanosecond }
	config.ReplicationStreamSenderErrorRetryMaxInterval = func() time.Duration { return time.Nanosecond }
	f.sender = NewStreamSender(f.server, f.shard, f.engine, quotas.NoopRequestRateLimiter, f.converter,
		"target_cluster", 1, NewClusterShardKey(2, 1), NewClusterShardKey(1, 1), config)
	registry, err := newSenderLaneRegistry(100, nil, classCount)
	require.NoError(t, err)
	f.sender.laneRegistry = registry
	f.sender.laneRateLimiters = make([]quotas.RateLimiter, classCount)
	f.sender.initialLaneStateApplied = make(chan struct{})
	f.sender.lanesConfirmed.Store(true)
	f.sender.markInitialLaneStateApplied()
	t.Cleanup(func() {
		f.sender.cancel()
		f.sender.shutdownChan.Shutdown()
	})

	var subscriberCount atomic.Int64
	f.engine.EXPECT().SubscribeReplicationNotification("target_cluster").DoAndReturn(func(string) (<-chan struct{}, string) {
		f.subscribed <- struct{}{}
		return make(chan struct{}), strconv.FormatInt(subscriberCount.Add(1), 10)
	}).AnyTimes()
	f.engine.EXPECT().UnsubscribeReplicationNotification(gomock.Any()).Do(func(string) {
		f.unsubscribed <- struct{}{}
	}).AnyTimes()
	end := int64(100 + len(scanTasks))
	f.shard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(end)).AnyTimes()
	f.engine.EXPECT().GetReplicationTasksIter(gomock.Any(), string(f.sender.clientShardKey.ClusterID), gomock.Any(), end).
		DoAndReturn(func(_ context.Context, _ string, begin, _ int64) (collection.Iterator[tasks.Task], error) {
			var remaining []tasks.Task
			for _, task := range scanTasks {
				if task.GetTaskID() >= begin {
					remaining = append(remaining, task)
				}
			}
			return collection.NewPagingIterator[tasks.Task](func([]byte) ([]tasks.Task, []byte, error) {
				return remaining, nil, nil
			}), nil
		}).AnyTimes()
	if len(scanTasks) > 0 {
		namespaces := namespace.NewMockRegistry(ctrl)
		for _, namespaceID := range []namespace.ID{"a", "b"} {
			namespaces.EXPECT().GetNamespaceByID(namespaceID).Return(namespace.NewGlobalNamespaceForTest(nil, nil,
				&persistencespb.NamespaceReplicationConfig{Clusters: []string{"source_cluster", "target_cluster"}}, 100), nil).AnyTimes()
		}
		f.shard.EXPECT().GetNamespaceRegistry().Return(namespaces).AnyTimes()
	}
	f.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(response *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		f.sent <- response.GetMessages()
		return nil
	}).AnyTimes()
	return f
}

func (f *laneSenderTest) createLane(t *testing.T, key string) senderLaneSnapshot {
	t.Helper()
	lane, created, err := f.sender.laneRegistry.Create("namespace:"+key, namespaceLaneScope(key, 100), 1)
	require.NoError(t, err)
	require.True(t, created)
	return lane
}

func (f *laneSenderTest) start(t *testing.T) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		done <- f.sender.sendLaneEventLoop()
		close(finished)
	}()
	t.Cleanup(func() {
		f.sender.shutdownChan.Shutdown()
		select {
		case <-finished:
		case <-time.After(time.Second):
			require.Fail(t, "lane workers did not stop")
		}
	})
	return done
}

func laneTestTask(namespaceID string, taskID int64) tasks.Task {
	return &tasks.HistoryReplicationTask{
		WorkflowKey:         definition.NewWorkflowKey(namespaceID, "workflow", "run"),
		TaskID:              taskID,
		VisibilityTimestamp: time.Now().UTC(),
	}
}

func laneTestConversion(task tasks.Task) *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		SourceTaskId: task.GetTaskID(), VisibilityTime: timestamppb.New(task.GetVisibilityTime()),
	}
}

func TestLaneWorkersSlowConversionDoesNotBlockPeer(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(strconv.FormatBool(retry), func(t *testing.T) {
			f := newLaneSenderTest(t, 1, []tasks.Task{laneTestTask("a", 100), laneTestTask("b", 101)})
			f.createLane(t, "a")
			blocked := make(chan struct{})
			release := make(chan struct{})
			attempts := 0
			if retry {
				f.sender.config.ReplicationStreamSenderErrorRetryMaxAttempts = func() int { return 2 }
			}
			f.converter.EXPECT().Convert(gomock.Any(), f.sender.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow).
				DoAndReturn(func(task tasks.Task, _ int32, _ enumsspb.TaskPriority, _ locks.Priority) (*replicationspb.ReplicationTask, error) {
					if task.GetNamespaceID() == "a" {
						attempts++
						if !retry || attempts == 2 {
							close(blocked)
							<-release
						}
						return nil, errors.New("conversion failed")
					}
					return laneTestConversion(task), nil
				}).AnyTimes()
			done := f.start(t)
			released := false
			t.Cleanup(func() {
				if !released {
					close(release)
				}
			})
			await.Rcv(t, blocked)
			peer := f.createLane(t, "b")
			message := await.Rcv(t, f.sent)
			require.Equal(t, peer.id, message.GetLaneInfo().GetLaneId())
			require.Len(t, message.GetReplicationTasks(), 1)
			require.Equal(t, int64(101), message.GetReplicationTasks()[0].GetSourceTaskId())
			close(release)
			released = true
			require.Error(t, await.Rcv(t, done))
		})
	}
}

func TestLaneWorkerUsesNewClassAfterInFlightSend(t *testing.T) {
	f := newLaneSenderTest(t, 2, []tasks.Task{laneTestTask("a", 100), laneTestTask("a", 101)})
	lane := f.createLane(t, "a")
	f.converter.EXPECT().Convert(gomock.Any(), f.sender.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow).
		DoAndReturn(func(task tasks.Task, _ int32, _ enumsspb.TaskPriority, _ locks.Priority) (*replicationspb.ReplicationTask, error) {
			return laneTestConversion(task), nil
		}).Times(2)
	ctrl := gomock.NewController(t)
	class1 := quotas.NewMockRateLimiter(ctrl)
	class2 := quotas.NewMockRateLimiter(ctrl)
	f.sender.laneRateLimiters = []quotas.RateLimiter{class1, class2}
	blocked := make(chan struct{})
	release := make(chan struct{})
	class1.EXPECT().Wait(gomock.Any()).DoAndReturn(func(context.Context) error {
		close(blocked)
		<-release
		return nil
	})
	class2.EXPECT().Wait(gomock.Any()).Return(nil)
	done := f.start(t)
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	await.Rcv(t, blocked)
	await.Rcv(t, f.subscribed)
	_, changed := f.sender.laneRegistry.SetClass(lane.logicalKey, 2)
	require.True(t, changed)
	close(release)
	released = true
	for _, taskID := range []int64{100, 101} {
		message := await.Rcv(t, f.sent)
		require.Equal(t, lane.id, message.GetLaneInfo().GetLaneId())
		require.Equal(t, taskID, message.GetReplicationTasks()[0].GetSourceTaskId())
	}
	f.sender.shutdownChan.Shutdown()
	require.NoError(t, await.Rcv(t, done))
	require.Empty(t, f.subscribed)
}

func TestLaneWorkersWaitForInitialState(t *testing.T) {
	f := newLaneSenderTest(t, 1, nil)
	lane := f.createLane(t, "a")
	f.sender.initialLaneStateApplied = make(chan struct{})
	done := f.start(t)
	require.Never(t, func() bool { return len(f.subscribed) != 0 }, 100*time.Millisecond, time.Millisecond)
	close(f.sender.initialLaneStateApplied)
	message := await.Rcv(t, f.sent)
	require.Equal(t, lane.id, message.GetLaneInfo().GetLaneId())
	f.sender.shutdownChan.Shutdown()
	require.NoError(t, await.Rcv(t, done))
}

func TestLaneWorkersRetirementStopsWorkerAndAllowsNewLane(t *testing.T) {
	f := newLaneSenderTest(t, 1, nil)
	lane := f.createLane(t, "a")
	done := f.start(t)
	await.Rcv(t, f.sent)
	f.sender.laneRegistry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {InclusiveLowWatermark: 100},
	})
	await.RequireTrue(t, func() bool { return f.sender.laneRegistry.RequestRetirement(lane.logicalKey) }, time.Second, time.Millisecond)
	_, retired := f.sender.laneRegistry.CompleteRetirement(lane.id)
	require.True(t, retired)
	await.Rcv(t, f.unsubscribed)
	newLane := f.createLane(t, "a")
	for {
		message := await.Rcv(t, f.sent)
		if message.GetLaneInfo().GetLaneId() == newLane.id {
			break
		}
	}
	f.sender.shutdownChan.Shutdown()
	require.NoError(t, await.Rcv(t, done))
}

func TestLaneWorkerShutdownCancelsRateLimitWaitAndReleasesLease(t *testing.T) {
	f := newLaneSenderTest(t, 1, []tasks.Task{laneTestTask("a", 100)})
	lane := f.createLane(t, "a")
	f.converter.EXPECT().Convert(gomock.Any(), f.sender.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow).
		Return(laneTestConversion(laneTestTask("a", 100)), nil)
	limiter := quotas.NewMockRateLimiter(gomock.NewController(t))
	f.sender.laneRateLimiters[0] = limiter
	waiting := make(chan struct{})
	limiter.EXPECT().Wait(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		close(waiting)
		<-ctx.Done()
		return ctx.Err()
	})
	done := f.start(t)
	await.Rcv(t, waiting)
	f.sender.shutdownChan.Shutdown()
	require.NoError(t, await.Rcv(t, done))
	_, _, acquired := f.sender.laneRegistry.Acquire(lane)
	require.True(t, acquired)
	f.sender.laneRegistry.Release(lane.id)
	require.Empty(t, f.sent)
	await.Rcv(t, f.unsubscribed)
}

func TestLaneWorkerWaitsForDefaultHandoff(t *testing.T) {
	f := newLaneSenderTest(t, 1, nil)
	_, acquired := f.sender.laneRegistry.AcquireDefault(100)
	require.True(t, acquired)
	lane := f.createLane(t, "a")
	done := f.start(t)
	await.Rcv(t, f.subscribed)
	require.Never(t, func() bool { return len(f.sent) != 0 }, 100*time.Millisecond, time.Millisecond)
	f.sender.laneRegistry.ReleaseDefault(100, true)
	message := await.Rcv(t, f.sent)
	require.Equal(t, lane.id, message.GetLaneInfo().GetLaneId())
	f.sender.shutdownChan.Shutdown()
	require.NoError(t, await.Rcv(t, done))
}

func TestStreamSenderLaneLimit(t *testing.T) {
	for _, limit := range []int{-1, 0, 1} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			f := newLaneSenderTest(t, 1, nil)
			config := f.sender.config
			config.EnableReplicationTaskTieredProcessing = func() bool { return true }
			config.EnableReplicationReaderGroup = func() bool { return true }
			config.EnableReplicationStreamLanes = func() bool { return true }
			config.ReplicationStreamSenderMaxLanes = func() int { return limit }
			if limit > 0 {
				f.shard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(nil, false)
			}
			sender := NewStreamSender(f.server, f.shard, f.engine, quotas.NoopRequestRateLimiter, f.converter,
				"target_cluster", 1, f.sender.clientShardKey, f.sender.serverShardKey, config)
			t.Cleanup(sender.cancel)
			if limit <= 0 {
				require.Nil(t, sender.laneRegistry)
				require.Nil(t, sender.laneController)
				require.Nil(t, sender.initialLaneStateApplied)
				require.Empty(t, sender.laneRateLimiters)
			} else {
				require.NotNil(t, sender.laneRegistry)
				require.NotNil(t, sender.laneController)
				require.Equal(t, limit, sender.laneController.maxLanes)
			}
			if limit == 0 {
				state := buildTieredReaderState(syncReplicationState(50, 200, 100))
				state.ReplicationLaneDefaultCursor = shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(200))
				state.Lanes = []*persistencespb.QueueReaderLane{{
					LogicalKey: "namespace:a", Scope: state.Scopes[readerOverallScopeIndex],
				}}
				queueState := &persistencespb.QueueState{
					ReaderStates: map[int64]*persistencespb.QueueReaderState{sender.readerGroup.ReaderID(): state},
				}
				f.shard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(queueState, true).Times(2)
				require.Equal(t, int64(50), sender.catchupBeginWatermark(enumsspb.TASK_PRIORITY_HIGH, 999))
			}
			if limit > 0 {
				config.ReplicationStreamSenderMaxLanes = func() int { return limit + 1 }
				require.ErrorContains(t, sender.recvEventLoop(), "replication lane limit change")
			}
			newLimit := 0
			if limit <= 0 {
				newLimit = 1
			}
			config.ReplicationStreamSenderMaxLanes = func() int { return newLimit }
			require.ErrorContains(t, sender.recvEventLoop(), "replication lane config change")
		})
	}
}
