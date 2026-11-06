package replication

import (
	"context"
	"sync"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *laneStreamSenderSuite) TestRestoreFailureReplaysBeforeNewLaneHandoff() {
	s.streamSender.isTieredStackEnabled = true
	reader := s.laneTestReaderState(500, 500, 900)
	reader.ReplicationLaneDefaultCursor = shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(950))
	reader.Lanes = []*persistencespb.QueueReaderLane{{
		LogicalKey: namespaceLaneKey("ns-a"),
		Scope:      reader.Scopes[1], // A universal predicate cannot be restored as a lane.
	}}
	s.expectPersistedReaderState(reader)
	s.streamSender.initLaneRegistry()
	s.True(s.streamSender.laneRestoreIncomplete)
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	key := namespaceLaneKey("ns-a")
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{{Key: key, Filter: namespaceLaneFilter("ns-a")}}, nil)
	policy.EXPECT().Limiter(key).Return(nil)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000))
	ack := syncReplicationState(500, 500, 900)
	ack.MaxLanes = 8
	s.NoError(s.streamSender.reconcileLanes(ack, true))
	lane, ok := s.streamSender.laneRegistry.SnapshotByKey(key)
	s.True(ok)
	_, acquired := s.streamSender.laneRegistry.Acquire(lane.id)
	s.False(acquired, "the new lane must wait for conservative replay")
	s.Equal(int64(500), s.streamSender.laneCatchupBeginWatermark(1000))

	s.expectLaneTestNamespaces("ns-a")
	task := s.laneTestTask("ns-a", 600, enumsspb.TASK_PRIORITY_HIGH)
	s.expectReplicationTasksIter(500, 1000, task)
	replicated := &replicationspb.ReplicationTask{
		SourceTaskId:   600,
		VisibilityTime: timestamppb.New(time.Unix(0, 600)),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	s.taskConverter.EXPECT().Convert(task, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow).Return(replicated, nil)
	s.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_HIGH).Return(nil)
	var sent []*replicationspb.ReplicationTask
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(response *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Nil(response.GetMessages().GetLaneInfo())
		sent = append(sent, response.GetMessages().GetReplicationTasks()...)
		return nil
	}).Times(2)
	s.NoError(s.streamSender.sendDefaultTasks(enumsspb.TASK_PRIORITY_HIGH, 500, 1000))
	s.Equal([]*replicationspb.ReplicationTask{replicated}, sent)
	current, acquired := s.streamSender.laneRegistry.Acquire(lane.id)
	s.True(acquired)
	s.Equal(int64(1000), current.cursor)
	s.streamSender.laneRegistry.Release(lane.id)
}

func (s *streamSenderSuite) TestLegacyResumeCoversFiniteRetirement() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(int64(s.clientShardKey.ClusterID), s.clientShardKey.ShardID)
	scope := laneScope(namespaceLaneFilter("ns-a"), 150)
	scope.Range.ExclusiveMax = tasks.NewImmediateKey(200)
	state := &persistencespb.QueueReaderState{
		Lanes:                        []*persistencespb.QueueReaderLane{{LogicalKey: "a", Scope: queues.ToPersistenceScope(scope)}},
		ReplicationLaneDefaultCursor: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(950)),
	}
	for _, floor := range []int64{150, 150, 900} {
		state.Scopes = append(state.Scopes, queues.ToPersistenceScope(queues.NewScope(
			queues.NewRange(tasks.NewImmediateKey(floor), tasks.NewImmediateKey(1000)),
			predicates.Universal[tasks.Task](),
		)))
	}
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{readerID: state},
	}, true).AnyTimes()
	s.streamSender.isTieredStackEnabled = true
	for _, tc := range []struct {
		name        string
		readerGroup *replicationReaderGroup
	}{
		{name: "ReaderGroupDisabled"},
		{name: "ReaderGroupEnabled", readerGroup: newReplicationReaderGroup(s.shardContext, s.clientShardKey, true, log.NewNoopLogger())},
	} {
		s.Run(tc.name, func() {
			s.streamSender.readerGroup = tc.readerGroup
			s.Equal(int64(150), s.streamSender.catchupBeginWatermark(enumsspb.TASK_PRIORITY_HIGH, 1000))
			s.Equal(int64(900), s.streamSender.catchupBeginWatermark(enumsspb.TASK_PRIORITY_LOW, 1000))
		})
	}
}

func (s *laneStreamSenderSuite) TestRetiringLaneSendsOnlyItsFinitePrefix() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	lane, _, err := registry.Create("a", namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	_, acquired := registry.AcquireDefault(200)
	s.True(acquired)
	registry.ReleaseDefault(200, true)
	s.True(registry.RequestRetirement("a"))
	current, acquired := registry.Acquire(lane.id)
	s.True(acquired)
	s.streamSender.laneRegistry = registry
	// Continued writes advance the shard tip, but this lane must stop at 200.
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000))
	s.expectReplicationTasksIter(100, 200)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(response *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(lane.id, response.GetMessages().GetLaneInfo().GetLaneId())
		s.Equal(int64(200), response.GetMessages().GetExclusiveHighWatermark())
		return nil
	})
	s.NoError(s.streamSender.sendLaneRange(current))
	registry.Release(lane.id)
	current, _ = registry.SnapshotByID(lane.id)
	s.Equal(int64(200), current.cursor)
}

func (s *laneStreamSenderSuite) TestRestoredEmptyRetirementPrimesItsNewReceiverTracker() {
	scope := laneScope(namespaceLaneFilter("ns-a"), 200)
	scope.Range.ExclusiveMax = tasks.NewImmediateKey(200)
	registry, err := newSenderLaneRegistry(500, []*persistencespb.QueueReaderLane{{LogicalKey: "a", Scope: queues.ToPersistenceScope(scope)}})
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	lane := registry.Snapshots()[0]
	current, acquired := registry.Acquire(lane.id)
	s.True(acquired)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000))
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(response *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(lane.id, response.GetMessages().GetLaneInfo().GetLaneId())
		s.False(response.GetMessages().GetLaneInfo().GetRetire())
		s.Empty(response.GetMessages().GetReplicationTasks())
		s.Equal(int64(200), response.GetMessages().GetExclusiveHighWatermark())
		return nil
	})
	s.NoError(s.streamSender.sendLaneRange(current))
	registry.Release(lane.id)
	s.Empty(registry.ReadyRetirements())
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 200}})
	s.Len(registry.ReadyRetirements(), 1)
}

func (s *laneStreamSenderSuite) TestReconcileRecreatesKeyAfterRetirementCompletes() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	key := namespaceLaneKey("ns-a")
	lane, _, err := registry.Create(key, namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	s.True(registry.RequestRetirement(key))
	s.streamSender.laneRegistry = registry
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{{Key: key, Filter: namespaceLaneFilter("ns-a")}}, nil).Times(2)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000)).Times(2)
	ack := syncReplicationState(100, 100, 100)
	ack.MaxLanes = 8
	s.NoError(s.streamSender.reconcileLanes(ack, false))
	s.False(s.streamSender.laneShouldBeRateLimited(key))
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 100}})
	s.server.EXPECT().Send(gomock.Any()).Return(nil)
	policy.EXPECT().LaneRetired(key)
	s.NoError(s.streamSender.sendReadyLaneRetirements())
	limiter := &fakeLaneLimiter{}
	policy.EXPECT().Limiter(key).Return(limiter)
	s.NoError(s.streamSender.reconcileLanes(ack, false))
	current, ok := registry.SnapshotByKey(key)
	s.True(ok)
	s.NotEqual(lane.id, current.id)
	s.False(current.retiring)
	s.Same(limiter, s.streamSender.laneLimiter(key))
}

func (s *laneStreamSenderSuite) TestReconcileInstallsLimiterBeforePublishingLane() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	key := namespaceLaneKey("ns-a")
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{{Key: key, Filter: namespaceLaneFilter("ns-a")}}, nil)
	limiter := &fakeLaneLimiter{}
	policy.EXPECT().Limiter(key).DoAndReturn(func(string) LaneLimiter {
		_, exists := registry.SnapshotByKey(key)
		s.False(exists, "a send loop must not see the lane before its limiter exists")
		return limiter
	})
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000))
	ack := syncReplicationState(100, 100, 100)
	ack.MaxLanes = 8
	s.NoError(s.streamSender.reconcileLanes(ack, false))
	s.Len(registry.Snapshots(), 1)
	s.Same(limiter, s.streamSender.laneLimiter(key))
}

func (s *laneStreamSenderSuite) TestLaneSignalsPreserveRefusalTimeAndRetirementIntent() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	_, _, err = registry.Create("a", namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	s.True(registry.RequestRetirement("a"))
	s.streamSender.laneRegistry = registry
	refused := time.Unix(100, 0)
	s.streamSender.markLaneRefused("a", refused)
	s.timeSource.Update(refused.Add(time.Hour))
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000))
	signals := s.streamSender.laneSignals(syncReplicationState(100, 100, 100))
	s.Len(signals.Lanes, 1)
	s.Equal(refused, signals.Lanes[0].LastRefused)
	s.True(signals.Lanes[0].Retiring)
}

func (s *laneStreamSenderSuite) TestStopWithHangingCloseAllowsStreamReplacement() {
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	s.streamSender.status = common.DaemonStatusStarted
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	closed := make(chan struct{}, 1)
	policy.EXPECT().Close().Do(func() {
		entered <- struct{}{}
		<-release
		closed <- struct{}{}
	})
	s.T().Cleanup(func() {
		close(release)
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			s.T().Error("policy Close did not exit after it was released")
		}
	})
	key := s.streamSender.Key()
	monitor := &StreamReceiverMonitorImpl{inboundStreams: map[ClusterShardKeyPair]StreamSender{key: s.streamSender}}
	replacement := NewMockStreamSender(s.controller)
	replacement.EXPECT().Key().Return(key)
	replaced := make(chan struct{}, 1)
	go func() {
		monitor.RegisterInboundStream(replacement)
		replaced <- struct{}{}
	}()
	await.Rcv(s.T(), entered)
	select {
	case <-replaced:
	case <-time.After(5 * time.Second):
		s.T().Fatal("stream replacement waited for a hanging policy Close")
	}
	s.True(s.streamSender.shutdownChan.IsShutdown())
	s.Same(replacement, monitor.inboundStreams[key])
}

func (s *laneStreamSenderSuite) TestRetirementSendDoesNotBlockAckProcessing() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	lane, _, err := registry.Create("a", namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 100}})
	s.True(registry.RequestRetirement("a"))
	s.streamSender.laneRegistry = registry
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	policy.EXPECT().Evaluate(gomock.Any()).Return(nil, nil)
	policy.EXPECT().LaneRetired("a")
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(1000))
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(gomock.Any())
	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(gomock.Any(), gomock.Any()).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	sent := make(chan error, 1)
	acked := make(chan error, 1)
	ackFinished := false
	sendFinished := false
	var releaseOnce sync.Once
	releaseSend := func() { releaseOnce.Do(func() { close(release) }) }
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(*historyservice.StreamWorkflowReplicationMessagesResponse) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	}).AnyTimes()
	s.T().Cleanup(func() {
		releaseSend()
		if !sendFinished {
			select {
			case <-sent:
			case <-time.After(5 * time.Second):
				s.T().Error("retirement send did not exit")
			}
		}
		if !ackFinished {
			select {
			case <-acked:
			case <-time.After(5 * time.Second):
				s.T().Error("ACK processing did not exit")
			}
		}
	})
	go func() { sent <- s.streamSender.sendReadyLaneRetirements() }()
	await.Rcv(s.T(), entered)
	ack := syncReplicationState(100, 100, 100)
	ack.LaneProtocolVersion = replicationLaneProtocolVersion
	ack.MaxLanes = 8
	go func() { acked <- s.streamSender.recvLaneSyncReplicationState(ack) }()
	ctx, cancel := context.WithTimeout(s.T().Context(), 5*time.Second)
	defer cancel()
	select {
	case err := <-acked:
		ackFinished = true
		s.NoError(err)
	case <-ctx.Done():
		s.T().Fatal("ACK processing waited for a blocked retirement send")
	}
	releaseSend()
	s.Require().NoError(await.Rcv(s.T(), sent))
	sendFinished = true
}
