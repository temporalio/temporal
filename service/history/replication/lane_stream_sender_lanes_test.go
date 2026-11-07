package replication

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// laneTestTask returns a task whose workflow maps to the stream's target shard.
func (s *laneStreamSenderSuite) laneTestTask(namespaceID string, taskID int64, priority enumsspb.TaskPriority) *tasks.SyncWorkflowStateTask {
	workflowID := "wf"
	for i := 0; common.WorkflowIDToHistoryShard(namespaceID, workflowID, s.streamSender.clientClusterShardCount) != s.clientShardKey.ShardID; i++ {
		workflowID = fmt.Sprintf("wf-%d", i)
	}
	return &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{NamespaceID: namespaceID, WorkflowID: workflowID},
		TaskID:      taskID,
		Priority:    priority,
	}
}

func (s *laneStreamSenderSuite) expectLaneTestNamespaces(namespaceIDs ...string) {
	registry := namespace.NewMockRegistry(s.controller)
	for _, namespaceID := range namespaceIDs {
		registry.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewGlobalNamespaceForTest(
			nil, nil, &persistencespb.NamespaceReplicationConfig{
				Clusters: []string{"source_cluster", "target_cluster"},
			}, 100), nil).AnyTimes()
		registry.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name(namespaceID), nil).AnyTimes()
	}
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
}

func (s *laneStreamSenderSuite) expectReplicationTasksIter(begin, end int64, items ...tasks.Task) {
	s.historyEngine.EXPECT().GetReplicationTasksIter(gomock.Any(), string(s.clientShardKey.ClusterID), begin, end).Return(
		collection.NewPagingIterator[tasks.Task](func([]byte) ([]tasks.Task, []byte, error) {
			return items, nil, nil
		}), nil)
}

func (s *laneStreamSenderSuite) laneTestReaderState(overall, high, low int64) *persistencespb.QueueReaderState {
	scope := func(inclusiveMin int64) *persistencespb.QueueSliceScope {
		return &persistencespb.QueueSliceScope{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(inclusiveMin)),
				ExclusiveMax: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(math.MaxInt64)),
			},
			Predicate: &persistencespb.Predicate{
				PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
				Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
			},
		}
	}
	return &persistencespb.QueueReaderState{Scopes: []*persistencespb.QueueSliceScope{scope(overall), scope(high), scope(low)}}
}

func (s *laneStreamSenderSuite) expectPersistedReaderState(readerState *persistencespb.QueueReaderState) {
	readerID := shard.ReplicationReaderIDFromClusterShardID(int64(s.clientShardKey.ClusterID), s.clientShardKey.ShardID)
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{readerID: readerState},
	}, true).AnyTimes()
}

func (s *laneStreamSenderSuite) TestSendTasks_LaneTarget() {
	s.streamSender.isTieredStackEnabled = true
	s.expectLaneTestNamespaces("ns-a", "ns-b")
	laneTask := s.laneTestTask("ns-a", 101, enumsspb.TASK_PRIORITY_HIGH)
	s.expectReplicationTasksIter(100, 200,
		laneTask,
		s.laneTestTask("ns-b", 102, enumsspb.TASK_PRIORITY_HIGH),
		s.laneTestTask("ns-a", 103, enumsspb.TASK_PRIORITY_LOW),
	)
	replicationTask := &replicationspb.ReplicationTask{
		SourceTaskId:   101,
		VisibilityTime: timestamppb.New(time.Unix(0, 101)),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	s.taskConverter.EXPECT().Convert(laneTask, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow).Return(replicationTask, nil)
	s.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_HIGH).Return(nil)
	laneInfo := &replicationspb.ReplicationLaneInfo{LaneId: "lane-a"}
	gomock.InOrder(
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     102,
					ExclusiveHighWatermarkTime: replicationTask.VisibilityTime,
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
					LaneInfo:                   laneInfo,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(int64(200), resp.GetMessages().GetExclusiveHighWatermark())
			s.Equal(laneInfo, resp.GetMessages().GetLaneInfo())
			return nil
		}),
	)
	rateLimited := 0
	laneScope := laneScope(namespaceLaneFilter("ns-a"), 100)

	err := s.streamSender.sendTasks(enumsspb.TASK_PRIORITY_HIGH, 100, 200, &laneSendTarget{
		laneInfo: laneInfo,
		accept:   laneScope.Contains,
		rateLimit: func(context.Context) error {
			rateLimited++
			return nil
		},
	})
	s.NoError(err)
	s.Equal(1, rateLimited)
}

// fakeLaneLimiter refuses its first refusals reservations with retryAfter, then allows every
// reservation. onReserve, when set, runs on every call.
type fakeLaneLimiter struct {
	refusals   int
	retryAfter time.Duration
	onReserve  func()
	calls      int
}

func (l *fakeLaneLimiter) Reserve(time.Time) (bool, time.Duration) {
	l.calls++
	if l.onReserve != nil {
		l.onReserve()
	}
	if l.calls <= l.refusals {
		return false, l.retryAfter
	}
	return true, 0
}

// requestLane makes logicalKey the only lane in the policy's latest answer, paced by limiter.
func (s *laneStreamSenderSuite) requestLane(logicalKey string, limiter LaneLimiter) {
	if s.streamSender.laneRegistry == nil {
		registry, err := newSenderLaneRegistry(0, nil)
		s.NoError(err)
		s.streamSender.laneRegistry = registry
	}
	s.streamSender.requestedLanes = map[string]struct{}{logicalKey: {}}
	s.streamSender.laneLimiters[logicalKey] = limiter
}

func (s *laneStreamSenderSuite) TestPaceLane_WaitsWhileReceiverPausesLane() {
	logicalKey := namespaceLaneKey("ns-a")
	limiter := &fakeLaneLimiter{}
	s.requestLane(logicalKey, limiter)
	lane, _, err := s.streamSender.laneRegistry.Create(logicalKey, namespaceLaneFilter("ns-a"), 0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE},
	})
	// A handover lifts the lane's rate limit but not the receiver's pause.
	s.streamSender.handoverInProgress.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.streamSender.paceLane(ctx, logicalKey) }()
	cancel()
	s.ErrorIs(<-done, context.Canceled)

	s.streamSender.laneRegistry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		lane.id: {FlowControlCommand: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME},
	})
	s.NoError(s.streamSender.paceLane(context.Background(), logicalKey))
	s.Zero(limiter.calls)
}

func (s *laneStreamSenderSuite) TestPaceLane_StreamEnded() {
	logicalKey := namespaceLaneKey("ns-a")
	limiter := &fakeLaneLimiter{refusals: 1, retryAfter: time.Hour}
	s.requestLane(logicalKey, limiter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.streamSender.paceLane(ctx, logicalKey)
	s.ErrorIs(err, context.Canceled)
	// sendTasks wraps the error before deciding whether to retry the send.
	s.False(isRetryableError(fmt.Errorf("lane_rate_limit: %w", err)))
}

func (s *laneStreamSenderSuite) TestPaceLane_RetriesUntilAllowed() {
	logicalKey := namespaceLaneKey("ns-a")
	limiter := &fakeLaneLimiter{refusals: 2}
	s.requestLane(logicalKey, limiter)

	s.NoError(s.streamSender.paceLane(context.Background(), logicalKey))
	s.Equal(3, limiter.calls)
	// A refusal marks the lane as paced for the policy.
	s.Contains(s.streamSender.laneRefusedAt, logicalKey)
}

func (s *laneStreamSenderSuite) TestPaceLane_NotPaced() {
	logicalKey := namespaceLaneKey("ns-a")
	limiter := &fakeLaneLimiter{refusals: 1, retryAfter: time.Hour}
	s.requestLane(logicalKey, limiter)
	s.streamSender.requestedLanes = nil

	// A lane the policy no longer requests catches up without its limiter.
	s.NoError(s.streamSender.paceLane(context.Background(), logicalKey))

	// So does every lane while a handover is in progress on the shard.
	s.requestLane(logicalKey, limiter)
	s.streamSender.handoverInProgress.Store(true)
	s.NoError(s.streamSender.paceLane(context.Background(), logicalKey))
	s.Zero(limiter.calls)
}

func (s *laneStreamSenderSuite) TestPaceLane_HandoverWhileRefused() {
	logicalKey := namespaceLaneKey("ns-a")
	// The limiter asks for an hour, and a handover starts meanwhile. The lane re-checks within
	// laneMaxRetryAfter and sends without asking the limiter again.
	limiter := &fakeLaneLimiter{refusals: 1, retryAfter: time.Hour}
	limiter.onReserve = func() { s.streamSender.handoverInProgress.Store(true) }
	s.requestLane(logicalKey, limiter)

	s.NoError(s.streamSender.paceLane(context.Background(), logicalKey))
	s.Equal(1, limiter.calls)
}

func (s *laneStreamSenderSuite) TestSendDefaultTasks_SkipsLaneOwnedTasks() {
	s.streamSender.isTieredStackEnabled = true
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	_, _, err = registry.Create(namespaceLaneKey("ns-a"), namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	s.expectLaneTestNamespaces("ns-a", "ns-b")
	defaultTask := s.laneTestTask("ns-b", 102, enumsspb.TASK_PRIORITY_HIGH)
	s.expectReplicationTasksIter(100, 200,
		s.laneTestTask("ns-a", 101, enumsspb.TASK_PRIORITY_HIGH),
		defaultTask,
	)
	replicationTask := &replicationspb.ReplicationTask{
		SourceTaskId:   102,
		VisibilityTime: timestamppb.New(time.Unix(0, 102)),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	s.taskConverter.EXPECT().Convert(defaultTask, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_HIGH, locks.PriorityLow).Return(replicationTask, nil)
	s.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_HIGH).Return(nil)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Nil(resp.GetMessages().GetLaneInfo())
		return nil
	}).Times(2)

	err = s.streamSender.sendDefaultTasks(enumsspb.TASK_PRIORITY_HIGH, 100, 200)
	s.NoError(err)
	s.Equal(int64(200), registry.DefaultReservedCursor())
}

func (s *laneStreamSenderSuite) TestSendDefaultTasks_LowPriorityIgnoresLanes() {
	s.streamSender.isTieredStackEnabled = true
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	_, _, err = registry.Create(namespaceLaneKey("ns-a"), namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	s.expectLaneTestNamespaces("ns-a")
	lowTask := s.laneTestTask("ns-a", 101, enumsspb.TASK_PRIORITY_LOW)
	s.expectReplicationTasksIter(100, 200, lowTask)
	s.taskConverter.EXPECT().Convert(lowTask, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_LOW, locks.PriorityLow).Return(&replicationspb.ReplicationTask{
		SourceTaskId: 101,
		Priority:     enumsspb.TASK_PRIORITY_LOW,
	}, nil)
	s.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_LOW).Return(nil)
	s.server.EXPECT().Send(gomock.Any()).Return(nil).Times(2)

	s.NoError(s.streamSender.sendDefaultTasks(enumsspb.TASK_PRIORITY_LOW, 100, 200))
	// The LOW priority default lane does not reserve a HIGH priority default range.
	s.Equal(int64(100), registry.DefaultReservedCursor())
}

func (s *laneStreamSenderSuite) TestRecvLaneSyncReplicationState_WithoutLaneSupport() {
	s.streamSender.isTieredStackEnabled = true
	persistedLane := &persistencespb.QueueReaderLane{
		LogicalKey: namespaceLaneKey("ns-a"),
		Scope:      queues.ToPersistenceScope(laneScope(namespaceLaneFilter("ns-a"), 700)),
	}
	registry, err := newSenderLaneRegistry(950, []*persistencespb.QueueReaderLane{persistedLane})
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	// Without lane support, the stream never creates a policy.
	s.streamSender.lanePolicyFactory = NewMockLaneStreamPolicyFactory(s.controller)
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(gomock.Any())
	readerID := shard.ReplicationReaderIDFromClusterShardID(int64(s.clientShardKey.ClusterID), s.clientShardKey.ShardID)
	attr := syncReplicationState(700, 700, 900)
	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, gomock.Any()).DoAndReturn(
		func(_ int64, state *persistencespb.QueueReaderState) error {
			s.Empty(state.GetLanes())
			s.Nil(state.GetReplicationLaneDefaultCursor())
			s.Equal(int64(700), state.GetScopes()[laneReaderHighScopeIndex].GetRange().GetInclusiveMin().GetTaskId())
			return nil
		})
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, int64(699), gomock.Any()).Return(nil)

	s.NoError(s.streamSender.recvLaneSyncReplicationState(attr))
	s.False(s.streamSender.lanesConfirmed.Load())
	s.Empty(registry.Snapshots())
	select {
	case <-s.streamSender.initialLaneState:
	default:
		s.Fail("the first ack did not close initialLaneState")
	}
}

func (s *laneStreamSenderSuite) TestRecvLaneSyncReplicationState_ReconcilesLanes() {
	s.streamSender.isTieredStackEnabled = true
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	// The stream creates its policy on the first ack, once.
	factory := NewMockLaneStreamPolicyFactory(s.controller)
	s.streamSender.lanePolicyFactory = factory
	policy := NewMockLaneStreamPolicy(s.controller)
	factory.EXPECT().NewStreamPolicy(LaneStream{RemoteCluster: "target_cluster", ShardID: s.serverShardKey.ShardID}).Return(policy, nil)
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(gomock.Any()).AnyTimes()
	s.shardContext.EXPECT().GetReplicationStatus([]string{"target_cluster"}).Return(nil, nil, nil).AnyTimes()
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(200)).AnyTimes()
	readerID := shard.ReplicationReaderIDFromClusterShardID(int64(s.clientShardKey.ClusterID), s.clientShardKey.ShardID)
	logicalKey := namespaceLaneKey("ns-a")
	limiter := &fakeLaneLimiter{}

	// The first ack confirms lane support and creates the requested lane at the acked HIGH
	// position, with the limiter and the policy state from the policy.
	attr := syncReplicationState(80, 100, 90)
	attr.LaneProtocolVersion = replicationLaneProtocolVersion
	attr.ThrottleHighNamespaceIds = []string{"ns-a"}
	attr.MaxLanes = 7
	policy.EXPECT().Evaluate(gomock.Any()).DoAndReturn(func(signals LaneSignals) ([]DesiredLane, error) {
		s.Equal([]string{"ns-a"}, signals.ThrottledNamespaceIDs)
		s.Equal(7, signals.ReceiverMaxLanes)
		s.Equal(int64(200), signals.SourceHighWatermark)
		s.Equal(int64(100), signals.DefaultAcked)
		s.Empty(signals.Lanes)
		return []DesiredLane{{Key: logicalKey, Filter: namespaceLaneFilter("ns-a"), PolicyState: []byte("class=1")}}, nil
	})
	policy.EXPECT().Limiter(logicalKey).Return(limiter)
	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, gomock.Any()).DoAndReturn(
		func(_ int64, state *persistencespb.QueueReaderState) error {
			s.Len(state.GetLanes(), 1)
			s.Equal(logicalKey, state.GetLanes()[0].GetLogicalKey())
			s.Equal([]byte("class=1"), state.GetLanes()[0].GetPolicyState())
			s.Equal(int64(100), state.GetLanes()[0].GetScope().GetRange().GetInclusiveMin().GetTaskId())
			s.Equal(int64(80), state.GetScopes()[laneReaderOverallScopeIndex].GetRange().GetInclusiveMin().GetTaskId())
			s.Equal(int64(100), state.GetScopes()[laneReaderHighScopeIndex].GetRange().GetInclusiveMin().GetTaskId())
			return nil
		})
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, int64(99), gomock.Any()).Return(nil)
	s.NoError(s.streamSender.recvLaneSyncReplicationState(attr))
	s.True(s.streamSender.lanesConfirmed.Load())
	_, ok := registry.SnapshotByKey(logicalKey)
	s.True(ok)
	s.Same(limiter, s.streamSender.laneLimiter(logicalKey))
	s.True(s.streamSender.laneShouldBeRateLimited(logicalKey))

	// The policy sees the lane on the next ack. A lane that is no longer requested sends
	// without a rate limit until it can retire.
	policy.EXPECT().Evaluate(gomock.Any()).DoAndReturn(func(signals LaneSignals) ([]DesiredLane, error) {
		s.Equal([]LaneStatus{{
			Key:         logicalKey,
			Filter:      namespaceLaneFilter("ns-a"),
			SentCursor:  100,
			PolicyState: []byte("class=1"),
		}}, signals.Lanes)
		return nil, nil
	})
	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, gomock.Any()).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, gomock.Any(), gomock.Any()).Return(nil)
	s.NoError(s.streamSender.recvLaneSyncReplicationState(attr))
	s.False(s.streamSender.laneShouldBeRateLimited(logicalKey))
	_, ok = registry.SnapshotByKey(logicalKey)
	s.True(ok)
}

func (s *laneStreamSenderSuite) TestRecvLaneSyncReplicationState_LaneSupportChange() {
	s.streamSender.isTieredStackEnabled = true
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(gomock.Any()).AnyTimes()
	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(gomock.Any(), gomock.Any()).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.NoError(s.streamSender.recvLaneSyncReplicationState(syncReplicationState(100, 100, 100)))

	attr := syncReplicationState(100, 100, 100)
	attr.LaneProtocolVersion = replicationLaneProtocolVersion
	err = s.streamSender.recvLaneSyncReplicationState(attr)
	s.ErrorAs(err, new(*StreamError))
}

func (s *laneStreamSenderSuite) TestLaneCatchupBeginWatermark() {
	withLanes := s.laneTestReaderState(600, 700, 600)
	withLanes.Lanes = []*persistencespb.QueueReaderLane{{
		LogicalKey: namespaceLaneKey("ns-a"),
		Scope:      queues.ToPersistenceScope(laneScope(namespaceLaneFilter("ns-a"), 700)),
	}}
	withLanes.ReplicationLaneDefaultCursor = shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(950))
	for _, tc := range []struct {
		name              string
		readerState       *persistencespb.QueueReaderState
		lanesConfirmed    bool
		laneRestoreFailed bool
		expected          int64
	}{
		{name: "LanesContinue", readerState: withLanes, lanesConfirmed: true, expected: 950},
		{name: "ReceiverWithoutLaneSupport", readerState: withLanes, lanesConfirmed: false, expected: 700},
		{name: "LaneRestoreFailed", readerState: withLanes, lanesConfirmed: true, laneRestoreFailed: true, expected: 700},
		{name: "NoPersistedLanes", readerState: s.laneTestReaderState(600, 950, 600), lanesConfirmed: true, expected: 950},
	} {
		s.Run(tc.name, func() {
			s.expectPersistedReaderState(tc.readerState)
			s.streamSender.hasPersistedLanes = len(tc.readerState.GetLanes()) > 0
			s.streamSender.laneRecoveryFloor = 700
			s.streamSender.laneRestoreIncomplete = tc.laneRestoreFailed
			s.streamSender.lanesConfirmed.Store(tc.lanesConfirmed)
			s.Equal(tc.expected, s.streamSender.laneCatchupBeginWatermark(1000))
		})
	}
}

func (s *laneStreamSenderSuite) TestSendReadyLaneRetirements() {
	registry, err := newSenderLaneRegistry(100, nil)
	s.NoError(err)
	lane, _, err := registry.Create(namespaceLaneKey("ns-a"), namespaceLaneFilter("ns-a"), 100, nil)
	s.NoError(err)
	registry.AdvanceLaneCursor(lane.id, 150)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 150}})
	s.True(registry.RequestRetirement(lane.logicalKey))
	s.streamSender.laneRegistry = registry
	s.streamSender.laneLimiters[lane.logicalKey] = &fakeLaneLimiter{}
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(&replicationspb.ReplicationLaneInfo{LaneId: lane.id, Retire: true}, resp.GetMessages().GetLaneInfo())
		s.Equal(int64(150), resp.GetMessages().GetExclusiveHighWatermark())
		s.Equal(enumsspb.TASK_PRIORITY_HIGH, resp.GetMessages().GetPriority())
		return nil
	})
	policy.EXPECT().LaneRetired(lane.logicalKey)

	s.NoError(s.streamSender.sendReadyLaneRetirements())
	s.Empty(registry.Snapshots())
	s.Empty(s.streamSender.laneLimiters)
}

func (s *laneStreamSenderSuite) TestIsValid_SenderLanesEnabledChanged() {
	s.streamSender.status = common.DaemonStatusStarted
	s.streamSender.lanesEnabledSetting = false
	s.streamSender.config.ReplicationStreamSenderLanesEnabled = dynamicconfig.GetBoolPropertyFn(false)
	s.True(s.streamSender.IsValid())

	s.streamSender.config.ReplicationStreamSenderLanesEnabled = dynamicconfig.GetBoolPropertyFn(true)
	s.False(s.streamSender.IsValid())
}

func (s *laneStreamSenderSuite) TestReconcileLanes_HandoverLiftsRateLimits() {
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	logicalKey := namespaceLaneKey("ns-a")
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{{Key: logicalKey, Filter: namespaceLaneFilter("ns-a")}}, nil).Times(2)
	policy.EXPECT().Limiter(logicalKey).Return(&fakeLaneLimiter{})
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100)).AnyTimes()
	attr := syncReplicationState(100, 100, 100)
	attr.MaxLanes = 100

	s.shardContext.EXPECT().GetReplicationStatus([]string{"target_cluster"}).Return(nil, map[string]*historyservice.HandoverNamespaceInfo{"ns-b": {}}, nil)
	s.NoError(s.streamSender.reconcileLanes(attr, false))
	s.False(s.streamSender.laneShouldBeRateLimited(logicalKey))

	s.shardContext.EXPECT().GetReplicationStatus([]string{"target_cluster"}).Return(nil, nil, nil)
	s.NoError(s.streamSender.reconcileLanes(attr, false))
	s.True(s.streamSender.laneShouldBeRateLimited(logicalKey))
}

func (s *laneStreamSenderSuite) TestReconcileLanes_MaxLanesKeepsPolicyOrder() {
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	s.streamSender.config.ReplicationStreamSenderMaxLanes = dynamicconfig.GetIntPropertyFn(2)
	policy := NewMockLaneStreamPolicy(s.controller)
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{
		{Key: namespaceLaneKey("ns-c"), Filter: namespaceLaneFilter("ns-c")},
		{Key: namespaceLaneKey("ns-a"), Filter: namespaceLaneFilter("ns-a")},
		{Key: namespaceLaneKey("ns-b"), Filter: namespaceLaneFilter("ns-b")},
	}, nil)
	policy.EXPECT().Limiter(gomock.Any()).Return(&fakeLaneLimiter{}).Times(2)
	s.streamSender.lanePolicy = policy
	s.shardContext.EXPECT().GetReplicationStatus(gomock.Any()).Return(nil, nil, nil)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100))
	attr := syncReplicationState(100, 100, 100)
	attr.MaxLanes = 100

	s.NoError(s.streamSender.reconcileLanes(attr, false))
	var logicalKeys []string
	for _, lane := range registry.Snapshots() {
		logicalKeys = append(logicalKeys, lane.logicalKey)
	}
	s.ElementsMatch([]string{namespaceLaneKey("ns-c"), namespaceLaneKey("ns-a")}, logicalKeys)
}

func (s *laneStreamSenderSuite) TestReconcileLanes_PolicyErrorRetiresLanes() {
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	_, _, err = registry.Create(namespaceLaneKey("ns-a"), namespaceLaneFilter("ns-a"), 0, nil)
	s.NoError(err)
	s.streamSender.requestedLanes = map[string]struct{}{namespaceLaneKey("ns-a"): {}}
	policy := NewMockLaneStreamPolicy(s.controller)
	policy.EXPECT().Evaluate(gomock.Any()).Return(nil, errors.New("invalid config"))
	s.streamSender.lanePolicy = policy
	s.shardContext.EXPECT().GetReplicationStatus(gomock.Any()).Return(nil, nil, nil)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100))

	s.NoError(s.streamSender.reconcileLanes(syncReplicationState(100, 100, 100), false))
	s.False(s.streamSender.laneShouldBeRateLimited(namespaceLaneKey("ns-a")))
}

func (s *laneStreamSenderSuite) TestReconcileLanes_WithoutPolicy() {
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	_, _, err = registry.Create(namespaceLaneKey("ns-a"), namespaceLaneFilter("ns-a"), 0, nil)
	s.NoError(err)
	s.streamSender.requestedLanes = map[string]struct{}{namespaceLaneKey("ns-a"): {}}
	s.shardContext.EXPECT().GetReplicationStatus(gomock.Any()).Return(nil, nil, nil)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100))

	s.NoError(s.streamSender.reconcileLanes(syncReplicationState(100, 100, 100), false))
	s.False(s.streamSender.laneShouldBeRateLimited(namespaceLaneKey("ns-a")))
}

func (s *laneStreamSenderSuite) TestAdmitDesiredLanes() {
	nsLane := func(key string, namespaceIDs ...namespace.ID) DesiredLane {
		return DesiredLane{Key: key, Filter: LaneFilter{NamespaceIDs: namespaceIDs}}
	}
	tooMany := make([]namespace.ID, laneFilterMaxNamespaces+1)
	for i := range tooMany {
		tooMany[i] = namespace.ID(fmt.Sprintf("ns-%d", i))
	}
	for _, tc := range []struct {
		name             string
		existing         []DesiredLane
		desired          []DesiredLane
		receiverMaxLanes int // 100 when zero
		expected         []DesiredLane
	}{
		{name: "EmptyKey", desired: []DesiredLane{nsLane("", "ns-a")}},
		{name: "EmptyFilter", desired: []DesiredLane{nsLane("a")}},
		{name: "TooManyNamespaces", desired: []DesiredLane{nsLane("a", tooMany...)}},
		{name: "EmptyNamespaceID", desired: []DesiredLane{nsLane("a", "")}},
		{name: "RepeatedNamespaceID", desired: []DesiredLane{nsLane("a", "ns-a", "ns-a")}},
		{
			name:    "PolicyStateTooLarge",
			desired: []DesiredLane{{Key: "a", Filter: namespaceLaneFilter("ns-a"), PolicyState: make([]byte, lanePolicyStateMaxBytes+1)}},
		},
		{
			name:     "PolicyStateAtLimit",
			desired:  []DesiredLane{{Key: "a", Filter: namespaceLaneFilter("ns-a"), PolicyState: make([]byte, lanePolicyStateMaxBytes)}},
			expected: []DesiredLane{{Key: "a", Filter: namespaceLaneFilter("ns-a"), PolicyState: make([]byte, lanePolicyStateMaxBytes)}},
		},
		{name: "SeveralNamespaces", desired: []DesiredLane{nsLane("a", "ns-a", "ns-b")}, expected: []DesiredLane{nsLane("a", "ns-a", "ns-b")}},
		{name: "RepeatedKey", desired: []DesiredLane{nsLane("a", "ns-a"), nsLane("a", "ns-b")}, expected: []DesiredLane{nsLane("a", "ns-a")}},
		{name: "OverlapsEarlierLane", desired: []DesiredLane{nsLane("a", "ns-a"), nsLane("b", "ns-b", "ns-a")}, expected: []DesiredLane{nsLane("a", "ns-a")}},
		{name: "OverlapsRetiringLane", existing: []DesiredLane{nsLane("old", "ns-a")}, desired: []DesiredLane{nsLane("new", "ns-a")}},
		{
			name:     "ExistingLaneKeepsItsFilter",
			existing: []DesiredLane{nsLane("a", "ns-a")},
			desired:  []DesiredLane{nsLane("a", "ns-b"), nsLane("b", "ns-a")},
			expected: []DesiredLane{nsLane("a", "ns-b")},
		},
		{
			// The receiver counts the retiring lane too, so only one new lane fits.
			name:             "ReceiverLaneLimit",
			existing:         []DesiredLane{nsLane("retiring", "ns-r")},
			desired:          []DesiredLane{nsLane("a", "ns-a"), nsLane("b", "ns-b")},
			receiverMaxLanes: 2,
			expected:         []DesiredLane{nsLane("a", "ns-a")},
		},
		{
			name:             "ReceiverLaneLimitKeepsExistingLanes",
			existing:         []DesiredLane{nsLane("a", "ns-a"), nsLane("b", "ns-b")},
			desired:          []DesiredLane{nsLane("c", "ns-c"), nsLane("a", "ns-a"), nsLane("b", "ns-b")},
			receiverMaxLanes: 2,
			expected:         []DesiredLane{nsLane("a", "ns-a"), nsLane("b", "ns-b")},
		},
	} {
		s.Run(tc.name, func() {
			registry, err := newSenderLaneRegistry(0, nil)
			s.NoError(err)
			for _, lane := range tc.existing {
				_, _, err := registry.Create(lane.Key, lane.Filter, 0, nil)
				s.NoError(err)
			}
			s.streamSender.laneRegistry = registry
			s.Equal(tc.expected, s.streamSender.admitDesiredLanes(tc.desired, cmp.Or(tc.receiverMaxLanes, 100)))
		})
	}
}

func (s *laneStreamSenderSuite) TestReconcileLanes_FirstAckDropsRestoredLanesOverLimit() {
	var persisted []*persistencespb.QueueReaderLane
	for _, namespaceID := range []namespace.ID{"ns-a", "ns-b", "ns-c"} {
		persisted = append(persisted, &persistencespb.QueueReaderLane{
			LogicalKey: namespaceLaneKey(namespaceID),
			Scope:      queues.ToPersistenceScope(laneScope(namespaceLaneFilter(namespaceID), 100)),
		})
	}
	registry, err := newSenderLaneRegistry(100, persisted)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	policy.EXPECT().Evaluate(gomock.Any()).Return([]DesiredLane{
		{Key: namespaceLaneKey("ns-c"), Filter: namespaceLaneFilter("ns-c")},
		{Key: namespaceLaneKey("ns-a"), Filter: namespaceLaneFilter("ns-a")},
	}, nil)
	policy.EXPECT().Limiter(gomock.Any()).Return(&fakeLaneLimiter{}).Times(2)
	s.shardContext.EXPECT().GetReplicationStatus(gomock.Any()).Return(nil, nil, nil)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100))
	attr := syncReplicationState(100, 100, 100)
	attr.MaxLanes = 2

	// The receiver accepts two lanes: the policy's two are kept, and ns-b goes back to the
	// default lane, which re-sends its tasks from the recovery floor.
	s.NoError(s.streamSender.reconcileLanes(attr, true))
	var logicalKeys []string
	for _, lane := range registry.Snapshots() {
		logicalKeys = append(logicalKeys, lane.logicalKey)
	}
	s.ElementsMatch([]string{namespaceLaneKey("ns-c"), namespaceLaneKey("ns-a")}, logicalKeys)
	s.True(s.streamSender.laneRestoreIncomplete)
}

func (s *laneStreamSenderSuite) TestEvaluateLanes_ErrorThenRecovery() {
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	good := []DesiredLane{{Key: namespaceLaneKey("ns-a"), Filter: namespaceLaneFilter("ns-a")}}

	policy.EXPECT().Evaluate(gomock.Any()).Return(good, nil)
	s.Equal(good, s.streamSender.evaluateLanes(LaneSignals{}))
	// An error is an answer with no lanes, and the stream asks again on the next ack.
	policy.EXPECT().Evaluate(gomock.Any()).Return(good, errors.New("unavailable"))
	s.Nil(s.streamSender.evaluateLanes(LaneSignals{}))
	policy.EXPECT().Evaluate(gomock.Any()).Return(good, nil)
	s.Equal(good, s.streamSender.evaluateLanes(LaneSignals{}))
	s.False(s.streamSender.lanePolicyDisabled.Load())
}

func (s *laneStreamSenderSuite) TestLanePolicyPanic() {
	logicalKey := namespaceLaneKey("ns-a")
	for _, tc := range []struct {
		name   string
		expect func(policy *MockLaneStreamPolicy, limiter *fakeLaneLimiter)
		call   func()
	}{
		{
			name: "Evaluate",
			expect: func(policy *MockLaneStreamPolicy, _ *fakeLaneLimiter) {
				policy.EXPECT().Evaluate(gomock.Any()).Do(func(LaneSignals) { panic("evaluate") })
			},
			call: func() { s.Nil(s.streamSender.evaluateLanes(LaneSignals{})) },
		},
		{
			name: "Limiter",
			expect: func(policy *MockLaneStreamPolicy, _ *fakeLaneLimiter) {
				policy.EXPECT().Limiter(namespaceLaneKey("ns-b")).Do(func(string) { panic("limiter") })
			},
			call: func() { s.streamSender.setRequestedLanes(map[string]struct{}{namespaceLaneKey("ns-b"): {}}) },
		},
		{
			name: "LaneRetired",
			expect: func(policy *MockLaneStreamPolicy, _ *fakeLaneLimiter) {
				policy.EXPECT().LaneRetired(logicalKey).Do(func(string) { panic("lane retired") })
			},
			call: func() { s.streamSender.laneRetired(logicalKey) },
		},
		{
			name: "Reserve",
			expect: func(_ *MockLaneStreamPolicy, limiter *fakeLaneLimiter) {
				limiter.onReserve = func() { panic("reserve") }
			},
			// The lane sends without its limiter.
			call: func() { s.NoError(s.streamSender.paceLane(context.Background(), logicalKey)) },
		},
	} {
		s.Run(tc.name, func() {
			policy := NewMockLaneStreamPolicy(s.controller)
			s.streamSender.lanePolicy = policy
			s.streamSender.lanePolicyClosed = false
			s.streamSender.lanePolicyStopped.Store(false)
			s.streamSender.lanePolicyDisabled.Store(false)
			limiter := &fakeLaneLimiter{refusals: 1, retryAfter: time.Hour}
			s.requestLane(logicalKey, limiter)
			tc.expect(policy, limiter)

			tc.call()

			// The stream calls nothing more on the policy or its limiters except Close.
			s.True(s.streamSender.lanePolicyDisabled.Load())
			s.Nil(s.streamSender.evaluateLanes(LaneSignals{}))
			s.Nil(s.streamSender.laneLimiter(logicalKey))
			s.streamSender.setRequestedLanes(map[string]struct{}{namespaceLaneKey("ns-c"): {}})
			s.streamSender.laneRetired(logicalKey)
			closed := make(chan struct{}, 1)
			policy.EXPECT().Close().Do(func() { closed <- struct{}{} })
			s.streamSender.closeLanePolicy()
			await.Rcv(s.T(), closed)
		})
	}
}

func (s *laneStreamSenderSuite) TestStartLanePolicy_FactoryPanic() {
	factory := NewMockLaneStreamPolicyFactory(s.controller)
	s.streamSender.lanePolicyFactory = factory
	factory.EXPECT().NewStreamPolicy(gomock.Any()).Do(func(LaneStream) { panic("factory") })

	s.streamSender.startLanePolicy()
	s.Nil(s.streamSender.lanePolicy)
	s.Nil(s.streamSender.evaluateLanes(LaneSignals{}))
}

func (s *laneStreamSenderSuite) TestCloseLanePolicy_PanicIsRecovered() {
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	closed := make(chan struct{}, 1)
	policy.EXPECT().Close().Do(func() {
		defer func() { closed <- struct{}{} }()
		panic("close")
	})

	s.streamSender.closeLanePolicy()
	s.True(s.streamSender.lanePolicyClosed)
	await.Rcv(s.T(), closed)
}

func (s *laneStreamSenderSuite) TestCloseLanePolicy_DoesNotWaitForRunningCall() {
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	evaluating := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan struct{})
	policy.EXPECT().Evaluate(gomock.Any()).DoAndReturn(func(LaneSignals) ([]DesiredLane, error) {
		close(evaluating)
		<-release
		return nil, nil
	})
	evaluated := make(chan struct{})
	go func() {
		defer close(evaluated)
		s.streamSender.evaluateLanes(LaneSignals{})
	}()
	<-evaluating

	// Stopping returns while Evaluate is still running, without closing the policy.
	s.streamSender.closeLanePolicy()
	select {
	case <-closed:
		s.Fail("the policy was closed while Evaluate was running")
	default:
	}

	// The running call closes the policy when it returns.
	policy.EXPECT().Close().Do(func() { close(closed) })
	close(release)
	<-evaluated
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		s.Fail("the policy was not closed after Evaluate returned")
	}
	// The stream calls nothing on the policy after Close.
	s.Nil(s.streamSender.evaluateLanes(LaneSignals{}))
}

func TestLaneRetryDelay(t *testing.T) {
	require.Equal(t, laneMinRetryAfter, laneRetryDelay(0))
	require.Equal(t, laneMinRetryAfter, laneRetryDelay(-time.Second))
	require.Equal(t, 200*time.Millisecond, laneRetryDelay(200*time.Millisecond))
	require.Equal(t, laneMaxRetryAfter, laneRetryDelay(time.Hour))
}

func (s *laneStreamSenderSuite) TestLanePolicyLifecycle() {
	factory := NewMockLaneStreamPolicyFactory(s.controller)
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicyFactory = factory
	factory.EXPECT().NewStreamPolicy(LaneStream{RemoteCluster: "target_cluster", ShardID: s.serverShardKey.ShardID}).Return(policy, nil)
	s.streamSender.startLanePolicy()

	// Stopping the stream closes the policy once, and no method is called after that.
	closed := make(chan struct{}, 1)
	policy.EXPECT().Close().Do(func() { closed <- struct{}{} })
	s.streamSender.closeLanePolicy()
	s.streamSender.closeLanePolicy()
	s.streamSender.laneRetired(namespaceLaneKey("ns-a"))
	await.Rcv(s.T(), closed)
}

func (s *laneStreamSenderSuite) TestStartLanePolicy_FactoryError() {
	factory := NewMockLaneStreamPolicyFactory(s.controller)
	s.streamSender.lanePolicyFactory = factory
	factory.EXPECT().NewStreamPolicy(gomock.Any()).Return(nil, errors.New("unavailable"))

	s.streamSender.startLanePolicy()
	s.Nil(s.streamSender.lanePolicy)
}

func (s *laneStreamSenderSuite) TestRecordLaneLag() {
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	behind, _, err := registry.Create(namespaceLaneKey("ns-behind"), namespaceLaneFilter("ns-behind"), 0, nil)
	s.NoError(err)
	current, _, err := registry.Create(namespaceLaneKey("ns-current"), namespaceLaneFilter("ns-current"), 0, nil)
	s.NoError(err)
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{
		behind.id:  {InclusiveLowWatermark: 1, InclusiveLowWatermarkTime: timestamppb.New(time.Now().Add(-time.Hour))},
		current.id: {InclusiveLowWatermark: 1, InclusiveLowWatermarkTime: timestamppb.New(time.Now())},
	})
	s.streamSender.laneRegistry = registry
	s.streamSender.config.ReplicationLaneMaxLag = dynamicconfig.GetDurationPropertyFn(30 * time.Minute)
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)
	s.streamSender.metrics = captureHandler
	// Only the lane over the limit is reported, with the shard's newest task for comparison.
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(500))

	s.streamSender.recordLaneLag()
	s.Len(capture.Snapshot()[metrics.ReplicationLaneLag.Name()], 2)
	exceeded := capture.Snapshot()[metrics.ReplicationLaneMaxLagExceeded.Name()]
	s.Len(exceeded, 1)
	s.Equal(namespaceLaneKey("ns-behind"), exceeded[0].Tags["replication_lane"])
}

func (s *laneStreamSenderSuite) TestRecordLaneLagBoundaries() {
	for _, tc := range []struct {
		name          string
		sourceHigh    int64
		retirementEnd int64
		wantLag       bool
	}{
		{name: "IdleActiveLane", sourceHigh: 100},
		{name: "UnsentBacklog", sourceHigh: 500, wantLag: true},
		{name: "RetiringPrefixAcknowledged", sourceHigh: 500, retirementEnd: 100},
		{name: "RetiringPrefixPending", sourceHigh: 500, retirementEnd: 200, wantLag: true},
	} {
		s.Run(tc.name, func() {
			t := s.T()
			registry, err := newSenderLaneRegistry(100, nil)
			require.NoError(t, err)
			lane, _, err := registry.Create("a", namespaceLaneFilter("ns-a"), 100, nil)
			require.NoError(t, err)
			if tc.retirementEnd != 0 {
				registry.AdvanceLaneCursor(lane.id, tc.retirementEnd)
				require.True(t, registry.RequestRetirement("a"))
			}
			registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {
				InclusiveLowWatermark:     100,
				InclusiveLowWatermarkTime: timestamppb.New(time.Now().Add(-time.Hour)),
			}})
			s.streamSender.laneRegistry = registry
			s.streamSender.config.ReplicationLaneMaxLag = dynamicconfig.GetDurationPropertyFn(30 * time.Minute)
			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)
			s.streamSender.metrics = captureHandler
			s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(tc.sourceHigh))

			s.streamSender.recordLaneLag()

			snapshot := capture.Snapshot()
			lag := snapshot[metrics.ReplicationLaneLag.Name()]
			require.Len(t, lag, 1)
			if tc.wantLag {
				require.GreaterOrEqual(t, lag[0].Value, time.Hour)
				require.Len(t, snapshot[metrics.ReplicationLaneMaxLagExceeded.Name()], 1)
			} else {
				require.Equal(t, time.Duration(0), lag[0].Value)
				require.Empty(t, snapshot[metrics.ReplicationLaneMaxLagExceeded.Name()])
			}
		})
	}
}
