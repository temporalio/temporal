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
	"go.temporal.io/server/common/namespace"
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
	s.Contains(s.streamSender.lanePacedAt, logicalKey)
}

func (s *laneStreamSenderSuite) TestPaceLane_NotPaced() {
	logicalKey := namespaceLaneKey("ns-a")
	limiter := &fakeLaneLimiter{refusals: 1, retryAfter: time.Hour}
	s.requestLane(logicalKey, limiter)
	s.streamSender.requestedLanes = nil

	// A lane the policy no longer requests catches up without its limiter.
	s.NoError(s.streamSender.paceLane(context.Background(), logicalKey))
	s.Zero(limiter.calls)
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
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(gomock.Any()).AnyTimes()
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
	s.True(s.streamSender.lanePaced(logicalKey))

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
	s.False(s.streamSender.lanePaced(logicalKey))
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
	registry.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 150}})
	s.True(registry.RequestRetirement(lane.logicalKey))
	s.streamSender.laneRegistry = registry
	s.streamSender.laneLimiters[lane.logicalKey] = &fakeLaneLimiter{}
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(&replicationspb.ReplicationLaneInfo{LaneId: lane.id, Retire: true}, resp.GetMessages().GetLaneInfo())
		s.Equal(int64(100), resp.GetMessages().GetExclusiveHighWatermark())
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
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100))

	s.NoError(s.streamSender.reconcileLanes(syncReplicationState(100, 100, 100), false))
	s.False(s.streamSender.lanePaced(namespaceLaneKey("ns-a")))
}

func (s *laneStreamSenderSuite) TestReconcileLanes_WithoutPolicy() {
	registry, err := newSenderLaneRegistry(0, nil)
	s.NoError(err)
	s.streamSender.laneRegistry = registry
	_, _, err = registry.Create(namespaceLaneKey("ns-a"), namespaceLaneFilter("ns-a"), 0, nil)
	s.NoError(err)
	s.streamSender.requestedLanes = map[string]struct{}{namespaceLaneKey("ns-a"): {}}
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100))

	s.NoError(s.streamSender.reconcileLanes(syncReplicationState(100, 100, 100), false))
	s.False(s.streamSender.lanePaced(namespaceLaneKey("ns-a")))
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

func (s *laneStreamSenderSuite) TestEvaluateLanes_KeepsLastAnswerForGrace() {
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	s.streamSender.config.ReplicationLaneEvaluateErrorGrace = dynamicconfig.GetDurationPropertyFn(time.Minute)
	start := time.Unix(1_000_000, 0)
	good := []DesiredLane{{Key: namespaceLaneKey("ns-a"), Filter: namespaceLaneFilter("ns-a")}}
	failure := errors.New("unavailable")

	// Before any good answer, an error retires all lanes.
	policy.EXPECT().Evaluate(gomock.Any()).Return(nil, failure)
	desired, err := s.streamSender.evaluateLanes(LaneSignals{Now: start})
	s.NoError(err)
	s.Nil(desired)

	policy.EXPECT().Evaluate(gomock.Any()).Return(good, nil)
	desired, err = s.streamSender.evaluateLanes(LaneSignals{Now: start})
	s.NoError(err)
	s.Equal(good, desired)

	// The grace starts at the first failure, and ends with the last answer retired.
	policy.EXPECT().Evaluate(gomock.Any()).Return(nil, failure).Times(2)
	desired, err = s.streamSender.evaluateLanes(LaneSignals{Now: start.Add(time.Hour)})
	s.NoError(err)
	s.Equal(good, desired)
	desired, err = s.streamSender.evaluateLanes(LaneSignals{Now: start.Add(time.Hour + time.Minute)})
	s.NoError(err)
	s.Nil(desired)

	// A good answer ends the failure, and a policy can ask for a longer grace.
	policy.EXPECT().Evaluate(gomock.Any()).Return(good, nil)
	_, err = s.streamSender.evaluateLanes(LaneSignals{Now: start.Add(2 * time.Hour)})
	s.NoError(err)
	policy.EXPECT().Evaluate(gomock.Any()).Return(nil, &LaneEvaluateError{Err: failure, Grace: time.Hour}).Times(2)
	desired, err = s.streamSender.evaluateLanes(LaneSignals{Now: start.Add(2 * time.Hour)})
	s.NoError(err)
	s.Equal(good, desired)
	desired, err = s.streamSender.evaluateLanes(LaneSignals{Now: start.Add(2*time.Hour + 30*time.Minute)})
	s.NoError(err)
	s.Equal(good, desired)
}

func (s *laneStreamSenderSuite) TestEvaluateLanes_Actions() {
	good := []DesiredLane{{Key: namespaceLaneKey("ns-a"), Filter: namespaceLaneFilter("ns-a")}}
	now := time.Unix(1_000_000, 0)
	for _, tc := range []struct {
		name          string
		action        LaneErrorAction
		expectRestart bool
		expectAsked   bool // whether the policy is asked again on the next ack
	}{
		{name: "RetireAll", action: LaneErrorRetireAll, expectAsked: true},
		{name: "NoLanes", action: LaneErrorNoLanes},
		{name: "RestartStream", action: LaneErrorRestartStream, expectRestart: true, expectAsked: true},
	} {
		s.Run(tc.name, func() {
			policy := NewMockLaneStreamPolicy(s.controller)
			s.streamSender.lanePolicy = policy
			s.streamSender.lanePolicyDisabled = false
			s.streamSender.laneRestarts = newLaneRestartTracker()
			policy.EXPECT().Evaluate(gomock.Any()).Return(good, nil)
			_, err := s.streamSender.evaluateLanes(LaneSignals{Now: now})
			s.NoError(err)

			policy.EXPECT().Evaluate(gomock.Any()).Return(nil, &LaneEvaluateError{Err: errors.New("failed"), Action: tc.action})
			desired, err := s.streamSender.evaluateLanes(LaneSignals{Now: now})
			s.Nil(desired)
			if tc.expectRestart {
				s.ErrorAs(err, new(*StreamError))
			} else {
				s.NoError(err)
			}

			if tc.expectAsked {
				policy.EXPECT().Evaluate(gomock.Any()).Return(good, nil)
			}
			desired, err = s.streamSender.evaluateLanes(LaneSignals{Now: now})
			s.NoError(err)
			if tc.expectAsked {
				s.Equal(good, desired)
			} else {
				s.Nil(desired)
			}
		})
	}
}

func (s *laneStreamSenderSuite) TestEvaluateLanes_RestartLimit() {
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicy = policy
	now := time.Unix(1_000_000, 0)
	restart := &LaneEvaluateError{Err: errors.New("failed"), Action: LaneErrorRestartStream}
	policy.EXPECT().Evaluate(gomock.Any()).Return(nil, restart).Times(laneRestartLimit + 1)

	for range laneRestartLimit {
		_, err := s.streamSender.evaluateLanes(LaneSignals{Now: now})
		s.ErrorAs(err, new(*StreamError))
	}
	// One restart too many retires the lanes instead, and stops asking the policy.
	desired, err := s.streamSender.evaluateLanes(LaneSignals{Now: now})
	s.NoError(err)
	s.Nil(desired)
	s.True(s.streamSender.lanePolicyDisabled)
}

func TestLaneRetryDelay(t *testing.T) {
	require.Equal(t, laneMinRetryAfter, laneRetryDelay(0))
	require.Equal(t, laneMinRetryAfter, laneRetryDelay(-time.Second))
	require.Equal(t, 200*time.Millisecond, laneRetryDelay(200*time.Millisecond))
	require.Equal(t, laneMaxRetryAfter, laneRetryDelay(time.Hour))
}

func TestLaneRestartTracker(t *testing.T) {
	tracker := newLaneRestartTracker()
	stream := LaneStream{RemoteCluster: "remote", ShardID: 1}
	start := time.Unix(1_000_000, 0)
	for i := range laneRestartLimit {
		require.True(t, tracker.Allow(stream, start.Add(time.Duration(i)*time.Minute)))
	}
	require.False(t, tracker.Allow(stream, start.Add(5*time.Minute)))
	// Other streams have their own count.
	require.True(t, tracker.Allow(LaneStream{RemoteCluster: "remote", ShardID: 2}, start))
	// Restarts older than the window no longer count.
	require.True(t, tracker.Allow(stream, start.Add(laneRestartWindow)))
}

func (s *laneStreamSenderSuite) TestLanePolicyLifecycle() {
	factory := NewMockLaneStreamPolicyFactory(s.controller)
	policy := NewMockLaneStreamPolicy(s.controller)
	s.streamSender.lanePolicyFactory = factory
	factory.EXPECT().NewStreamPolicy(LaneStream{RemoteCluster: "target_cluster", ShardID: s.serverShardKey.ShardID}).Return(policy, nil)
	s.streamSender.startLanePolicy()

	// Stopping the stream closes the policy once, and no method is called after that.
	policy.EXPECT().Close()
	s.streamSender.closeLanePolicy()
	s.streamSender.closeLanePolicy()
	s.streamSender.laneRetired(namespaceLaneKey("ns-a"))
}

func (s *laneStreamSenderSuite) TestStartLanePolicy_FactoryError() {
	factory := NewMockLaneStreamPolicyFactory(s.controller)
	s.streamSender.lanePolicyFactory = factory
	factory.EXPECT().NewStreamPolicy(gomock.Any()).Return(nil, errors.New("unavailable"))

	s.streamSender.startLanePolicy()
	s.Nil(s.streamSender.lanePolicy)
}
