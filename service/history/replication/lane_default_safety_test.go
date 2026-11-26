package replication

import (
	"math"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func universalScope(inclusiveMin int64) *persistencespb.QueueSliceScope {
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

// laneCapableAck is an ack from a receiver that supports lanes and reports one lane.
func laneCapableAck(tiered bool) *replicationspb.SyncReplicationState {
	at := func(taskID int64) *timestamppb.Timestamp { return timestamppb.New(time.Unix(0, taskID)) }
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     680,
		InclusiveLowWatermarkTime: at(680),
		LaneProtocolVersion:       replicationLaneProtocolVersion,
		LaneStates: map[string]*replicationspb.ReplicationState{
			"lane-a": {InclusiveLowWatermark: 690, InclusiveLowWatermarkTime: at(690)},
		},
		MaxLanes: 4,
	}
	if tiered {
		attr.HighPriorityState = &replicationspb.ReplicationState{InclusiveLowWatermark: 720, InclusiveLowWatermarkTime: at(720)}
		attr.LowPriorityState = &replicationspb.ReplicationState{InclusiveLowWatermark: 910, InclusiveLowWatermarkTime: at(910)}
	}
	return attr
}

// TestLanesOffIgnoresPersistedLaneState checks a sender created while lanes are off, over a
// reader state that a lane sender wrote. It restores no lanes and never asks the policy for
// any. It resumes from the plain scopes, which the lane sender lowered to every lane's resume
// point. It persists plain scopes even when the receiver supports lanes.
func (s *laneStreamSenderSuite) TestLanesOffIgnoresPersistedLaneState() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(int64(s.clientShardKey.ClusterID), s.clientShardKey.ShardID)
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{universalScope(650), universalScope(700), universalScope(900)},
				Lanes: []*persistencespb.QueueReaderLane{{
					LogicalKey: namespaceLaneKey("ns-a"),
					Scope:      queues.ToPersistenceScope(laneScope(namespaceLaneFilter("ns-a"), 700)),
				}},
				ReplicationLaneDefaultCursor: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(950)),
			},
		},
	}, true).AnyTimes()

	for _, tc := range []struct {
		name          string
		tiered        bool
		senderLanes   bool
		policyFactory bool
	}{
		{name: "SenderLanesDisabled", tiered: true, senderLanes: false, policyFactory: true},
		{name: "TieredStackDisabled", tiered: false, senderLanes: true, policyFactory: true},
		{name: "NoPolicyFactory", tiered: true, senderLanes: true, policyFactory: false},
	} {
		s.Run(tc.name, func() {
			s.config.EnableReplicationTaskTieredProcessing = dynamicconfig.GetBoolPropertyFn(tc.tiered)
			s.config.ReplicationStreamSenderLanesEnabled = dynamicconfig.GetBoolPropertyFn(tc.senderLanes)
			var policyFactory LaneStreamPolicyFactory
			if tc.policyFactory {
				// The factory has no expectations, so asking it for a policy fails the test.
				policyFactory = NewMockLaneStreamPolicyFactory(s.controller)
			}
			sender := newLaneStreamSender(
				s.server,
				s.shardContext,
				s.historyEngine,
				quotas.NoopRequestRateLimiter,
				s.taskConverter,
				"target_cluster",
				2,
				s.clientShardKey,
				s.serverShardKey,
				s.config,
				policyFactory,
			)
			s.Nil(sender.laneRegistry)

			attr := laneCapableAck(tc.tiered)
			if tc.tiered {
				s.Equal(int64(700), sender.catchupBeginWatermark(enumsspb.TASK_PRIORITY_HIGH, 1000))
				s.Equal(int64(900), sender.catchupBeginWatermark(enumsspb.TASK_PRIORITY_LOW, 1000))
				s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, &persistencespb.QueueReaderState{
					Scopes: []*persistencespb.QueueSliceScope{universalScope(680), universalScope(720), universalScope(910)},
				})
				s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, int64(719), attr.HighPriorityState.InclusiveLowWatermarkTime.AsTime())
			} else {
				s.Equal(int64(650), sender.catchupBeginWatermark(enumsspb.TASK_PRIORITY_UNSPECIFIED, 1000))
				s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, &persistencespb.QueueReaderState{
					Scopes: []*persistencespb.QueueSliceScope{universalScope(680)},
				})
				s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, int64(679), attr.InclusiveLowWatermarkTime.AsTime())
			}
			s.NoError(sender.recvSyncReplicationState(attr))
		})
	}
}

// TestInitLaneRegistryRestoresPersistedLanes checks a sender created with lanes on over a
// reader state with a persisted lane. It restores the lane from the lane's resume point. Its
// first batch, the empty HIGH batch that makes the receiver ack, ends at the recovery floor,
// so the receiver cannot pass the lane's tasks before the lane is continued.
func (s *laneStreamSenderSuite) TestInitLaneRegistryRestoresPersistedLanes() {
	persisted := s.laneTestReaderState(650, 700, 900)
	persisted.Lanes = []*persistencespb.QueueReaderLane{{
		LogicalKey: namespaceLaneKey("ns-a"),
		Scope:      queues.ToPersistenceScope(laneScope(namespaceLaneFilter("ns-a"), 700)),
	}}
	persisted.ReplicationLaneDefaultCursor = shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(950))
	s.expectPersistedReaderState(persisted)
	s.config.EnableReplicationTaskTieredProcessing = dynamicconfig.GetBoolPropertyFn(true)
	s.config.ReplicationStreamSenderLanesEnabled = dynamicconfig.GetBoolPropertyFn(true)

	sender := newLaneStreamSender(
		s.server,
		s.shardContext,
		s.historyEngine,
		quotas.NoopRequestRateLimiter,
		s.taskConverter,
		"target_cluster",
		2,
		s.clientShardKey,
		s.serverShardKey,
		s.config,
		NewMockLaneStreamPolicyFactory(s.controller),
	)
	s.True(sender.hasPersistedLanes)
	s.False(sender.laneRestoreIncomplete)
	s.Equal(int64(700), sender.laneRecoveryFloor)
	lanes := sender.laneRegistry.Snapshots()
	s.Len(lanes, 1)
	s.Equal(namespaceLaneKey("ns-a"), lanes[0].logicalKey)
	s.Equal(int64(700), lanes[0].scope.Range.InclusiveMin.TaskID)
	s.Equal(int64(math.MaxInt64), lanes[0].scope.Range.ExclusiveMax.TaskID)

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(int64(700), resp.GetMessages().GetExclusiveHighWatermark())
		s.Empty(resp.GetMessages().GetReplicationTasks())
		s.Nil(resp.GetMessages().GetLaneInfo())
		return nil
	})
	close(sender.initialLaneState)
	s.NoError(sender.sendLaneCatchUpPrime(1000))
}

// TestRecvSyncReplicationState_IgnoresLaneFields checks that the legacy sender persists plain
// scopes for an ack from a receiver that supports lanes.
func (s *streamSenderSuite) TestRecvSyncReplicationState_IgnoresLaneFields() {
	s.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(int64(s.clientShardKey.ClusterID), s.clientShardKey.ShardID)
	attr := laneCapableAck(true)
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(attr)
	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, &persistencespb.QueueReaderState{
		Scopes: []*persistencespb.QueueSliceScope{universalScope(680), universalScope(720), universalScope(910)},
	})
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, int64(719), attr.HighPriorityState.InclusiveLowWatermarkTime.AsTime())

	s.NoError(s.streamSender.recvSyncReplicationState(attr))
}
