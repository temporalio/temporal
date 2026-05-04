package replication

import (
	"math"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

// ReplicationReaderGroup manages per-priority cursor state for a replication stream
// sender. It encapsulates the QueueReaderState scope-index arithmetic that was
// previously spread across recvSyncReplicationState and
// getSendCatchupBeginInclusiveWatermark.
//
// HIGH/LOW priority lane isolation using the existing 3-scope QueueReaderState
// encoding. No wire-protocol or persistence-format changes.
type ReplicationReaderGroup struct {
	shardContext   historyi.ShardContext
	clientShardKey ClusterShardKey
	tieredEnabled  bool
}

func NewReplicationReaderGroup(
	shardContext historyi.ShardContext,
	clientShardKey ClusterShardKey,
	tieredEnabled bool,
) *ReplicationReaderGroup {
	return &ReplicationReaderGroup{
		shardContext:   shardContext,
		clientShardKey: clientShardKey,
		tieredEnabled:  tieredEnabled,
	}
}

// ReaderID returns the persistence reader ID for this stream's remote shard.
func (r *ReplicationReaderGroup) ReaderID() int64 {
	return shard.ReplicationReaderIDFromClusterShardID(
		int64(r.clientShardKey.ClusterID),
		r.clientShardKey.ShardID,
	)
}

// CatchupBeginWatermark returns the inclusive low watermark for the given priority
// lane from the persisted QueueState. Falls back to catchupEnd if no state is found.
func (r *ReplicationReaderGroup) CatchupBeginWatermark(
	catchupEnd int64,
	priority enumsspb.TaskPriority,
) int64 {
	queueState, ok := r.shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		return catchupEnd
	}
	readerState, ok := queueState.ReaderStates[r.ReaderID()]
	if !ok {
		return catchupEnd
	}
	return readerState.Scopes[priorityScopeIndex(priority, len(readerState.Scopes))].Range.InclusiveMin.TaskId
}

// BuildReaderState constructs the QueueReaderState to persist from a received
// SyncReplicationState ack. Returns an error if the priority fields are inconsistent
// with the current tiered/single-stack mode.
func (r *ReplicationReaderGroup) BuildReaderState(
	attr *replicationspb.SyncReplicationState,
) (*persistencespb.QueueReaderState, error) {
	if r.tieredEnabled {
		if attr.HighPriorityState == nil || attr.LowPriorityState == nil {
			return nil, NewStreamError("ReplicationReaderGroup: missing priority state in tiered mode", nil)
		}
		return buildTieredReaderState(attr), nil
	}
	if attr.HighPriorityState != nil || attr.LowPriorityState != nil {
		return nil, NewStreamError("ReplicationReaderGroup: unexpected priority state in single-stack mode", nil)
	}
	return buildSingleStackReaderState(attr), nil
}

// FailoverWatermark returns the task ID and timestamp for UpdateRemoteReaderInfo.
// In tiered mode, the high-priority watermark is used since that lane carries live traffic.
func (r *ReplicationReaderGroup) FailoverWatermark(
	attr *replicationspb.SyncReplicationState,
) (int64, time.Time) {
	if r.tieredEnabled {
		return attr.HighPriorityState.InclusiveLowWatermark - 1,
			attr.HighPriorityState.InclusiveLowWatermarkTime.AsTime()
	}
	return attr.GetInclusiveLowWatermark() - 1,
		attr.GetInclusiveLowWatermarkTime().AsTime()
}

// priorityScopeIndex maps a priority to its index within QueueReaderState.Scopes.
// Falls back to index 0 (the overall watermark) when the state was written by an
// older single-stack version that only has one scope.
func priorityScopeIndex(priority enumsspb.TaskPriority, scopeCount int) int {
	if scopeCount == 3 {
		switch priority {
		case enumsspb.TASK_PRIORITY_HIGH:
			return 1
		case enumsspb.TASK_PRIORITY_LOW:
			return 2
		}
	}
	return 0
}

func buildTieredReaderState(attr *replicationspb.SyncReplicationState) *persistencespb.QueueReaderState {
	makeScope := func(taskID int64) *persistencespb.QueueSliceScope {
		return &persistencespb.QueueSliceScope{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(taskID)),
				ExclusiveMax: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(math.MaxInt64)),
			},
			Predicate: &persistencespb.Predicate{
				PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
				Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
			},
		}
	}
	return &persistencespb.QueueReaderState{
		Scopes: []*persistencespb.QueueSliceScope{
			makeScope(attr.GetInclusiveLowWatermark()),
			makeScope(attr.GetHighPriorityState().GetInclusiveLowWatermark()),
			makeScope(attr.GetLowPriorityState().GetInclusiveLowWatermark()),
		},
	}
}

func buildSingleStackReaderState(attr *replicationspb.SyncReplicationState) *persistencespb.QueueReaderState {
	return &persistencespb.QueueReaderState{
		Scopes: []*persistencespb.QueueSliceScope{
			{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(attr.GetInclusiveLowWatermark())),
					ExclusiveMax: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(math.MaxInt64)),
				},
				Predicate: &persistencespb.Predicate{
					PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
					Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
				},
			},
		},
	}
}
