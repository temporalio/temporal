package replication

import (
	"fmt"
	"math"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

// replicationReaderGroup manages per-priority cursor state for a replication stream
// sender. It encapsulates the QueueReaderState scope-index arithmetic that was
// previously spread across recvSyncReplicationState and
// getSendCatchupBeginInclusiveWatermark.
//
// HIGH/LOW priority lane isolation using the existing 3-scope QueueReaderState
// encoding. No wire-protocol or persistence-format changes.
type replicationReaderGroup struct {
	shardContext   historyi.ShardContext
	clientShardKey ClusterShardKey
	tieredEnabled  bool
	logger         log.Logger
}

func newReplicationReaderGroup(
	shardContext historyi.ShardContext,
	clientShardKey ClusterShardKey,
	tieredEnabled bool,
	logger log.Logger,
) *replicationReaderGroup {
	return &replicationReaderGroup{
		shardContext:   shardContext,
		clientShardKey: clientShardKey,
		tieredEnabled:  tieredEnabled,
		logger:         logger,
	}
}

// ReaderID returns the persistence reader ID for this stream's remote shard.
func (r *replicationReaderGroup) ReaderID() int64 {
	return shard.ReplicationReaderIDFromClusterShardID(
		int64(r.clientShardKey.ClusterID),
		r.clientShardKey.ShardID,
	)
}

// CatchupBeginWatermark returns the inclusive low watermark for the given priority
// lane from the persisted QueueState. Falls back to catchupEnd if no state is found.
func (r *replicationReaderGroup) CatchupBeginWatermark(
	catchupEnd int64,
	priority enumsspb.TaskPriority,
) int64 {
	queueState, ok := r.shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		r.logger.Debug("StreamSender queueState not found")
		return catchupEnd
	}
	readerState, ok := queueState.ReaderStates[r.ReaderID()]
	if !ok {
		r.logger.Debug(fmt.Sprintf("StreamSender readerState not found, readerID %v", r.ReaderID()))
		return catchupEnd
	}
	return readerState.Scopes[priorityScopeIndex(priority, len(readerState.Scopes), true)].Range.InclusiveMin.TaskId
}

// BuildReaderState constructs the QueueReaderState to persist from a received
// SyncReplicationState ack. Returns an error if the priority fields are inconsistent
// with the current tiered/single-stack mode.
func (r *replicationReaderGroup) BuildReaderState(
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
// It validates the priority fields itself so callers need not run BuildReaderState first.
func (r *replicationReaderGroup) FailoverWatermark(
	attr *replicationspb.SyncReplicationState,
) (int64, time.Time, error) {
	if r.tieredEnabled {
		high := attr.GetHighPriorityState()
		if high == nil {
			return 0, time.Time{}, NewStreamError("ReplicationReaderGroup: missing high priority state in tiered mode", nil)
		}
		return high.InclusiveLowWatermark - 1,
			high.InclusiveLowWatermarkTime.AsTime(), nil
	}
	return attr.GetInclusiveLowWatermark() - 1,
		attr.GetInclusiveLowWatermarkTime().AsTime(), nil
}

// priorityScopeIndex maps a priority to its index within QueueReaderState.Scopes.
// Scope 0 is the overall watermark, scope 1 HIGH, scope 2 default-LOW.
// Falls back to index 0 (the overall watermark) when the state was written by an
// older single-stack version that only has one scope.
//
// allowExtraScopes controls the >3-scope case: with the reader group disabled it is
// false and any scope count other than exactly 3 falls back to index 0 — the
// pre-reader-group behavior, byte for byte. With the reader group enabled it is true
// and scopes 3+ are tolerated: they are reserved for the per-lane cursors of
// replication namespace isolation, introduced by a follow-up PR (which updates this
// comment to point at the component that owns them).
func priorityScopeIndex(priority enumsspb.TaskPriority, scopeCount int, allowExtraScopes bool) int {
	if scopeCount == 3 || (allowExtraScopes && scopeCount > 3) {
		switch priority {
		case enumsspb.TASK_PRIORITY_HIGH:
			return 1
		case enumsspb.TASK_PRIORITY_LOW:
			return 2
		case enumsspb.TASK_PRIORITY_UNSPECIFIED:
			return 0
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
