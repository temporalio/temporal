package shard

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func convertPersistenceAckLevelToTaskKey(
	categoryType tasks.CategoryType,
	ackLevel int64,
) tasks.Key {
	if categoryType == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(ackLevel)
	}
	return tasks.NewKey(timestamp.UnixOrZeroTime(ackLevel), 0)
}

func convertTaskKeyToPersistenceAckLevel(
	categoryType tasks.CategoryType,
	taskKey tasks.Key,
) int64 {
	if categoryType == tasks.CategoryTypeImmediate {
		return taskKey.TaskID
	}
	return taskKey.FireTime.UnixNano()
}

func ConvertFromPersistenceTaskKey(
	key *persistencespb.TaskKey,
) tasks.Key {
	return tasks.NewKey(
		timestamp.TimeValue(key.FireTime),
		key.TaskId,
	)
}

func ConvertToPersistenceTaskKey(
	key tasks.Key,
) *persistencespb.TaskKey {
	return &persistencespb.TaskKey{
		FireTime: timestamppb.New(key.FireTime),
		TaskId:   key.TaskID,
	}
}

// ReplicationReaderIDFromClusterShardID convert from cluster ID & shard ID to reader ID
// NOTE: cluster metadata guarantee
//  1. initial failover version <= int32 max
//  2. failover increment <= int32 max
//  3. initial failover version == cluster ID
func ReplicationReaderIDFromClusterShardID(
	clusterID int64,
	shardID int32,
) int64 {
	return clusterID<<32 + int64(shardID)
}

// ReplicationReaderIDToClusterShardID convert from reader ID to cluster ID & shard ID
// NOTE: see ReplicationReaderIDFromClusterShardID
func ReplicationReaderIDToClusterShardID(
	readerID int64,
) (int64, int32) {
	return readerID >> 32, int32(readerID & 0xffffffff)
}

func getMinTaskKey(
	queueState *persistencespb.QueueState,
) *tasks.Key {
	var minTaskKey *tasks.Key
	if queueState.ExclusiveReaderHighWatermark != nil {
		taskKey := ConvertFromPersistenceTaskKey(queueState.ExclusiveReaderHighWatermark)
		minTaskKey = &taskKey
	}
	consider := func(persistenceKey *persistencespb.TaskKey) {
		taskKey := ConvertFromPersistenceTaskKey(persistenceKey)
		if minTaskKey == nil || taskKey.CompareTo(*minTaskKey) < 0 {
			minTaskKey = &taskKey
		}
	}
	for _, readerState := range queueState.ReaderStates {
		// Only the first scope: replication readers hold per-priority scopes and lag follows the first.
		if len(readerState.Scopes) > 0 {
			consider(readerState.Scopes[0].Range.InclusiveMin)
		}
		for _, group := range readerState.PredicateGroups {
			for _, taskRange := range group.Ranges {
				consider(taskRange.InclusiveMin)
			}
		}
	}
	return minTaskKey
}
