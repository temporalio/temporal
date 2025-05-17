package vclock

import (
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
)

func NewVectorClock(
	clusterID int64,
	shardID int32,
	clock int64,
) *clockspb.VectorClock {
	return &clockspb.VectorClock{
		ClusterId: clusterID,
		ShardId:   shardID,
		Clock:     clock,
	}
}

func Comparable(
	clock1 *clockspb.VectorClock,
	clock2 *clockspb.VectorClock,
) bool {
	if clock1 == nil || clock2 == nil {
		return false
	}
	return clock1.GetClusterId() == clock2.GetClusterId() &&
		clock1.GetShardId() == clock2.GetShardId()
}

func Compare(
	clock1 *clockspb.VectorClock,
	clock2 *clockspb.VectorClock,
) (int, error) {
	if !Comparable(clock1, clock2) {
		return 0, serviceerror.NewInternalf(
			"Encountered shard ID mismatch: %v:%v vs %v:%v",
			clock1.GetClusterId(),
			clock1.GetShardId(),
			clock2.GetClusterId(),
			clock2.GetShardId(),
		)
	}

	vClock1 := clock1.GetClock()
	vClock2 := clock2.GetClock()
	if vClock1 < vClock2 {
		return -1, nil
	} else if vClock1 > vClock2 {
		return 1, nil
	} else {
		return 0, nil
	}
}
