package workflow

import (
	"sync/atomic"

	"code.uber.internal/devexp/minions/persistence"
)

type (
	shardContext struct {
		executionManager       persistence.ExecutionManager
		shardInfo              *persistence.ShardInfo
		transferSequenceNumber int64
		timerSequeceNumber     int64
	}
)

func (s *shardContext) GetTimerSequenceNumber() int64 {
	return atomic.AddInt64(&s.timerSequeceNumber, 1)
}

func (s *shardContext) GetTransferTaskID() int64 {
	return atomic.AddInt64(&s.transferSequenceNumber, 1)
}

func (s *shardContext) GetRangeID() int64 {
	return atomic.LoadInt64(&s.shardInfo.RangeID)
}

func acquireShard(shardID int, executionManager persistence.ExecutionManager) (*shardContext, error) {
	response, err0 := executionManager.GetShard(&persistence.GetShardRequest{ShardID: shardID})
	if err0 != nil {
		return nil, err0
	}

	shardInfo := response.ShardInfo
	updatedShardInfo := copyShardInfo(shardInfo)
	updatedShardInfo.RangeID++

	err1 := executionManager.UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: shardInfo.RangeID})
	if err1 != nil {
		return nil, err1
	}

	context := &shardContext{
		executionManager:       executionManager,
		shardInfo:              updatedShardInfo,
		transferSequenceNumber: updatedShardInfo.RangeID << 24,
	}

	return context, nil
}

func copyShardInfo(shardInfo *persistence.ShardInfo) *persistence.ShardInfo {
	copy := &persistence.ShardInfo{
		ShardID:          shardInfo.ShardID,
		RangeID:          shardInfo.RangeID,
		TransferAckLevel: shardInfo.TransferAckLevel,
	}

	return copy
}
