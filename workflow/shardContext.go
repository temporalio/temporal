package workflow

import (
	"sync/atomic"

	"code.uber.internal/devexp/minions/persistence"
)

type (
	// ShardContext represents a history engine shard
	ShardContext interface {
		GetTransferTaskID() int64
		GetRangeID() int64
		GetTransferAckLevel() int64
		GetTimerSequenceNumber() int64
		UpdateAckLevel(ackLevel int64) error
	}

	shardContextImpl struct {
		executionManager       persistence.ExecutionManager
		shardInfo              *persistence.ShardInfo
		transferSequenceNumber int64
		timerSequeceNumber     int64
	}
)

func (s *shardContextImpl) GetTimerSequenceNumber() int64 {
	return atomic.AddInt64(&s.timerSequeceNumber, 1)
}

func (s *shardContextImpl) GetTransferTaskID() int64 {
	return atomic.AddInt64(&s.transferSequenceNumber, 1)
}

func (s *shardContextImpl) GetRangeID() int64 {
	return atomic.LoadInt64(&s.shardInfo.RangeID)
}

func (s *shardContextImpl) GetTransferAckLevel() int64 {
	return atomic.LoadInt64(&s.shardInfo.TransferAckLevel)
}

func (s *shardContextImpl) UpdateAckLevel(ackLevel int64) error {
	atomic.StoreInt64(&s.shardInfo.TransferAckLevel, ackLevel)
	updatedShardInfo := copyShardInfo(s.shardInfo)
	return s.executionManager.UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: updatedShardInfo.RangeID,
	})
}

func acquireShard(shardID int, executionManager persistence.ExecutionManager) (ShardContext, error) {
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

	context := &shardContextImpl{
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
		TransferAckLevel: atomic.LoadInt64(&shardInfo.TransferAckLevel),
	}

	return copy
}
