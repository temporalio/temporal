package history

import (
	"sync"
	"sync/atomic"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

const (
	defaultRangeSize = 20 // 20 bits for sequencer, 2^20 sequence number for any range
)

type (
	// ShardContext represents a history engine shard
	ShardContext interface {
		GetExecutionManager() persistence.ExecutionManager
		GetNextTransferTaskID() (int64, error)
		GetTransferSequenceNumber() int64
		GetTransferAckLevel() int64
		UpdateAckLevel(ackLevel int64) error
		GetTimerSequenceNumber() int64
		CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (
			*persistence.CreateWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error
		GetLogger() bark.Logger
		GetMetricsClient() metrics.Client
	}

	shardContextImpl struct {
		shardID            int
		shardManager       persistence.ShardManager
		executionManager   persistence.ExecutionManager
		timerSequeceNumber int64
		rangeSize          uint
		closeCh            chan<- int
		isClosed           int32
		logger             bark.Logger
		metricsClient      metrics.Client

		sync.RWMutex
		shardInfo                 *persistence.ShardInfo
		transferSequenceNumber    int64
		maxTransferSequenceNumber int64
	}
)

var _ ShardContext = (*shardContextImpl)(nil)

func (s *shardContextImpl) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

func (s *shardContextImpl) GetNextTransferTaskID() (int64, error) {
	s.Lock()
	defer s.Unlock()

	if err := s.updateRangeIfNeededLocked(); err != nil {
		return -1, err
	}

	taskID := s.transferSequenceNumber
	s.transferSequenceNumber++

	return taskID, nil
}

func (s *shardContextImpl) GetTransferSequenceNumber() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.transferSequenceNumber - 1
}

func (s *shardContextImpl) GetTransferAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.TransferAckLevel
}

func (s *shardContextImpl) UpdateAckLevel(ackLevel int64) error {
	s.Lock()
	s.shardInfo.TransferAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	updatedShardInfo := copyShardInfo(s.shardInfo)
	s.Unlock()

	err := s.shardManager.UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: s.shardInfo.RangeID,
	})

	if err != nil {
		// Shard is stolen, trigger history engine shutdown
		if _, ok := err.(*persistence.ShardOwnershipLostError); ok {
			s.close()
		}
	}

	return err
}

func (s *shardContextImpl) GetTimerSequenceNumber() int64 {
	return atomic.AddInt64(&s.timerSequeceNumber, 1)
}

func (s *shardContextImpl) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse, error) {
Create_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		response, err := s.executionManager.CreateWorkflowExecution(request)
		if err != nil {
			if _, ok := err.(*persistence.ShardOwnershipLostError); ok {
				// RangeID might have been renewed by the same host while this update was in flight
				// Retry the operation if we still have the shard ownership
				if currentRangeID != s.getRangeID() {
					continue Create_Loop
				} else {
					// Shard is stolen, trigger shutdown of history engine
					s.close()
				}
			}
		}

		return response, err
	}

	return nil, ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error {
Update_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		err := s.executionManager.UpdateWorkflowExecution(request)
		if err != nil {
			if _, ok := err.(*persistence.ShardOwnershipLostError); ok {
				// RangeID might have been renewed by the same host while this update was in flight
				// Retry the operation if we still have the shard ownership
				if currentRangeID != s.getRangeID() {
					continue Update_Loop
				} else {
					// Shard is stolen, trigger shutdown of history engine
					s.close()
				}
			}
		}

		return err
	}

	return ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) GetLogger() bark.Logger {
	return s.logger
}

func (s *shardContextImpl) GetMetricsClient() metrics.Client {
	return s.metricsClient
}

func (s *shardContextImpl) getRangeID() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.RangeID
}

func (s *shardContextImpl) close() {
	if !atomic.CompareAndSwapInt32(&s.isClosed, 0, 1) {
		return
	}

	if s.closeCh != nil {
		// This is the channel passed in by shard controller to monitor if a shard needs to be unloaded
		// It will trigger the HistoryEngine unload and removal of engine from shard controller
		s.closeCh <- s.shardID
	}
}

func (s *shardContextImpl) updateRangeIfNeededLocked() error {
	if s.transferSequenceNumber < s.maxTransferSequenceNumber {
		return nil
	}

	return s.renewRangeLocked(false)
}

func (s *shardContextImpl) renewRangeLocked(isStealing bool) error {
	updatedShardInfo := copyShardInfo(s.shardInfo)
	updatedShardInfo.RangeID++
	if isStealing {
		updatedShardInfo.StolenSinceRenew++
	}

	err := s.shardManager.UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: s.shardInfo.RangeID})
	if err != nil {
		// Shard is stolen, trigger history engine shutdown
		if _, ok := err.(*persistence.ShardOwnershipLostError); ok {
			s.close()
		}
		return err
	}

	// Range is successfully updated in cassandra now update shard context to reflect new range
	s.transferSequenceNumber = updatedShardInfo.RangeID << s.rangeSize
	s.maxTransferSequenceNumber = (updatedShardInfo.RangeID + 1) << s.rangeSize
	s.shardInfo = updatedShardInfo

	logShardRangeUpdatedEvent(s.logger, s.shardInfo.ShardID, s.shardInfo.RangeID, s.transferSequenceNumber,
		s.maxTransferSequenceNumber)

	return nil
}

func acquireShard(shardID int, shardManager persistence.ShardManager, executionMgr persistence.ExecutionManager,
	owner string, closeCh chan<- int, logger bark.Logger, reporter metrics.Client) (ShardContext, error) {
	response, err0 := shardManager.GetShard(&persistence.GetShardRequest{ShardID: shardID})
	if err0 != nil {
		return nil, err0
	}

	shardInfo := response.ShardInfo
	updatedShardInfo := copyShardInfo(shardInfo)
	updatedShardInfo.Owner = owner
	context := &shardContextImpl{
		shardID:          shardID,
		shardManager:     shardManager,
		executionManager: executionMgr,
		shardInfo:        updatedShardInfo,
		rangeSize:        defaultRangeSize,
		closeCh:          closeCh,
	}
	context.logger = logger.WithFields(bark.Fields{
		tagHistoryShardID: shardID,
	})
	tags := map[string]string{
		metrics.ShardTagName: string(shardID),
	}
	context.metricsClient = reporter.Tagged(tags)

	err1 := context.renewRangeLocked(true)
	if err1 != nil {
		return nil, err1
	}

	return context, nil
}

func copyShardInfo(shardInfo *persistence.ShardInfo) *persistence.ShardInfo {
	shardInfoCopy := &persistence.ShardInfo{
		ShardID:          shardInfo.ShardID,
		Owner:            shardInfo.Owner,
		RangeID:          shardInfo.RangeID,
		StolenSinceRenew: shardInfo.StolenSinceRenew,
		TransferAckLevel: atomic.LoadInt64(&shardInfo.TransferAckLevel),
	}

	return shardInfoCopy
}
