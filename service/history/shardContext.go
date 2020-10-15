// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
)

type (
	// ShardContext represents a history engine shard
	ShardContext interface {
		GetShardID() int32
		GetService() resource.Resource
		GetExecutionManager() persistence.ExecutionManager
		GetHistoryManager() persistence.HistoryManager
		GetNamespaceCache() cache.NamespaceCache
		GetClusterMetadata() cluster.Metadata
		GetConfig() *Config
		GetEventsCache() eventsCache
		GetLogger() log.Logger
		GetThrottledLogger() log.Logger
		GetMetricsClient() metrics.Client
		GetTimeSource() clock.TimeSource
		PreviousShardOwnerWasDifferent() bool

		GetEngine() Engine
		SetEngine(Engine)

		GenerateTransferTaskID() (int64, error)
		GenerateTransferTaskIDs(number int) ([]int64, error)

		GetTransferMaxReadLevel() int64
		UpdateTimerMaxReadLevel(cluster string) time.Time

		SetCurrentTime(cluster string, currentTime time.Time)
		GetCurrentTime(cluster string) time.Time
		GetLastUpdatedTime() time.Time
		GetTimerMaxReadLevel(cluster string) time.Time

		GetTransferAckLevel() int64
		UpdateTransferAckLevel(ackLevel int64) error
		GetTransferClusterAckLevel(cluster string) int64
		UpdateTransferClusterAckLevel(cluster string, ackLevel int64) error

		GetReplicatorAckLevel() int64
		UpdateReplicatorAckLevel(ackLevel int64) error
		GetReplicatorDLQAckLevel(sourceCluster string) int64
		UpdateReplicatorDLQAckLevel(sourCluster string, ackLevel int64) error

		GetClusterReplicationLevel(cluster string) int64
		UpdateClusterReplicationLevel(cluster string, lastTaskID int64) error

		GetTimerAckLevel() time.Time
		UpdateTimerAckLevel(ackLevel time.Time) error
		GetTimerClusterAckLevel(cluster string) time.Time
		UpdateTimerClusterAckLevel(cluster string, ackLevel time.Time) error

		UpdateTransferFailoverLevel(failoverID string, level persistence.TransferFailoverLevel) error
		DeleteTransferFailoverLevel(failoverID string) error
		GetAllTransferFailoverLevels() map[string]persistence.TransferFailoverLevel

		UpdateTimerFailoverLevel(failoverID string, level persistence.TimerFailoverLevel) error
		DeleteTimerFailoverLevel(failoverID string) error
		GetAllTimerFailoverLevels() map[string]persistence.TimerFailoverLevel

		GetNamespaceNotificationVersion() int64
		UpdateNamespaceNotificationVersion(namespaceNotificationVersion int64) error

		CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error)
		ConflictResolveWorkflowExecution(request *persistence.ConflictResolveWorkflowExecutionRequest) error
		ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error
		AppendHistoryV2Events(request *persistence.AppendHistoryNodesRequest, namespaceID string, execution commonpb.WorkflowExecution) (int, error)
	}

	shardContextImpl struct {
		resource.Resource

		shardItem        *historyShardsItem
		shardID          int32
		rangeID          int64
		executionManager persistence.ExecutionManager
		eventsCache      eventsCache
		closeCallback    func(int32, *historyShardsItem)
		closed           int32
		config           *Config
		logger           log.Logger
		throttledLogger  log.Logger
		engine           Engine

		sync.RWMutex
		lastUpdated               time.Time
		shardInfo                 *persistence.ShardInfoWithFailover
		transferSequenceNumber    int64
		maxTransferSequenceNumber int64
		transferMaxReadLevel      int64
		timerMaxReadLevelMap      map[string]time.Time // cluster -> timerMaxReadLevel

		// exist only in memory
		remoteClusterCurrentTime map[string]time.Time

		// true if previous owner was different from the acquirer's identity.
		previousShardOwnerWasDifferent bool
	}
)

var _ ShardContext = (*shardContextImpl)(nil)

// ErrShardClosed is returned when shard is closed and a req cannot be processed
var ErrShardClosed = errors.New("shard closed")

const (
	logWarnTransferLevelDiff = 3000000 // 3 million
	logWarnTimerLevelDiff    = time.Duration(30 * time.Minute)
	historySizeLogThreshold  = 10 * 1024 * 1024
)

func (s *shardContextImpl) GetShardID() int32 {
	return s.shardID
}

func (s *shardContextImpl) GetService() resource.Resource {
	return s.Resource
}

func (s *shardContextImpl) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

func (s *shardContextImpl) GetEngine() Engine {
	return s.engine
}

func (s *shardContextImpl) SetEngine(engine Engine) {
	s.engine = engine
}

func (s *shardContextImpl) GenerateTransferTaskID() (int64, error) {
	s.Lock()
	defer s.Unlock()

	return s.generateTransferTaskIDLocked()
}

func (s *shardContextImpl) GenerateTransferTaskIDs(number int) ([]int64, error) {
	s.Lock()
	defer s.Unlock()

	result := []int64{}
	for i := 0; i < number; i++ {
		id, err := s.generateTransferTaskIDLocked()
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}

func (s *shardContextImpl) GetTransferMaxReadLevel() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.transferMaxReadLevel
}

func (s *shardContextImpl) GetTransferAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.TransferAckLevel
}

func (s *shardContextImpl) UpdateTransferAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetTransferClusterAckLevel(cluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding ack level
	if ackLevel, ok := s.shardInfo.ClusterTransferAckLevel[cluster]; ok {
		return ackLevel
	}
	// otherwise, default to existing ack level, which belongs to local cluster
	// this can happen if you add more cluster
	return s.shardInfo.TransferAckLevel
}

func (s *shardContextImpl) UpdateTransferClusterAckLevel(cluster string, ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTransferAckLevel[cluster] = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetReplicatorAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.ReplicationAckLevel
}

func (s *shardContextImpl) UpdateReplicatorAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()
	s.shardInfo.ReplicationAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetReplicatorDLQAckLevel(sourceCluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	if ackLevel, ok := s.shardInfo.ReplicationDlqAckLevel[sourceCluster]; ok {
		return ackLevel
	}
	return -1
}

func (s *shardContextImpl) UpdateReplicatorDLQAckLevel(
	sourceCluster string,
	ackLevel int64,
) error {

	s.Lock()
	defer s.Unlock()

	s.shardInfo.ReplicationDlqAckLevel[sourceCluster] = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	if err := s.updateShardInfoLocked(); err != nil {
		return err
	}

	s.GetMetricsClient().Scope(
		metrics.ReplicationDLQStatsScope,
		metrics.TargetClusterTag(sourceCluster),
		metrics.InstanceTag(convert.Int32ToString(s.shardID)),
	).UpdateGauge(
		metrics.ReplicationDLQAckLevelGauge,
		float64(ackLevel),
	)
	return nil
}

func (s *shardContextImpl) GetClusterReplicationLevel(cluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding replication level
	if replicationLevel, ok := s.shardInfo.ClusterReplicationLevel[cluster]; ok {
		return replicationLevel
	}

	// New cluster always starts from -1
	return -1
}

func (s *shardContextImpl) UpdateClusterReplicationLevel(cluster string, lastTaskID int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterReplicationLevel[cluster] = lastTaskID
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetTimerAckLevel() time.Time {
	s.RLock()
	defer s.RUnlock()

	return timestamp.TimeValue(s.shardInfo.TimerAckLevelTime)
}

func (s *shardContextImpl) UpdateTimerAckLevel(ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerAckLevelTime = &ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetTimerClusterAckLevel(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding ack level
	if ackLevel, ok := s.shardInfo.ClusterTimerAckLevel[cluster]; ok {

		return timestamp.TimeValue(ackLevel)
	}
	// otherwise, default to existing ack level, which belongs to local cluster
	// this can happen if you add more cluster
	return timestamp.TimeValue(s.shardInfo.TimerAckLevelTime)
}

func (s *shardContextImpl) UpdateTimerClusterAckLevel(cluster string, ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTimerAckLevel[cluster] = &ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) UpdateTransferFailoverLevel(failoverID string, level persistence.TransferFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferFailoverLevels[failoverID] = level
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) DeleteTransferFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	if level, ok := s.shardInfo.TransferFailoverLevels[failoverID]; ok {
		s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferFailoverLatencyTimer, time.Since(level.StartTime))
		delete(s.shardInfo.TransferFailoverLevels, failoverID)
	}
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetAllTransferFailoverLevels() map[string]persistence.TransferFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TransferFailoverLevel{}
	for k, v := range s.shardInfo.TransferFailoverLevels {
		ret[k] = v
	}
	return ret
}

func (s *shardContextImpl) UpdateTimerFailoverLevel(failoverID string, level persistence.TimerFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerFailoverLevels[failoverID] = level
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) DeleteTimerFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	if level, ok := s.shardInfo.TimerFailoverLevels[failoverID]; ok {
		s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerFailoverLatencyTimer, time.Since(level.StartTime))
		delete(s.shardInfo.TimerFailoverLevels, failoverID)
	}
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetAllTimerFailoverLevels() map[string]persistence.TimerFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TimerFailoverLevel{}
	for k, v := range s.shardInfo.TimerFailoverLevels {
		ret[k] = v
	}
	return ret
}

func (s *shardContextImpl) GetNamespaceNotificationVersion() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.NamespaceNotificationVersion
}

func (s *shardContextImpl) UpdateNamespaceNotificationVersion(namespaceNotificationVersion int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.NamespaceNotificationVersion = namespaceNotificationVersion
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetTimerMaxReadLevel(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()

	return s.timerMaxReadLevelMap[cluster]
}

func (s *shardContextImpl) UpdateTimerMaxReadLevel(cluster string) time.Time {
	s.Lock()
	defer s.Unlock()

	currentTime := s.GetTimeSource().Now()
	if cluster != "" && cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		currentTime = s.remoteClusterCurrentTime[cluster]
	}

	s.timerMaxReadLevelMap[cluster] = currentTime.Add(s.config.TimerProcessorMaxTimeShift()).Truncate(time.Millisecond)
	return s.timerMaxReadLevelMap[cluster]
}

func (s *shardContextImpl) CreateWorkflowExecution(
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {

	namespaceID := request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId

	// do not try to get namespace cache within shard lock
	namespaceEntry, err := s.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.NewWorkflowSnapshot.TransferTasks,
		request.NewWorkflowSnapshot.ReplicationTasks,
		request.NewWorkflowSnapshot.TimerTasks,
		&transferMaxReadLevel,
	); err != nil {
		return nil, err
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

Create_Loop:
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID

		response, err := s.executionManager.CreateWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *serviceerror.WorkflowExecutionAlreadyStarted,
				*persistence.WorkflowExecutionAlreadyStartedError,
				*persistence.TimeoutError,
				*serviceerror.ResourceExhausted:
				// No special handling required for these errors
			case *persistence.ShardOwnershipLostError:
				{
					// RangeID might have been renewed by the same host while this update was in flight
					// Retry the operation if we still have the shard ownership
					if currentRangeID != s.getRangeID() {
						continue Create_Loop
					} else {
						// Shard is stolen, trigger shutdown of history engine
						s.closeShard()
					}
				}
			default:
				{
					// We have no idea if the write failed or will eventually make it to
					// persistence. Increment RangeID to guarantee that subsequent reads
					// will either see that write, or know for certain that it failed.
					// This allows the callers to reliably check the outcome by performing
					// a read.
					err1 := s.renewRangeLocked(false)
					if err1 != nil {
						// At this point we have no choice but to unload the shard, so that it
						// gets a new RangeID when it's reloaded.
						s.closeShard()
						break Create_Loop
					}
				}
			}
		}

		return response, err
	}

	return nil, ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) UpdateWorkflowExecution(
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {

	namespaceID := request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId
	workflowID := request.UpdateWorkflowMutation.ExecutionInfo.WorkflowId

	// do not try to get namespace cache within shard lock
	namespaceEntry, err := s.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.UpdateWorkflowMutation.TransferTasks,
		request.UpdateWorkflowMutation.ReplicationTasks,
		request.UpdateWorkflowMutation.TimerTasks,
		&transferMaxReadLevel,
	); err != nil {
		return nil, err
	}
	if request.NewWorkflowSnapshot != nil {
		if err := s.allocateTaskIDsLocked(
			namespaceEntry,
			workflowID,
			request.NewWorkflowSnapshot.TransferTasks,
			request.NewWorkflowSnapshot.ReplicationTasks,
			request.NewWorkflowSnapshot.TimerTasks,
			&transferMaxReadLevel,
		); err != nil {
			return nil, err
		}
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

Update_Loop:
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		resp, err := s.executionManager.UpdateWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *persistence.ConditionFailedError,
				*serviceerror.ResourceExhausted:
				// No special handling required for these errors
			case *persistence.ShardOwnershipLostError:
				{
					// RangeID might have been renewed by the same host while this update was in flight
					// Retry the operation if we still have the shard ownership
					if currentRangeID != s.getRangeID() {
						continue Update_Loop
					} else {
						// Shard is stolen, trigger shutdown of history engine
						s.closeShard()
						break Update_Loop
					}
				}
			default:
				{
					// We have no idea if the write failed or will eventually make it to
					// persistence. Increment RangeID to guarantee that subsequent reads
					// will either see that write, or know for certain that it failed.
					// This allows the callers to reliably check the outcome by performing
					// a read.
					err1 := s.renewRangeLocked(false)
					if err1 != nil {
						// At this point we have no choice but to unload the shard, so that it
						// gets a new RangeID when it's reloaded.
						s.closeShard()
						break Update_Loop
					}
				}
			}
		}

		return resp, err
	}

	return nil, ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error {

	namespaceID := request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId

	// do not try to get namespace cache within shard lock
	namespaceEntry, err := s.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if request.CurrentWorkflowMutation != nil {
		if err := s.allocateTaskIDsLocked(
			namespaceEntry,
			workflowID,
			request.CurrentWorkflowMutation.TransferTasks,
			request.CurrentWorkflowMutation.ReplicationTasks,
			request.CurrentWorkflowMutation.TimerTasks,
			&transferMaxReadLevel,
		); err != nil {
			return err
		}
	}
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.NewWorkflowSnapshot.TransferTasks,
		request.NewWorkflowSnapshot.ReplicationTasks,
		request.NewWorkflowSnapshot.TimerTasks,
		&transferMaxReadLevel,
	); err != nil {
		return err
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

Reset_Loop:
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		err := s.executionManager.ResetWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *persistence.ConditionFailedError,
				*persistence.TimeoutError,
				*serviceerror.ResourceExhausted:
				// No special handling required for these errors
			case *persistence.ShardOwnershipLostError:
				{
					// RangeID might have been renewed by the same host while this update was in flight
					// Retry the operation if we still have the shard ownership
					if currentRangeID != s.getRangeID() {
						continue Reset_Loop
					} else {
						// Shard is stolen, trigger shutdown of history engine
						s.closeShard()
						break Reset_Loop
					}
				}
			default:
				{
					// We have no idea if the write failed or will eventually make it to
					// persistence. Increment RangeID to guarantee that subsequent reads
					// will either see that write, or know for certain that it failed.
					// This allows the callers to reliably check the outcome by performing
					// a read.
					err1 := s.renewRangeLocked(false)
					if err1 != nil {
						// At this point we have no choice but to unload the shard, so that it
						// gets a new RangeID when it's reloaded.
						s.closeShard()
						break Reset_Loop
					}
				}
			}
		}

		return err
	}

	return ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) ConflictResolveWorkflowExecution(
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) error {

	namespaceID := request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId
	workflowID := request.ResetWorkflowSnapshot.ExecutionInfo.WorkflowId

	// do not try to get namespace cache within shard lock
	namespaceEntry, err := s.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if request.CurrentWorkflowMutation != nil {
		if err := s.allocateTaskIDsLocked(
			namespaceEntry,
			workflowID,
			request.CurrentWorkflowMutation.TransferTasks,
			request.CurrentWorkflowMutation.ReplicationTasks,
			request.CurrentWorkflowMutation.TimerTasks,
			&transferMaxReadLevel,
		); err != nil {
			return err
		}
	}
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.ResetWorkflowSnapshot.TransferTasks,
		request.ResetWorkflowSnapshot.ReplicationTasks,
		request.ResetWorkflowSnapshot.TimerTasks,
		&transferMaxReadLevel,
	); err != nil {
		return err
	}
	if request.NewWorkflowSnapshot != nil {
		if err := s.allocateTaskIDsLocked(
			namespaceEntry,
			workflowID,
			request.NewWorkflowSnapshot.TransferTasks,
			request.NewWorkflowSnapshot.ReplicationTasks,
			request.NewWorkflowSnapshot.TimerTasks,
			&transferMaxReadLevel,
		); err != nil {
			return err
		}
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

Reset_Loop:
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		err := s.executionManager.ConflictResolveWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *persistence.ConditionFailedError,
				*serviceerror.ResourceExhausted:
				// No special handling required for these errors
			case *persistence.ShardOwnershipLostError:
				{
					// RangeID might have been renewed by the same host while this update was in flight
					// Retry the operation if we still have the shard ownership
					if currentRangeID != s.getRangeID() {
						continue Reset_Loop
					} else {
						// Shard is stolen, trigger shutdown of history engine
						s.closeShard()
						break Reset_Loop
					}
				}
			default:
				{
					// We have no idea if the write failed or will eventually make it to
					// persistence. Increment RangeID to guarantee that subsequent reads
					// will either see that write, or know for certain that it failed.
					// This allows the callers to reliably check the outcome by performing
					// a read.
					err1 := s.renewRangeLocked(false)
					if err1 != nil {
						// At this point we have no choice but to unload the shard, so that it
						// gets a new RangeID when it's reloaded.
						s.closeShard()
						break Reset_Loop
					}
				}
			}
		}

		return err
	}

	return ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) AppendHistoryV2Events(
	request *persistence.AppendHistoryNodesRequest, namespaceID string, execution commonpb.WorkflowExecution) (int, error) {

	// NOTE: do not use generateNextTransferTaskIDLocked since
	// generateNextTransferTaskIDLocked is not guarded by lock
	transactionID, err := s.GenerateTransferTaskID()
	if err != nil {
		return 0, err
	}

	request.ShardID = convert.Int32Ptr(s.shardID)
	request.TransactionID = transactionID

	size := 0
	defer func() {
		// N.B. - Dual emit here makes sense so that we can see aggregate timer stats across all
		// namespaces along with the individual namespaces stats
		s.GetMetricsClient().RecordTimer(metrics.SessionSizeStatsScope, metrics.HistorySize, time.Duration(size))
		if entry, err := s.GetNamespaceCache().GetNamespaceByID(namespaceID); err == nil && entry != nil && entry.GetInfo() != nil {
			s.GetMetricsClient().Scope(metrics.SessionSizeStatsScope, metrics.NamespaceTag(entry.GetInfo().Name)).RecordTimer(metrics.HistorySize, time.Duration(size))
		}
		if size >= historySizeLogThreshold {
			s.throttledLogger.Warn("history size threshold breached",
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.WorkflowNamespaceID(namespaceID),
				tag.WorkflowHistorySizeBytes(size))
		}
	}()
	resp, err0 := s.GetHistoryManager().AppendHistoryNodes(request)
	if resp != nil {
		size = resp.Size
	}
	return size, err0
}

func (s *shardContextImpl) GetConfig() *Config {
	return s.config
}

func (s *shardContextImpl) PreviousShardOwnerWasDifferent() bool {
	return s.previousShardOwnerWasDifferent
}

func (s *shardContextImpl) GetEventsCache() eventsCache {
	return s.eventsCache
}

func (s *shardContextImpl) GetLogger() log.Logger {
	return s.logger
}

func (s *shardContextImpl) GetThrottledLogger() log.Logger {
	return s.throttledLogger
}

func (s *shardContextImpl) getRangeID() int64 {
	return s.shardInfo.GetRangeId()
}

func (s *shardContextImpl) isClosed() bool {
	return atomic.LoadInt32(&s.closed) != 0
}

func (s *shardContextImpl) closeShard() {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return
	}

	s.logger.Info("Close shard")

	go func() {
		s.closeCallback(s.shardID, s.shardItem)
	}()

	// fails any writes that may start after this point.
	s.shardInfo.RangeId = -1
	atomic.StoreInt64(&s.rangeID, s.shardInfo.RangeId)
}

func (s *shardContextImpl) generateTransferTaskIDLocked() (int64, error) {
	if err := s.updateRangeIfNeededLocked(); err != nil {
		return -1, err
	}

	taskID := s.transferSequenceNumber
	s.transferSequenceNumber++

	return taskID, nil
}

func (s *shardContextImpl) updateRangeIfNeededLocked() error {
	if s.transferSequenceNumber < s.maxTransferSequenceNumber {
		return nil
	}

	return s.renewRangeLocked(false)
}

func (s *shardContextImpl) renewRangeLocked(isStealing bool) error {
	updatedShardInfo := copyShardInfo(s.shardInfo)
	updatedShardInfo.RangeId++
	if isStealing {
		updatedShardInfo.StolenSinceRenew++
	}

	err := s.GetShardManager().UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo.ShardInfo,
		PreviousRangeID: s.shardInfo.GetRangeId()})
	if err != nil {
		// Failure in updating shard to grab new RangeID
		s.logger.Error("Persistent store operation failure",
			tag.StoreOperationUpdateShard,
			tag.Error(err),
			tag.ShardRangeID(updatedShardInfo.GetRangeId()),
			tag.PreviousShardRangeID(s.shardInfo.GetRangeId()),
		)
		// Shard is stolen, trigger history engine shutdown
		if _, ok := err.(*persistence.ShardOwnershipLostError); ok {
			s.closeShard()
		}
		return err
	}

	// Range is successfully updated in cassandra now update shard context to reflect new range
	s.logger.Info("Range updated for shardID",
		tag.ShardRangeID(updatedShardInfo.RangeId),
		tag.PreviousShardRangeID(s.shardInfo.RangeId),
		tag.Number(s.transferSequenceNumber),
		tag.NextNumber(s.maxTransferSequenceNumber),
	)

	s.transferSequenceNumber = updatedShardInfo.GetRangeId() << s.config.RangeSizeBits
	s.maxTransferSequenceNumber = (updatedShardInfo.GetRangeId() + 1) << s.config.RangeSizeBits
	s.transferMaxReadLevel = s.transferSequenceNumber - 1
	atomic.StoreInt64(&s.rangeID, updatedShardInfo.GetRangeId())
	s.shardInfo = updatedShardInfo

	return nil
}

func (s *shardContextImpl) updateMaxReadLevelLocked(rl int64) {
	if rl > s.transferMaxReadLevel {
		s.logger.Debug("Updating MaxReadLevel", tag.MaxLevel(rl))
		s.transferMaxReadLevel = rl
	}
}

func (s *shardContextImpl) updateShardInfoLocked() error {
	if s.isClosed() {
		return ErrShardClosed
	}

	var err error
	now := clock.NewRealTimeSource().Now()
	if s.lastUpdated.Add(s.config.ShardUpdateMinInterval()).After(now) {
		return nil
	}
	updatedShardInfo := copyShardInfo(s.shardInfo)
	s.emitShardInfoMetricsLogsLocked()

	err = s.GetShardManager().UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo.ShardInfo,
		PreviousRangeID: s.shardInfo.GetRangeId(),
	})

	if err != nil {
		// Shard is stolen, trigger history engine shutdown
		if _, ok := err.(*persistence.ShardOwnershipLostError); ok {
			s.closeShard()
		}
	} else {
		s.lastUpdated = now
	}

	return err
}

func (s *shardContextImpl) emitShardInfoMetricsLogsLocked() {
	currentCluster := s.GetClusterMetadata().GetCurrentClusterName()

	minTransferLevel := s.shardInfo.ClusterTransferAckLevel[currentCluster]
	maxTransferLevel := s.shardInfo.ClusterTransferAckLevel[currentCluster]
	for _, v := range s.shardInfo.ClusterTransferAckLevel {
		if v < minTransferLevel {
			minTransferLevel = v
		}
		if v > maxTransferLevel {
			maxTransferLevel = v
		}
	}
	diffTransferLevel := maxTransferLevel - minTransferLevel

	minTimerLevel := timestamp.TimeValue(s.shardInfo.ClusterTimerAckLevel[currentCluster])
	maxTimerLevel := timestamp.TimeValue(s.shardInfo.ClusterTimerAckLevel[currentCluster])
	for _, v := range s.shardInfo.ClusterTimerAckLevel {
		t := timestamp.TimeValue(v)
		if t.Before(minTimerLevel) {
			minTimerLevel = t
		}
		if t.After(maxTimerLevel) {
			maxTimerLevel = t
		}
	}
	diffTimerLevel := maxTimerLevel.Sub(minTimerLevel)

	replicationLag := s.transferMaxReadLevel - s.shardInfo.ReplicationAckLevel
	transferLag := s.transferMaxReadLevel - s.shardInfo.TransferAckLevel
	timerLag := time.Since(timestamp.TimeValue(s.shardInfo.TimerAckLevelTime))

	transferFailoverInProgress := len(s.shardInfo.TransferFailoverLevels)
	timerFailoverInProgress := len(s.shardInfo.TimerFailoverLevels)

	if s.config.EmitShardDiffLog() &&
		(logWarnTransferLevelDiff < diffTransferLevel ||
			logWarnTimerLevelDiff < diffTimerLevel ||
			logWarnTransferLevelDiff < transferLag ||
			logWarnTimerLevelDiff < timerLag) {

		logger := s.logger.WithTags(
			tag.ShardTime(s.remoteClusterCurrentTime),
			tag.ShardReplicationAck(s.shardInfo.ReplicationAckLevel),
			tag.ShardTimerAcks(s.shardInfo.ClusterTimerAckLevel),
			tag.ShardTransferAcks(s.shardInfo.ClusterTransferAckLevel))

		logger.Warn("Shard ack levels diff exceeds warn threshold.")
	}

	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferDiffTimer, time.Duration(diffTransferLevel))
	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerDiffTimer, diffTimerLevel)

	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoReplicationLagTimer, time.Duration(replicationLag))
	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferLagTimer, time.Duration(transferLag))
	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerLagTimer, timerLag)

	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferFailoverInProgressTimer, time.Duration(transferFailoverInProgress))
	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerFailoverInProgressTimer, time.Duration(timerFailoverInProgress))
}

func (s *shardContextImpl) allocateTaskIDsLocked(
	namespaceEntry *cache.NamespaceCacheEntry,
	workflowID string,
	transferTasks []persistence.Task,
	replicationTasks []persistence.Task,
	timerTasks []persistence.Task,
	transferMaxReadLevel *int64,
) error {

	if err := s.allocateTransferIDsLocked(
		transferTasks,
		transferMaxReadLevel); err != nil {
		return err
	}
	if err := s.allocateTransferIDsLocked(
		replicationTasks,
		transferMaxReadLevel); err != nil {
		return err
	}
	return s.allocateTimerIDsLocked(
		namespaceEntry,
		workflowID,
		timerTasks)
}

func (s *shardContextImpl) allocateTransferIDsLocked(
	tasks []persistence.Task,
	transferMaxReadLevel *int64,
) error {

	for _, task := range tasks {
		id, err := s.generateTransferTaskIDLocked()
		if err != nil {
			return err
		}
		s.logger.Debug("Assigning task ID", tag.TaskID(id))
		task.SetTaskID(id)
		*transferMaxReadLevel = id
	}
	return nil
}

// NOTE: allocateTimerIDsLocked should always been called after assigning taskID for transferTasks when assigning taskID together,
// because Temporal Indexer assume timer taskID of deleteWorkflowExecution is larger than transfer taskID of closeWorkflowExecution
// for a given workflow.
func (s *shardContextImpl) allocateTimerIDsLocked(
	namespaceEntry *cache.NamespaceCacheEntry,
	workflowID string,
	timerTasks []persistence.Task,
) error {

	// assign IDs for the timer tasks. They need to be assigned under shard lock.
	currentCluster := s.GetClusterMetadata().GetCurrentClusterName()
	for _, task := range timerTasks {
		ts := task.GetVisibilityTimestamp()
		if task.GetVersion() != common.EmptyVersion {
			// cannot use version to determine the corresponding cluster for timer task
			// this is because during failover, timer task should be created as active
			// or otherwise, failover + active processing logic may not pick up the task.
			currentCluster = namespaceEntry.GetReplicationConfig().ActiveClusterName
		}
		readCursorTS := s.timerMaxReadLevelMap[currentCluster]
		if ts.Before(readCursorTS) {
			// This can happen if shard move and new host have a time SKU, or there is db write delay.
			// We generate a new timer ID using timerMaxReadLevel.
			s.logger.Debug("New timer generated is less than read level",
				tag.WorkflowNamespaceID(namespaceEntry.GetInfo().Id),
				tag.WorkflowID(workflowID),
				tag.Timestamp(ts),
				tag.CursorTimestamp(readCursorTS),
				tag.ValueShardAllocateTimerBeforeRead)
			task.SetVisibilityTimestamp(s.timerMaxReadLevelMap[currentCluster].Add(time.Millisecond))
		}

		seqNum, err := s.generateTransferTaskIDLocked()
		if err != nil {
			return err
		}
		task.SetTaskID(seqNum)
		visibilityTs := task.GetVisibilityTimestamp()
		s.logger.Debug("Assigning new timer",
			tag.Timestamp(visibilityTs), tag.TaskID(task.GetTaskID()), tag.AckLevel(s.shardInfo.TimerAckLevelTime))
	}
	return nil
}

func (s *shardContextImpl) SetCurrentTime(cluster string, currentTime time.Time) {
	s.Lock()
	defer s.Unlock()
	if cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		prevTime := s.remoteClusterCurrentTime[cluster]
		if prevTime.Before(currentTime) {
			s.remoteClusterCurrentTime[cluster] = currentTime
		}
	} else {
		panic("Cannot set current time for current cluster")
	}
}

func (s *shardContextImpl) GetCurrentTime(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()
	if cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		return s.remoteClusterCurrentTime[cluster]
	}
	return s.GetTimeSource().Now().UTC()
}

func (s *shardContextImpl) GetLastUpdatedTime() time.Time {
	s.RLock()
	defer s.RUnlock()
	return s.lastUpdated
}

func acquireShard(
	shardItem *historyShardsItem,
	closeCallback func(int32, *historyShardsItem),
) (ShardContext, error) {

	var shardInfo *persistence.ShardInfoWithFailover

	retryPolicy := backoff.NewExponentialRetryPolicy(50 * time.Millisecond)
	retryPolicy.SetMaximumInterval(time.Second)
	retryPolicy.SetExpirationInterval(5 * time.Second)

	retryPredicate := func(err error) bool {
		if common.IsPersistenceTransientError(err) {
			return true
		}
		_, ok := err.(*persistence.ShardAlreadyExistError)
		return ok

	}

	getShard := func() error {
		resp, err := shardItem.GetShardManager().GetShard(&persistence.GetShardRequest{
			ShardID: int32(shardItem.shardID),
		})
		if err == nil {
			shardInfo = &persistence.ShardInfoWithFailover{ShardInfo: resp.ShardInfo}
			return nil
		}
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}

		// EntityNotExistsError error
		shardInfo = &persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId: int32(shardItem.shardID),
			},
		}
		return shardItem.GetShardManager().CreateShard(&persistence.CreateShardRequest{ShardInfo: shardInfo.ShardInfo})
	}

	err := backoff.Retry(getShard, retryPolicy, retryPredicate)
	if err != nil {
		shardItem.logger.Error("Fail to acquire shard.", tag.ShardID(shardItem.shardID), tag.Error(err))
		return nil, err
	}

	updatedShardInfo := copyShardInfo(shardInfo)
	ownershipChanged := shardInfo.Owner != shardItem.GetHostInfo().Identity()
	updatedShardInfo.Owner = shardItem.GetHostInfo().Identity()

	// initialize the cluster current time to be the same as ack level
	remoteClusterCurrentTime := make(map[string]time.Time)
	timerMaxReadLevelMap := make(map[string]time.Time)
	for clusterName, info := range shardItem.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		currentReadTime := timestamp.TimeValue(shardInfo.TimerAckLevelTime)
		if clusterName != shardItem.GetClusterMetadata().GetCurrentClusterName() {
			if currentTime, ok := shardInfo.ClusterTimerAckLevel[clusterName]; ok {
				currentReadTime = timestamp.TimeValue(currentTime)
			}

			remoteClusterCurrentTime[clusterName] = currentReadTime
			timerMaxReadLevelMap[clusterName] = currentReadTime
		} else { // active cluster
			timerMaxReadLevelMap[clusterName] = currentReadTime
		}

		timerMaxReadLevelMap[clusterName] = timerMaxReadLevelMap[clusterName].Truncate(time.Millisecond)
	}

	executionMgr, err := shardItem.GetExecutionManager(shardItem.shardID)
	if err != nil {
		return nil, err
	}

	shardContext := &shardContextImpl{
		Resource:                       shardItem.Resource,
		shardItem:                      shardItem,
		shardID:                        shardItem.shardID,
		executionManager:               executionMgr,
		shardInfo:                      updatedShardInfo,
		closeCallback:                  closeCallback,
		config:                         shardItem.config,
		remoteClusterCurrentTime:       remoteClusterCurrentTime,
		timerMaxReadLevelMap:           timerMaxReadLevelMap, // use ack to init read level
		logger:                         shardItem.logger,
		throttledLogger:                shardItem.throttledLogger,
		previousShardOwnerWasDifferent: ownershipChanged,
	}
	shardContext.eventsCache = newEventsCache(shardContext)

	err1 := shardContext.renewRangeLocked(true)
	if err1 != nil {
		return nil, err1
	}

	shardItem.logger.Info("Acquired shard")

	return shardContext, nil
}

func copyShardInfo(shardInfo *persistence.ShardInfoWithFailover) *persistence.ShardInfoWithFailover {
	transferFailoverLevels := map[string]persistence.TransferFailoverLevel{}
	for k, v := range shardInfo.TransferFailoverLevels {
		transferFailoverLevels[k] = v
	}
	timerFailoverLevels := map[string]persistence.TimerFailoverLevel{}
	for k, v := range shardInfo.TimerFailoverLevels {
		timerFailoverLevels[k] = v
	}
	clusterTransferAckLevel := make(map[string]int64)
	for k, v := range shardInfo.ClusterTransferAckLevel {
		clusterTransferAckLevel[k] = v
	}
	clusterTimerAckLevel := make(map[string]*time.Time)
	for k, v := range shardInfo.ClusterTimerAckLevel {
		clusterTimerAckLevel[k] = v
	}
	clusterReplicationLevel := make(map[string]int64)
	for k, v := range shardInfo.ClusterReplicationLevel {
		clusterReplicationLevel[k] = v
	}
	shardInfoCopy := &persistence.ShardInfoWithFailover{
		ShardInfo: &persistenceblobs.ShardInfo{
			ShardId:                      shardInfo.GetShardId(),
			Owner:                        shardInfo.Owner,
			RangeId:                      shardInfo.GetRangeId(),
			StolenSinceRenew:             shardInfo.StolenSinceRenew,
			ReplicationAckLevel:          shardInfo.ReplicationAckLevel,
			TransferAckLevel:             shardInfo.TransferAckLevel,
			TimerAckLevelTime:            shardInfo.TimerAckLevelTime,
			ClusterTransferAckLevel:      clusterTransferAckLevel,
			ClusterTimerAckLevel:         clusterTimerAckLevel,
			NamespaceNotificationVersion: shardInfo.NamespaceNotificationVersion,
			ClusterReplicationLevel:      clusterReplicationLevel,
			UpdateTime:                   shardInfo.UpdateTime,
		},
		TransferFailoverLevels: transferFailoverLevels,
		TimerFailoverLevels:    timerFailoverLevels,
	}

	return shardInfoCopy
}
