// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	// ShardContext represents a history engine shard
	ShardContext interface {
		GetShardID() int
		GetService() service.Service
		GetExecutionManager() persistence.ExecutionManager
		GetHistoryV2Manager() persistence.HistoryV2Manager
		GetDomainCache() cache.DomainCache
		GetClusterMetadata() cluster.Metadata
		GetConfig() *Config
		GetEventsCache() eventsCache
		GetLogger() log.Logger
		GetThrottledLogger() log.Logger
		GetMetricsClient() metrics.Client
		GetTimeSource() clock.TimeSource

		GetEngine() Engine
		SetEngine(Engine)

		GenerateTransferTaskID() (int64, error)
		GenerateTransferTaskIDs(number int) ([]int64, error)

		GetTransferMaxReadLevel() int64
		UpdateTimerMaxReadLevel(cluster string) time.Time

		SetCurrentTime(cluster string, currentTime time.Time)
		GetCurrentTime(cluster string) time.Time
		GetTimerMaxReadLevel(cluster string) time.Time

		GetTransferAckLevel() int64
		UpdateTransferAckLevel(ackLevel int64) error
		GetTransferClusterAckLevel(cluster string) int64
		UpdateTransferClusterAckLevel(cluster string, ackLevel int64) error

		GetReplicatorAckLevel() int64
		UpdateReplicatorAckLevel(ackLevel int64) error

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

		GetDomainNotificationVersion() int64
		UpdateDomainNotificationVersion(domainNotificationVersion int64) error

		CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error)
		ConflictResolveWorkflowExecution(request *persistence.ConflictResolveWorkflowExecutionRequest) error
		ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error
		AppendHistoryV2Events(request *persistence.AppendHistoryNodesRequest, domainID string, execution shared.WorkflowExecution) (int, error)
	}

	shardContextImpl struct {
		shardItem        *historyShardsItem
		shardID          int
		clusterMetadata  cluster.Metadata
		service          service.Service
		rangeID          int64
		shardManager     persistence.ShardManager
		historyV2Mgr     persistence.HistoryV2Manager
		executionManager persistence.ExecutionManager
		domainCache      cache.DomainCache
		eventsCache      eventsCache
		closeCh          chan<- int
		isClosed         bool
		config           *Config
		logger           log.Logger
		throttledLogger  log.Logger
		metricsClient    metrics.Client
		timeSource       clock.TimeSource
		engine           Engine

		sync.RWMutex
		lastUpdated               time.Time
		shardInfo                 *persistence.ShardInfo
		transferSequenceNumber    int64
		maxTransferSequenceNumber int64
		transferMaxReadLevel      int64
		timerMaxReadLevelMap      map[string]time.Time // cluster -> timerMaxReadLevel

		// exist only in memory
		standbyClusterCurrentTime map[string]time.Time
	}
)

var _ ShardContext = (*shardContextImpl)(nil)

const (
	logWarnTransferLevelDiff = 3000000 // 3 million
	logWarnTimerLevelDiff    = time.Duration(30 * time.Minute)
	historySizeLogThreshold  = 10 * 1024 * 1024
)

func (s *shardContextImpl) GetShardID() int {
	return s.shardID
}

func (s *shardContextImpl) GetService() service.Service {
	return s.service
}

func (s *shardContextImpl) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

func (s *shardContextImpl) GetHistoryV2Manager() persistence.HistoryV2Manager {
	return s.historyV2Mgr
}

func (s *shardContextImpl) GetDomainCache() cache.DomainCache {
	return s.domainCache
}

func (s *shardContextImpl) GetClusterMetadata() cluster.Metadata {
	return s.clusterMetadata
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

	return s.shardInfo.TimerAckLevel
}

func (s *shardContextImpl) UpdateTimerAckLevel(ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *shardContextImpl) GetTimerClusterAckLevel(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding ack level
	if ackLevel, ok := s.shardInfo.ClusterTimerAckLevel[cluster]; ok {
		return ackLevel
	}
	// otherwise, default to existing ack level, which belongs to local cluster
	// this can happen if you add more cluster
	return s.shardInfo.TimerAckLevel
}

func (s *shardContextImpl) UpdateTimerClusterAckLevel(cluster string, ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTimerAckLevel[cluster] = ackLevel
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
		s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferFailoverLatencyTimer, time.Since(level.StartTime))
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
		s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerFailoverLatencyTimer, time.Since(level.StartTime))
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

func (s *shardContextImpl) GetDomainNotificationVersion() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.DomainNotificationVersion
}

func (s *shardContextImpl) UpdateDomainNotificationVersion(domainNotificationVersion int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.DomainNotificationVersion = domainNotificationVersion
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
	if cluster != "" && cluster != s.GetService().GetClusterMetadata().GetCurrentClusterName() {
		currentTime = s.standbyClusterCurrentTime[cluster]
	}

	s.timerMaxReadLevelMap[cluster] = currentTime.Add(s.config.TimerProcessorMaxTimeShift())
	return s.timerMaxReadLevelMap[cluster]
}

func (s *shardContextImpl) CreateWorkflowExecution(
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {

	domainID := request.NewWorkflowSnapshot.ExecutionInfo.DomainID
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.domainCache.GetDomainByID(domainID)
	if err != nil {
		return nil, err
	}

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		domainEntry,
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
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID

		response, err := s.executionManager.CreateWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *shared.WorkflowExecutionAlreadyStartedError,
				*persistence.WorkflowExecutionAlreadyStartedError,
				*shared.ServiceBusyError,
				*persistence.TimeoutError,
				*shared.LimitExceededError:
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
					}
				}
			}
		}

		return response, err
	}

	return nil, ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) getDefaultEncoding(domainEntry *cache.DomainCacheEntry) common.EncodingType {
	return common.EncodingType(s.config.EventEncodingType(domainEntry.GetInfo().Name))
}

func (s *shardContextImpl) UpdateWorkflowExecution(
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {

	domainID := request.UpdateWorkflowMutation.ExecutionInfo.DomainID
	workflowID := request.UpdateWorkflowMutation.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.domainCache.GetDomainByID(domainID)
	if err != nil {
		return nil, err
	}
	request.Encoding = s.getDefaultEncoding(domainEntry)

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		domainEntry,
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
			domainEntry,
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
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		resp, err := s.executionManager.UpdateWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *persistence.ConditionFailedError,
				*shared.ServiceBusyError,
				*shared.LimitExceededError:
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
					}
				}
			}
		}

		return resp, err
	}

	return nil, ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error {

	domainID := request.NewWorkflowSnapshot.ExecutionInfo.DomainID
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.domainCache.GetDomainByID(domainID)
	if err != nil {
		return err
	}
	request.Encoding = s.getDefaultEncoding(domainEntry)

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if request.CurrentWorkflowMutation != nil {
		if err := s.allocateTaskIDsLocked(
			domainEntry,
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
		domainEntry,
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
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		err := s.executionManager.ResetWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *persistence.ConditionFailedError,
				*shared.ServiceBusyError,
				*persistence.TimeoutError,
				*shared.LimitExceededError:
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

	domainID := request.ResetWorkflowSnapshot.ExecutionInfo.DomainID
	workflowID := request.ResetWorkflowSnapshot.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.domainCache.GetDomainByID(domainID)
	if err != nil {
		return err
	}
	request.Encoding = s.getDefaultEncoding(domainEntry)

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if request.CurrentWorkflowMutation != nil {
		if err := s.allocateTaskIDsLocked(
			domainEntry,
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
		domainEntry,
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
			domainEntry,
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
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		currentRangeID := s.getRangeID()
		request.RangeID = currentRangeID
		err := s.executionManager.ConflictResolveWorkflowExecution(request)
		if err != nil {
			switch err.(type) {
			case *persistence.ConditionFailedError,
				*shared.ServiceBusyError,
				*shared.LimitExceededError:
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
					}
				}
			}
		}

		return err
	}

	return ErrMaxAttemptsExceeded
}

func (s *shardContextImpl) AppendHistoryV2Events(
	request *persistence.AppendHistoryNodesRequest, domainID string, execution shared.WorkflowExecution) (int, error) {

	domainEntry, err := s.domainCache.GetDomainByID(domainID)
	if err != nil {
		return 0, err
	}

	// NOTE: do not use generateNextTransferTaskIDLocked since
	// generateNextTransferTaskIDLocked is not guarded by lock
	transactionID, err := s.GenerateTransferTaskID()
	if err != nil {
		return 0, err
	}

	request.Encoding = s.getDefaultEncoding(domainEntry)
	request.ShardID = common.IntPtr(s.shardID)
	request.TransactionID = transactionID

	size := 0
	defer func() {
		// N.B. - Dual emit here makes sense so that we can see aggregate timer stats across all
		// domains along with the individual domains stats
		s.metricsClient.RecordTimer(metrics.SessionSizeStatsScope, metrics.HistorySize, time.Duration(size))
		if entry, err := s.domainCache.GetDomainByID(domainID); err == nil && entry != nil && entry.GetInfo() != nil {
			s.metricsClient.Scope(metrics.SessionSizeStatsScope, metrics.DomainTag(entry.GetInfo().Name)).RecordTimer(metrics.HistorySize, time.Duration(size))
		}
		if size >= historySizeLogThreshold {
			s.throttledLogger.Warn("history size threshold breached",
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.WorkflowDomainID(domainID),
				tag.WorkflowHistorySizeBytes(size))
		}
	}()
	resp, err0 := s.historyV2Mgr.AppendHistoryNodes(request)
	if resp != nil {
		size = resp.Size
	}
	return size, err0
}

func (s *shardContextImpl) GetConfig() *Config {
	return s.config
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

func (s *shardContextImpl) GetMetricsClient() metrics.Client {
	return s.metricsClient
}

func (s *shardContextImpl) getRangeID() int64 {
	return s.shardInfo.RangeID
}

func (s *shardContextImpl) closeShard() {
	if s.isClosed {
		return
	}

	s.isClosed = true

	go s.shardItem.stopEngine()

	// fails any writes that may start after this point.
	s.shardInfo.RangeID = -1
	atomic.StoreInt64(&s.rangeID, s.shardInfo.RangeID)

	if s.closeCh != nil {
		// This is the channel passed in by shard controller to monitor if a shard needs to be unloaded
		// It will trigger the HistoryEngine unload and removal of engine from shard controller
		s.closeCh <- s.shardID
	}
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
			s.closeShard()
		} else {
			// Failure in updating shard to grab new RangeID
			s.logger.Error("Persistent store operation failure",
				tag.StoreOperationUpdateShard,
				tag.Error(err),
				tag.ShardRangeID(updatedShardInfo.RangeID),
				tag.PreviousShardRangeID(s.shardInfo.RangeID))
		}
		return err
	}

	// Range is successfully updated in cassandra now update shard context to reflect new range
	s.transferSequenceNumber = updatedShardInfo.RangeID << s.config.RangeSizeBits
	s.maxTransferSequenceNumber = (updatedShardInfo.RangeID + 1) << s.config.RangeSizeBits
	s.transferMaxReadLevel = s.transferSequenceNumber - 1
	atomic.StoreInt64(&s.rangeID, updatedShardInfo.RangeID)
	s.shardInfo = updatedShardInfo

	s.logger.Info("Range updated for shardID",
		tag.ShardID(s.shardInfo.ShardID),
		tag.ShardRangeID(s.shardInfo.RangeID),
		tag.Number(s.transferSequenceNumber),
		tag.NextNumber(s.maxTransferSequenceNumber))
	return nil
}

func (s *shardContextImpl) updateMaxReadLevelLocked(rl int64) {
	if rl > s.transferMaxReadLevel {
		s.logger.Debug(fmt.Sprintf("Updating MaxReadLevel: %v", rl))
		s.transferMaxReadLevel = rl
	}
}

func (s *shardContextImpl) updateShardInfoLocked() error {
	var err error
	now := clock.NewRealTimeSource().Now()
	if s.lastUpdated.Add(s.config.ShardUpdateMinInterval()).After(now) {
		return nil
	}
	updatedShardInfo := copyShardInfo(s.shardInfo)
	s.emitShardInfoMetricsLogsLocked()

	err = s.shardManager.UpdateShard(&persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: s.shardInfo.RangeID,
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
	currentCluster := s.clusterMetadata.GetCurrentClusterName()

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

	minTimerLevel := s.shardInfo.ClusterTimerAckLevel[currentCluster]
	maxTimerLevel := s.shardInfo.ClusterTimerAckLevel[currentCluster]
	for _, v := range s.shardInfo.ClusterTimerAckLevel {
		if v.Before(minTimerLevel) {
			minTimerLevel = v
		}
		if v.After(maxTimerLevel) {
			maxTimerLevel = v
		}
	}
	diffTimerLevel := maxTimerLevel.Sub(minTimerLevel)

	replicationLag := s.transferMaxReadLevel - s.shardInfo.ReplicationAckLevel
	transferLag := s.transferMaxReadLevel - s.shardInfo.TransferAckLevel
	timerLag := time.Since(s.shardInfo.TimerAckLevel)

	transferFailoverInProgress := len(s.shardInfo.TransferFailoverLevels)
	timerFailoverInProgress := len(s.shardInfo.TimerFailoverLevels)

	if s.config.EmitShardDiffLog() &&
		(logWarnTransferLevelDiff < diffTransferLevel ||
			logWarnTimerLevelDiff < diffTimerLevel ||
			logWarnTransferLevelDiff < transferLag ||
			logWarnTimerLevelDiff < timerLag) {

		logger := s.logger.WithTags(
			tag.ShardTime(s.standbyClusterCurrentTime),
			tag.ShardReplicationAck(s.shardInfo.ReplicationAckLevel),
			tag.ShardTimerAcks(s.shardInfo.ClusterTimerAckLevel),
			tag.ShardTransferAcks(s.shardInfo.ClusterTransferAckLevel))

		logger.Warn("Shard ack levels diff exceeds warn threshold.")
	}

	s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferDiffTimer, time.Duration(diffTransferLevel))
	s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerDiffTimer, diffTimerLevel)

	s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoReplicationLagTimer, time.Duration(replicationLag))
	s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferLagTimer, time.Duration(transferLag))
	s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerLagTimer, timerLag)

	s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferFailoverInProgressTimer, time.Duration(transferFailoverInProgress))
	s.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerFailoverInProgressTimer, time.Duration(timerFailoverInProgress))
}

func (s *shardContextImpl) allocateTaskIDsLocked(
	domainEntry *cache.DomainCacheEntry,
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
		domainEntry,
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
		s.logger.Debug(fmt.Sprintf("Assigning task ID: %v", id))
		task.SetTaskID(id)
		*transferMaxReadLevel = id
	}
	return nil
}

// NOTE: allocateTimerIDsLocked should always been called after assigning taskID for transferTasks when assigning taskID together,
// because Cadence Indexer assume timer taskID of deleteWorkflowExecution is larger than transfer taskID of closeWorkflowExecution
// for a given workflow.
func (s *shardContextImpl) allocateTimerIDsLocked(
	domainEntry *cache.DomainCacheEntry,
	workflowID string,
	timerTasks []persistence.Task,
) error {

	// assign IDs for the timer tasks. They need to be assigned under shard lock.
	currentCluster := s.clusterMetadata.GetCurrentClusterName()
	for _, task := range timerTasks {
		ts := task.GetVisibilityTimestamp()
		if task.GetVersion() != common.EmptyVersion {
			// cannot use version to determine the corresponding cluster for timer task
			// this is because during failover, timer task should be created as active
			// or otherwise, failover + active processing logic may not pick up the task.
			currentCluster = domainEntry.GetReplicationConfig().ActiveClusterName
		}
		readCursorTS := s.timerMaxReadLevelMap[currentCluster]
		if ts.Before(readCursorTS) {
			// This can happen if shard move and new host have a time SKU, or there is db write delay.
			// We generate a new timer ID using timerMaxReadLevel.
			s.logger.Warn("New timer generated is less than read level",
				tag.WorkflowDomainID(domainEntry.GetInfo().ID),
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
		s.logger.Debug(fmt.Sprintf("Assigning new timer (timestamp: %v, seq: %v)) ackLeveL: %v",
			visibilityTs, task.GetTaskID(), s.shardInfo.TimerAckLevel))
	}
	return nil
}

func (s *shardContextImpl) GetTimeSource() clock.TimeSource {
	return s.timeSource
}

func (s *shardContextImpl) SetCurrentTime(cluster string, currentTime time.Time) {
	s.Lock()
	defer s.Unlock()
	if cluster != s.GetService().GetClusterMetadata().GetCurrentClusterName() {
		prevTime := s.standbyClusterCurrentTime[cluster]
		if prevTime.Before(currentTime) {
			s.standbyClusterCurrentTime[cluster] = currentTime
		}
	} else {
		panic("Cannot set current time for current cluster")
	}
}

func (s *shardContextImpl) GetCurrentTime(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()
	if cluster != s.GetService().GetClusterMetadata().GetCurrentClusterName() {
		return s.standbyClusterCurrentTime[cluster]
	}
	return s.GetTimeSource().Now()
}

func acquireShard(shardItem *historyShardsItem, closeCh chan<- int) (ShardContext,
	error) {

	var shardInfo *persistence.ShardInfo

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
		resp, err := shardItem.shardMgr.GetShard(&persistence.GetShardRequest{
			ShardID: shardItem.shardID,
		})
		if err == nil {
			shardInfo = resp.ShardInfo
			return nil
		}
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return err
		}

		// EntityNotExistsError error
		shardInfo = &persistence.ShardInfo{
			ShardID:          shardItem.shardID,
			RangeID:          0,
			TransferAckLevel: 0,
		}
		return shardItem.shardMgr.CreateShard(&persistence.CreateShardRequest{ShardInfo: shardInfo})
	}

	err := backoff.Retry(getShard, retryPolicy, retryPredicate)
	if err != nil {
		shardItem.logger.Error("Fail to acquire shard.", tag.ShardID(shardItem.shardID), tag.Error(err))
		return nil, err
	}

	updatedShardInfo := copyShardInfo(shardInfo)
	updatedShardInfo.Owner = shardItem.host.Identity()

	// initialize the cluster current time to be the same as ack level
	standbyClusterCurrentTime := make(map[string]time.Time)
	timerMaxReadLevelMap := make(map[string]time.Time)
	for clusterName, info := range shardItem.service.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		if clusterName != shardItem.service.GetClusterMetadata().GetCurrentClusterName() {
			if currentTime, ok := shardInfo.ClusterTimerAckLevel[clusterName]; ok {
				standbyClusterCurrentTime[clusterName] = currentTime
				timerMaxReadLevelMap[clusterName] = currentTime
			} else {
				standbyClusterCurrentTime[clusterName] = shardInfo.TimerAckLevel
				timerMaxReadLevelMap[clusterName] = shardInfo.TimerAckLevel
			}
		} else { // active cluster
			timerMaxReadLevelMap[clusterName] = shardInfo.TimerAckLevel
		}
	}

	context := &shardContextImpl{
		shardItem:                 shardItem,
		shardID:                   shardItem.shardID,
		clusterMetadata:           shardItem.service.GetClusterMetadata(),
		service:                   shardItem.service,
		shardManager:              shardItem.shardMgr,
		historyV2Mgr:              shardItem.historyV2Mgr,
		executionManager:          shardItem.executionMgr,
		domainCache:               shardItem.domainCache,
		shardInfo:                 updatedShardInfo,
		closeCh:                   closeCh,
		metricsClient:             shardItem.metricsClient,
		config:                    shardItem.config,
		timeSource:                shardItem.service.GetTimeSource(),
		standbyClusterCurrentTime: standbyClusterCurrentTime,
		timerMaxReadLevelMap:      timerMaxReadLevelMap, // use ack to init read level
	}
	context.logger = shardItem.logger
	context.throttledLogger = shardItem.throttledLogger
	context.eventsCache = newEventsCache(context)

	err1 := context.renewRangeLocked(true)
	if err1 != nil {
		return nil, err1
	}

	return context, nil
}

func copyShardInfo(shardInfo *persistence.ShardInfo) *persistence.ShardInfo {
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
	clusterTimerAckLevel := make(map[string]time.Time)
	for k, v := range shardInfo.ClusterTimerAckLevel {
		clusterTimerAckLevel[k] = v
	}
	clusterReplicationLevel := make(map[string]int64)
	for k, v := range shardInfo.ClusterReplicationLevel {
		clusterReplicationLevel[k] = v
	}
	shardInfoCopy := &persistence.ShardInfo{
		ShardID:                   shardInfo.ShardID,
		Owner:                     shardInfo.Owner,
		RangeID:                   shardInfo.RangeID,
		StolenSinceRenew:          shardInfo.StolenSinceRenew,
		ReplicationAckLevel:       shardInfo.ReplicationAckLevel,
		TransferAckLevel:          shardInfo.TransferAckLevel,
		TimerAckLevel:             shardInfo.TimerAckLevel,
		TransferFailoverLevels:    transferFailoverLevels,
		TimerFailoverLevels:       timerFailoverLevels,
		ClusterTransferAckLevel:   clusterTransferAckLevel,
		ClusterTimerAckLevel:      clusterTimerAckLevel,
		DomainNotificationVersion: shardInfo.DomainNotificationVersion,
		ClusterReplicationLevel:   clusterReplicationLevel,
	}

	return shardInfoCopy
}
