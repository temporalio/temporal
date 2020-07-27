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

package shard

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/.gen/go/replicator"
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
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/resource"
)

type (
	// Context represents a history engine shard
	Context interface {
		GetShardID() int
		GetService() resource.Resource
		GetExecutionManager() persistence.ExecutionManager
		GetHistoryManager() persistence.HistoryManager
		GetDomainCache() cache.DomainCache
		GetClusterMetadata() cluster.Metadata
		GetConfig() *config.Config
		GetEventsCache() events.Cache
		GetLogger() log.Logger
		GetThrottledLogger() log.Logger
		GetMetricsClient() metrics.Client
		GetTimeSource() clock.TimeSource
		PreviousShardOwnerWasDifferent() bool

		GetEngine() engine.Engine
		SetEngine(engine.Engine)

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

		GetDomainNotificationVersion() int64
		UpdateDomainNotificationVersion(domainNotificationVersion int64) error

		CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error)
		UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error)
		ConflictResolveWorkflowExecution(request *persistence.ConflictResolveWorkflowExecutionRequest) error
		ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error
		AppendHistoryV2Events(request *persistence.AppendHistoryNodesRequest, domainID string, execution shared.WorkflowExecution) (int, error)

		ReplicateFailoverMarkers(makers []*persistence.FailoverMarkerTask) error
		AddingPendingFailoverMarker(*replicator.FailoverMarkerAttributes) error
		ValidateAndUpdateFailoverMarkers() ([]*replicator.FailoverMarkerAttributes, error)
	}

	contextImpl struct {
		resource.Resource

		shardItem        *historyShardsItem
		shardID          int
		rangeID          int64
		executionManager persistence.ExecutionManager
		eventsCache      events.Cache
		closeCallback    func(int, *historyShardsItem)
		closed           int32
		config           *config.Config
		logger           log.Logger
		throttledLogger  log.Logger
		engine           engine.Engine

		sync.RWMutex
		lastUpdated               time.Time
		shardInfo                 *persistence.ShardInfo
		transferSequenceNumber    int64
		maxTransferSequenceNumber int64
		transferMaxReadLevel      int64
		timerMaxReadLevelMap      map[string]time.Time // cluster -> timerMaxReadLevel
		pendingFailoverMarkers    []*replicator.FailoverMarkerAttributes

		// exist only in memory
		remoteClusterCurrentTime map[string]time.Time

		// true if previous owner was different from the acquirer's identity.
		previousShardOwnerWasDifferent bool
	}
)

var _ Context = (*contextImpl)(nil)

var (
	// ErrShardClosed is returned when shard is closed and a req cannot be processed
	ErrShardClosed = errors.New("shard closed")
)

var (
	errMaxAttemptsExceeded = errors.New("maximum attempts exceeded to update history")
)

const (
	conditionalRetryCount    = 5
	logWarnTransferLevelDiff = 3000000 // 3 million
	logWarnTimerLevelDiff    = time.Duration(30 * time.Minute)
	historySizeLogThreshold  = 10 * 1024 * 1024
)

func (s *contextImpl) GetShardID() int {
	return s.shardID
}

func (s *contextImpl) GetService() resource.Resource {
	return s.Resource
}

func (s *contextImpl) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

func (s *contextImpl) GetEngine() engine.Engine {
	return s.engine
}

func (s *contextImpl) SetEngine(engine engine.Engine) {
	s.engine = engine
}

func (s *contextImpl) GenerateTransferTaskID() (int64, error) {
	s.Lock()
	defer s.Unlock()

	return s.generateTransferTaskIDLocked()
}

func (s *contextImpl) GenerateTransferTaskIDs(number int) ([]int64, error) {
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

func (s *contextImpl) GetTransferMaxReadLevel() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.transferMaxReadLevel
}

func (s *contextImpl) GetTransferAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.TransferAckLevel
}

func (s *contextImpl) UpdateTransferAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetTransferClusterAckLevel(cluster string) int64 {
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

func (s *contextImpl) UpdateTransferClusterAckLevel(cluster string, ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTransferAckLevel[cluster] = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetReplicatorAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.ReplicationAckLevel
}

func (s *contextImpl) UpdateReplicatorAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()
	s.shardInfo.ReplicationAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetReplicatorDLQAckLevel(sourceCluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	if ackLevel, ok := s.shardInfo.ReplicationDLQAckLevel[sourceCluster]; ok {
		return ackLevel
	}
	return -1
}

func (s *contextImpl) UpdateReplicatorDLQAckLevel(
	sourceCluster string,
	ackLevel int64,
) error {

	s.Lock()
	defer s.Unlock()

	s.shardInfo.ReplicationDLQAckLevel[sourceCluster] = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	if err := s.updateShardInfoLocked(); err != nil {
		return err
	}

	s.GetMetricsClient().Scope(
		metrics.ReplicationDLQStatsScope,
		metrics.TargetClusterTag(sourceCluster),
		metrics.InstanceTag(strconv.Itoa(s.shardID)),
	).UpdateGauge(
		metrics.ReplicationDLQAckLevelGauge,
		float64(ackLevel),
	)
	return nil
}

func (s *contextImpl) GetClusterReplicationLevel(cluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding replication level
	if replicationLevel, ok := s.shardInfo.ClusterReplicationLevel[cluster]; ok {
		return replicationLevel
	}

	// New cluster always starts from -1
	return -1
}

func (s *contextImpl) UpdateClusterReplicationLevel(cluster string, lastTaskID int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterReplicationLevel[cluster] = lastTaskID
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetTimerAckLevel() time.Time {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.TimerAckLevel
}

func (s *contextImpl) UpdateTimerAckLevel(ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetTimerClusterAckLevel(cluster string) time.Time {
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

func (s *contextImpl) UpdateTimerClusterAckLevel(cluster string, ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTimerAckLevel[cluster] = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *contextImpl) UpdateTransferFailoverLevel(failoverID string, level persistence.TransferFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferFailoverLevels[failoverID] = level
	return s.updateShardInfoLocked()
}

func (s *contextImpl) DeleteTransferFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	if level, ok := s.shardInfo.TransferFailoverLevels[failoverID]; ok {
		s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferFailoverLatencyTimer, time.Since(level.StartTime))
		delete(s.shardInfo.TransferFailoverLevels, failoverID)
	}
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetAllTransferFailoverLevels() map[string]persistence.TransferFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TransferFailoverLevel{}
	for k, v := range s.shardInfo.TransferFailoverLevels {
		ret[k] = v
	}
	return ret
}

func (s *contextImpl) UpdateTimerFailoverLevel(failoverID string, level persistence.TimerFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerFailoverLevels[failoverID] = level
	return s.updateShardInfoLocked()
}

func (s *contextImpl) DeleteTimerFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	if level, ok := s.shardInfo.TimerFailoverLevels[failoverID]; ok {
		s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerFailoverLatencyTimer, time.Since(level.StartTime))
		delete(s.shardInfo.TimerFailoverLevels, failoverID)
	}
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetAllTimerFailoverLevels() map[string]persistence.TimerFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TimerFailoverLevel{}
	for k, v := range s.shardInfo.TimerFailoverLevels {
		ret[k] = v
	}
	return ret
}

func (s *contextImpl) GetDomainNotificationVersion() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.DomainNotificationVersion
}

func (s *contextImpl) UpdateDomainNotificationVersion(domainNotificationVersion int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.DomainNotificationVersion = domainNotificationVersion
	return s.updateShardInfoLocked()
}

func (s *contextImpl) GetTimerMaxReadLevel(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()

	return s.timerMaxReadLevelMap[cluster]
}

func (s *contextImpl) UpdateTimerMaxReadLevel(cluster string) time.Time {
	s.Lock()
	defer s.Unlock()

	currentTime := s.GetTimeSource().Now()
	if cluster != "" && cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		currentTime = s.remoteClusterCurrentTime[cluster]
	}

	s.timerMaxReadLevelMap[cluster] = currentTime.Add(s.config.TimerProcessorMaxTimeShift()).Truncate(time.Millisecond)
	return s.timerMaxReadLevelMap[cluster]
}

func (s *contextImpl) CreateWorkflowExecution(
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {

	domainID := request.NewWorkflowSnapshot.ExecutionInfo.DomainID
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.GetDomainCache().GetDomainByID(domainID)
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
				*persistence.CurrentWorkflowConditionFailedError,
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
						s.logger.Warn(
							"Closing shard: CreateWorkflowExecution failed due to stolen shard.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
						s.closeShard()
						return nil, err
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
						s.logger.Warn(
							"Closing shard: CreateWorkflowExecution failed due to unknown error.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
						s.closeShard()
						break Create_Loop
					}
				}
			}
		}

		return response, err
	}

	return nil, errMaxAttemptsExceeded
}

func (s *contextImpl) getDefaultEncoding(domainEntry *cache.DomainCacheEntry) common.EncodingType {
	return common.EncodingType(s.config.EventEncodingType(domainEntry.GetInfo().Name))
}

func (s *contextImpl) UpdateWorkflowExecution(
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {

	domainID := request.UpdateWorkflowMutation.ExecutionInfo.DomainID
	workflowID := request.UpdateWorkflowMutation.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.GetDomainCache().GetDomainByID(domainID)
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
						s.logger.Warn(
							"Closing shard: UpdateWorkflowExecution failed due to stolen shard.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
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
						s.logger.Warn(
							"Closing shard: UpdateWorkflowExecution failed due to unknown error.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
						s.closeShard()
						break Update_Loop
					}
				}
			}
		}

		return resp, err
	}

	return nil, errMaxAttemptsExceeded
}

func (s *contextImpl) ResetWorkflowExecution(request *persistence.ResetWorkflowExecutionRequest) error {

	domainID := request.NewWorkflowSnapshot.ExecutionInfo.DomainID
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.GetDomainCache().GetDomainByID(domainID)
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
						s.logger.Warn(
							"Closing shard: ResetWorkflowExecution failed due to stolen shard.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
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
						s.logger.Warn(
							"Closing shard: ResetWorkflowExecution failed due to unknown error.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
						s.closeShard()
						break Reset_Loop
					}
				}
			}
		}

		return err
	}

	return errMaxAttemptsExceeded
}

func (s *contextImpl) ConflictResolveWorkflowExecution(
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) error {

	domainID := request.ResetWorkflowSnapshot.ExecutionInfo.DomainID
	workflowID := request.ResetWorkflowSnapshot.ExecutionInfo.WorkflowID

	// do not try to get domain cache within shard lock
	domainEntry, err := s.GetDomainCache().GetDomainByID(domainID)
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

Conflict_Resolve_Loop:
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
						continue Conflict_Resolve_Loop
					} else {
						// Shard is stolen, trigger shutdown of history engine
						s.logger.Warn(
							"Closing shard: ConflictResolveWorkflowExecution failed due to stolen shard.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
						s.closeShard()
						break Conflict_Resolve_Loop
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
						s.logger.Warn(
							"Closing shard: ConflictResolveWorkflowExecution failed due to unknown error.",
							tag.ShardID(s.GetShardID()),
							tag.Error(err),
						)
						s.closeShard()
						break Conflict_Resolve_Loop
					}
				}
			}
		}

		return err
	}

	return errMaxAttemptsExceeded
}

func (s *contextImpl) AppendHistoryV2Events(
	request *persistence.AppendHistoryNodesRequest, domainID string, execution shared.WorkflowExecution) (int, error) {

	domainEntry, err := s.GetDomainCache().GetDomainByID(domainID)
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
		s.GetMetricsClient().RecordTimer(metrics.SessionSizeStatsScope, metrics.HistorySize, time.Duration(size))
		if entry, err := s.GetDomainCache().GetDomainByID(domainID); err == nil && entry != nil && entry.GetInfo() != nil {
			s.GetMetricsClient().Scope(metrics.SessionSizeStatsScope, metrics.DomainTag(entry.GetInfo().Name)).RecordTimer(metrics.HistorySize, time.Duration(size))
		}
		if size >= historySizeLogThreshold {
			s.throttledLogger.Warn("history size threshold breached",
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.WorkflowDomainID(domainID),
				tag.WorkflowHistorySizeBytes(size))
		}
	}()
	resp, err0 := s.GetHistoryManager().AppendHistoryNodes(request)
	if resp != nil {
		size = resp.Size
	}
	return size, err0
}

func (s *contextImpl) GetConfig() *config.Config {
	return s.config
}

func (s *contextImpl) PreviousShardOwnerWasDifferent() bool {
	return s.previousShardOwnerWasDifferent
}

func (s *contextImpl) GetEventsCache() events.Cache {
	// the shard needs to be restarted to release the shard cache once global mode is on.
	if s.config.EventsCacheGlobalEnable() {
		return s.GetEventCache()
	}
	return s.eventsCache
}

func (s *contextImpl) GetLogger() log.Logger {
	return s.logger
}

func (s *contextImpl) GetThrottledLogger() log.Logger {
	return s.throttledLogger
}

func (s *contextImpl) getRangeID() int64 {
	return s.shardInfo.RangeID
}

func (s *contextImpl) isClosed() bool {
	return atomic.LoadInt32(&s.closed) != 0
}

func (s *contextImpl) closeShard() {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return
	}

	go func() {
		s.closeCallback(s.shardID, s.shardItem)
	}()

	// fails any writes that may start after this point.
	s.shardInfo.RangeID = -1
	atomic.StoreInt64(&s.rangeID, s.shardInfo.RangeID)
}

func (s *contextImpl) generateTransferTaskIDLocked() (int64, error) {
	if err := s.updateRangeIfNeededLocked(); err != nil {
		return -1, err
	}

	taskID := s.transferSequenceNumber
	s.transferSequenceNumber++

	return taskID, nil
}

func (s *contextImpl) updateRangeIfNeededLocked() error {
	if s.transferSequenceNumber < s.maxTransferSequenceNumber {
		return nil
	}

	return s.renewRangeLocked(false)
}

func (s *contextImpl) renewRangeLocked(isStealing bool) error {
	updatedShardInfo := copyShardInfo(s.shardInfo)
	updatedShardInfo.RangeID++
	if isStealing {
		updatedShardInfo.StolenSinceRenew++
	}

	var err error
	var attempt int32
Retry_Loop:
	for attempt = 0; attempt < conditionalRetryCount; attempt++ {
		err = s.GetShardManager().UpdateShard(&persistence.UpdateShardRequest{
			ShardInfo:       updatedShardInfo,
			PreviousRangeID: s.shardInfo.RangeID})
		switch err.(type) {
		case nil:
			break Retry_Loop
		case *persistence.ShardOwnershipLostError:
			// Shard is stolen, trigger history engine shutdown
			s.logger.Warn(
				"Closing shard: renewRangeLocked failed due to stolen shard.",
				tag.ShardID(s.GetShardID()),
				tag.Error(err),
				tag.Attempt(attempt),
			)
			s.closeShard()
			break Retry_Loop
		default:
			s.logger.Warn("UpdateShard failed with an unknown error.",
				tag.Error(err),
				tag.ShardRangeID(updatedShardInfo.RangeID),
				tag.PreviousShardRangeID(s.shardInfo.RangeID),
				tag.Attempt(attempt))
		}
	}
	if err != nil {
		// Failure in updating shard to grab new RangeID
		s.logger.Error("renewRangeLocked failed.",
			tag.StoreOperationUpdateShard,
			tag.Error(err),
			tag.ShardRangeID(updatedShardInfo.RangeID),
			tag.PreviousShardRangeID(s.shardInfo.RangeID),
			tag.Attempt(attempt))
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

func (s *contextImpl) updateMaxReadLevelLocked(rl int64) {
	if rl > s.transferMaxReadLevel {
		s.logger.Debug(fmt.Sprintf("Updating MaxReadLevel: %v", rl))
		s.transferMaxReadLevel = rl
	}
}

func (s *contextImpl) updateShardInfoLocked() error {
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
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: s.shardInfo.RangeID,
	})

	if err != nil {
		// Shard is stolen, trigger history engine shutdown
		if _, ok := err.(*persistence.ShardOwnershipLostError); ok {
			s.logger.Warn(
				"Closing shard: updateShardInfoLocked failed due to stolen shard.",
				tag.ShardID(s.GetShardID()),
				tag.Error(err),
			)
			s.closeShard()
		}
	} else {
		s.lastUpdated = now
	}

	return err
}

func (s *contextImpl) emitShardInfoMetricsLogsLocked() {
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

func (s *contextImpl) allocateTaskIDsLocked(
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

func (s *contextImpl) allocateTransferIDsLocked(
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
func (s *contextImpl) allocateTimerIDsLocked(
	domainEntry *cache.DomainCacheEntry,
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

func (s *contextImpl) SetCurrentTime(cluster string, currentTime time.Time) {
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

func (s *contextImpl) GetCurrentTime(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()
	if cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		return s.remoteClusterCurrentTime[cluster]
	}
	return s.GetTimeSource().Now()
}

func (s *contextImpl) GetLastUpdatedTime() time.Time {
	s.RLock()
	defer s.RUnlock()
	return s.lastUpdated
}

func (s *contextImpl) ReplicateFailoverMarkers(
	markers []*persistence.FailoverMarkerTask,
) error {

	tasks := make([]persistence.Task, 0, len(markers))
	for _, marker := range markers {
		tasks = append(tasks, marker)
	}

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if err := s.allocateTransferIDsLocked(
		tasks,
		&transferMaxReadLevel,
	); err != nil {
		return err
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

	var err error
Retry_Loop:
	for attempt := int32(0); attempt < conditionalRetryCount; attempt++ {
		err = s.executionManager.CreateFailoverMarkerTasks(
			&persistence.CreateFailoverMarkersRequest{
				RangeID: s.getRangeID(),
				Markers: markers,
			},
		)
		switch err.(type) {
		case nil:
			break Retry_Loop
		case *persistence.ShardOwnershipLostError:
			// do not retry on ShardOwnershipLostError
			s.logger.Warn(
				"Closing shard: ReplicateFailoverMarkers failed due to stolen shard.",
				tag.ShardID(s.GetShardID()),
				tag.Error(err),
			)
			s.closeShard()
			break Retry_Loop
		default:
			s.logger.Error(
				"Failed to insert the failover marker into replication queue.",
				tag.Error(err),
				tag.Attempt(attempt),
			)
		}
	}
	return err
}

func (s *contextImpl) AddingPendingFailoverMarker(
	marker *replicator.FailoverMarkerAttributes,
) error {

	domainEntry, err := s.GetDomainCache().GetDomainByID(marker.GetDomainID())
	if err != nil {
		return err
	}
	// domain is active, the marker is expired
	if domainEntry.IsDomainActive() || domainEntry.GetFailoverVersion() > marker.GetFailoverVersion() {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	s.pendingFailoverMarkers = append(s.pendingFailoverMarkers, marker)
	if err := s.updateFailoverMarkersInShardInfoLocked(); err != nil {
		return err
	}
	return s.updateShardInfoLocked()
}

func (s *contextImpl) ValidateAndUpdateFailoverMarkers() ([]*replicator.FailoverMarkerAttributes, error) {

	completedFailoverMarkers := make(map[*replicator.FailoverMarkerAttributes]struct{})
	s.RLock()
	for _, marker := range s.pendingFailoverMarkers {
		domainEntry, err := s.GetDomainCache().GetDomainByID(marker.GetDomainID())
		if err != nil {
			s.RUnlock()
			return nil, err
		}
		if domainEntry.IsDomainActive() || domainEntry.GetFailoverVersion() > marker.GetFailoverVersion() {
			completedFailoverMarkers[marker] = struct{}{}
		}
	}

	if len(completedFailoverMarkers) == 0 {
		return s.pendingFailoverMarkers, nil
	}
	s.RUnlock()

	// clean up all pending failover tasks
	s.Lock()
	defer s.Unlock()

	for idx, marker := range s.pendingFailoverMarkers {
		if _, ok := completedFailoverMarkers[marker]; ok {
			s.pendingFailoverMarkers[idx] = s.pendingFailoverMarkers[len(s.pendingFailoverMarkers)-1]
			s.pendingFailoverMarkers[len(s.pendingFailoverMarkers)-1] = nil
			s.pendingFailoverMarkers = s.pendingFailoverMarkers[:len(s.pendingFailoverMarkers)-1]
		}
	}
	if err := s.updateFailoverMarkersInShardInfoLocked(); err != nil {
		return nil, err
	}
	if err := s.updateShardInfoLocked(); err != nil {
		return nil, err
	}

	return s.pendingFailoverMarkers, nil
}

func (s *contextImpl) updateFailoverMarkersInShardInfoLocked() error {

	serializer := s.GetPayloadSerializer()
	data, err := serializer.SerializePendingFailoverMarkers(s.pendingFailoverMarkers, common.EncodingTypeThriftRW)
	if err != nil {
		return err
	}

	s.shardInfo.PendingFailoverMarkers = data
	return nil
}

func acquireShard(
	shardItem *historyShardsItem,
	closeCallback func(int, *historyShardsItem),
) (Context, error) {

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
		resp, err := shardItem.GetShardManager().GetShard(&persistence.GetShardRequest{
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
		return shardItem.GetShardManager().CreateShard(&persistence.CreateShardRequest{ShardInfo: shardInfo})
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

		if clusterName != shardItem.GetClusterMetadata().GetCurrentClusterName() {
			if currentTime, ok := shardInfo.ClusterTimerAckLevel[clusterName]; ok {
				remoteClusterCurrentTime[clusterName] = currentTime
				timerMaxReadLevelMap[clusterName] = currentTime
			} else {
				remoteClusterCurrentTime[clusterName] = shardInfo.TimerAckLevel
				timerMaxReadLevelMap[clusterName] = shardInfo.TimerAckLevel
			}
		} else { // active cluster
			timerMaxReadLevelMap[clusterName] = shardInfo.TimerAckLevel
		}

		timerMaxReadLevelMap[clusterName] = timerMaxReadLevelMap[clusterName].Truncate(time.Millisecond)
	}

	executionMgr, err := shardItem.GetExecutionManager(shardItem.shardID)
	if err != nil {
		return nil, err
	}

	context := &contextImpl{
		Resource:                       shardItem.Resource,
		shardItem:                      shardItem,
		shardID:                        shardItem.shardID,
		executionManager:               executionMgr,
		shardInfo:                      updatedShardInfo,
		closeCallback:                  closeCallback,
		config:                         shardItem.config,
		remoteClusterCurrentTime:       remoteClusterCurrentTime,
		timerMaxReadLevelMap:           timerMaxReadLevelMap, // use ack to init read level
		pendingFailoverMarkers:         []*replicator.FailoverMarkerAttributes{},
		logger:                         shardItem.logger,
		throttledLogger:                shardItem.throttledLogger,
		previousShardOwnerWasDifferent: ownershipChanged,
	}

	// TODO remove once migrated to global event cache
	context.eventsCache = events.NewCache(
		context.shardID,
		context.Resource.GetHistoryManager(),
		context.config,
		context.logger,
		context.Resource.GetMetricsClient(),
	)

	context.logger.Debug(fmt.Sprintf("Global event cache mode: %v", context.config.EventsCacheGlobalEnable()))

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
	replicationDLQAckLevel := make(map[string]int64)
	for k, v := range shardInfo.ReplicationDLQAckLevel {
		replicationDLQAckLevel[k] = v
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
		ReplicationDLQAckLevel:    replicationDLQAckLevel,
		PendingFailoverMarkers:    shardInfo.PendingFailoverMarkers,
		UpdatedAt:                 shardInfo.UpdatedAt,
	}

	return shardInfoCopy
}
