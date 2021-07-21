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

package shard

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
)

const (
	conditionalRetryCount = 5
)

type (
	ContextImpl struct {
		resource.Resource

		status           int32
		shardItem        *historyShardsItem
		shardID          int32
		executionManager persistence.ExecutionManager
		metricsClient    metrics.Client
		EventsCache      events.Cache
		closeCallback    func(int32, *historyShardsItem)
		config           *configs.Config
		logger           log.Logger
		throttledLogger  log.Logger
		engine           Engine

		rwLock                    sync.RWMutex
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

var _ Context = (*ContextImpl)(nil)

// ErrShardClosed is returned when shard is closed and a req cannot be processed
var ErrShardClosed = errors.New("shard closed")

const (
	logWarnTransferLevelDiff = 3000000 // 3 million
	logWarnTimerLevelDiff    = time.Duration(30 * time.Minute)
	historySizeLogThreshold  = 10 * 1024 * 1024
)

func (s *ContextImpl) GetShardID() int32 {
	return s.shardID
}

func (s *ContextImpl) GetService() resource.Resource {
	return s.Resource
}

func (s *ContextImpl) GetExecutionManager() persistence.ExecutionManager {
	return s.executionManager
}

func (s *ContextImpl) GetEngine() Engine {
	return s.engine
}

func (s *ContextImpl) SetEngine(engine Engine) {
	s.engine = engine
}

func (s *ContextImpl) GenerateTransferTaskID() (int64, error) {
	s.Lock()
	defer s.Unlock()

	return s.generateTransferTaskIDLocked()
}

func (s *ContextImpl) GenerateTransferTaskIDs(number int) ([]int64, error) {
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

func (s *ContextImpl) GetTransferMaxReadLevel() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.transferMaxReadLevel
}

func (s *ContextImpl) GetTransferAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.TransferAckLevel
}

func (s *ContextImpl) UpdateTransferAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetTransferClusterAckLevel(cluster string) int64 {
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

func (s *ContextImpl) UpdateTransferClusterAckLevel(cluster string, ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTransferAckLevel[cluster] = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetVisibilityAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.VisibilityAckLevel
}

func (s *ContextImpl) UpdateVisibilityAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.VisibilityAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetReplicatorAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.ReplicationAckLevel
}

func (s *ContextImpl) UpdateReplicatorAckLevel(ackLevel int64) error {
	s.Lock()
	defer s.Unlock()
	s.shardInfo.ReplicationAckLevel = ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetReplicatorDLQAckLevel(sourceCluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	if ackLevel, ok := s.shardInfo.ReplicationDlqAckLevel[sourceCluster]; ok {
		return ackLevel
	}
	return -1
}

func (s *ContextImpl) UpdateReplicatorDLQAckLevel(
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

func (s *ContextImpl) GetClusterReplicationLevel(cluster string) int64 {
	s.RLock()
	defer s.RUnlock()

	// if we can find corresponding replication level
	if replicationLevel, ok := s.shardInfo.ClusterReplicationLevel[cluster]; ok {
		return replicationLevel
	}

	// New cluster always starts from -1
	return persistence.EmptyQueueMessageID
}

func (s *ContextImpl) UpdateClusterReplicationLevel(cluster string, ackTaskID int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterReplicationLevel[cluster] = ackTaskID
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetTimerAckLevel() time.Time {
	s.RLock()
	defer s.RUnlock()

	return timestamp.TimeValue(s.shardInfo.TimerAckLevelTime)
}

func (s *ContextImpl) UpdateTimerAckLevel(ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerAckLevelTime = &ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetTimerClusterAckLevel(cluster string) time.Time {
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

func (s *ContextImpl) UpdateTimerClusterAckLevel(cluster string, ackLevel time.Time) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.ClusterTimerAckLevel[cluster] = &ackLevel
	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) UpdateTransferFailoverLevel(failoverID string, level persistence.TransferFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TransferFailoverLevels[failoverID] = level
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) DeleteTransferFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	if level, ok := s.shardInfo.TransferFailoverLevels[failoverID]; ok {
		s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferFailoverLatencyTimer, time.Since(level.StartTime))
		delete(s.shardInfo.TransferFailoverLevels, failoverID)
	}
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetAllTransferFailoverLevels() map[string]persistence.TransferFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TransferFailoverLevel{}
	for k, v := range s.shardInfo.TransferFailoverLevels {
		ret[k] = v
	}
	return ret
}

func (s *ContextImpl) UpdateTimerFailoverLevel(failoverID string, level persistence.TimerFailoverLevel) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.TimerFailoverLevels[failoverID] = level
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) DeleteTimerFailoverLevel(failoverID string) error {
	s.Lock()
	defer s.Unlock()

	if level, ok := s.shardInfo.TimerFailoverLevels[failoverID]; ok {
		s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerFailoverLatencyTimer, time.Since(level.StartTime))
		delete(s.shardInfo.TimerFailoverLevels, failoverID)
	}
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetAllTimerFailoverLevels() map[string]persistence.TimerFailoverLevel {
	s.RLock()
	defer s.RUnlock()

	ret := map[string]persistence.TimerFailoverLevel{}
	for k, v := range s.shardInfo.TimerFailoverLevels {
		ret[k] = v
	}
	return ret
}

func (s *ContextImpl) GetNamespaceNotificationVersion() int64 {
	s.RLock()
	defer s.RUnlock()

	return s.shardInfo.NamespaceNotificationVersion
}

func (s *ContextImpl) UpdateNamespaceNotificationVersion(namespaceNotificationVersion int64) error {
	s.Lock()
	defer s.Unlock()

	s.shardInfo.NamespaceNotificationVersion = namespaceNotificationVersion
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetTimerMaxReadLevel(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()

	return s.timerMaxReadLevelMap[cluster]
}

func (s *ContextImpl) UpdateTimerMaxReadLevel(cluster string) time.Time {
	s.Lock()
	defer s.Unlock()

	currentTime := s.GetTimeSource().Now()
	if cluster != "" && cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		currentTime = s.remoteClusterCurrentTime[cluster]
	}

	s.timerMaxReadLevelMap[cluster] = currentTime.Add(s.config.TimerProcessorMaxTimeShift()).Truncate(time.Millisecond)
	return s.timerMaxReadLevelMap[cluster]
}

func (s *ContextImpl) CreateWorkflowExecution(
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {
	if s.isStopped() {
		return nil, ErrShardClosed
	}

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
		request.NewWorkflowSnapshot.VisibilityTasks,
		&transferMaxReadLevel,
	); err != nil {
		return nil, err
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

	currentRangeID := s.getRangeID()
	request.RangeID = currentRangeID
	resp, err := s.executionManager.CreateWorkflowExecution(request)
	if err = s.handleError(err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) UpdateWorkflowExecution(
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	if s.isStopped() {
		return nil, ErrShardClosed
	}

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
		request.UpdateWorkflowMutation.VisibilityTasks,
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
			request.NewWorkflowSnapshot.VisibilityTasks,
			&transferMaxReadLevel,
		); err != nil {
			return nil, err
		}
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

	currentRangeID := s.getRangeID()
	request.RangeID = currentRangeID
	resp, err := s.executionManager.UpdateWorkflowExecution(request)
	if err = s.handleError(err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) ConflictResolveWorkflowExecution(
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) error {
	if s.isStopped() {
		return ErrShardClosed
	}

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
			request.CurrentWorkflowMutation.VisibilityTasks,
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
		request.ResetWorkflowSnapshot.VisibilityTasks,
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
			request.NewWorkflowSnapshot.VisibilityTasks,
			&transferMaxReadLevel,
		); err != nil {
			return err
		}
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

	currentRangeID := s.getRangeID()
	request.RangeID = currentRangeID
	err = s.executionManager.ConflictResolveWorkflowExecution(request)
	return s.handleError(err)
}

func (s *ContextImpl) AddTasks(
	request *persistence.AddTasksRequest,
) error {
	if s.isStopped() {
		return ErrShardClosed
	}

	namespaceID := request.NamespaceID
	workflowID := request.WorkflowID

	// do not try to get namespace cache within shard lock
	namespaceEntry, err := s.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()

	transferMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.TransferTasks,
		request.ReplicationTasks,
		request.TimerTasks,
		request.VisibilityTasks,
		&transferMaxReadLevel,
	); err != nil {
		return err
	}
	defer s.updateMaxReadLevelLocked(transferMaxReadLevel)

	request.RangeID = s.getRangeID()
	err = s.executionManager.AddTasks(request)
	if err = s.handleError(err); err != nil {
		return err
	}
	s.engine.NotifyNewTransferTasks(request.TransferTasks)
	s.engine.NotifyNewTimerTasks(request.TimerTasks)
	s.engine.NotifyNewVisibilityTasks(request.VisibilityTasks)
	s.engine.NotifyNewReplicationTasks(request.ReplicationTasks)
	return nil
}

func (s *ContextImpl) AppendHistoryEvents(
	request *persistence.AppendHistoryNodesRequest,
	namespaceID string,
	execution commonpb.WorkflowExecution,
) (int, error) {
	if s.isStopped() {
		return 0, ErrShardClosed
	}

	request.ShardID = s.shardID

	size := 0
	defer func() {
		// N.B. - Dual emit here makes sense so that we can see aggregate timer stats across all
		// namespaces along with the individual namespaces stats
		s.GetMetricsClient().RecordDistribution(metrics.SessionSizeStatsScope, metrics.HistorySize, size)
		if entry, err := s.GetNamespaceCache().GetNamespaceByID(namespaceID); err == nil && entry != nil && entry.GetInfo() != nil {
			s.GetMetricsClient().Scope(
				metrics.SessionSizeStatsScope,
				metrics.NamespaceTag(entry.GetInfo().Name),
			).RecordDistribution(metrics.HistorySize, size)
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

func (s *ContextImpl) GetConfig() *configs.Config {
	return s.config
}

func (s *ContextImpl) PreviousShardOwnerWasDifferent() bool {
	return s.previousShardOwnerWasDifferent
}

func (s *ContextImpl) GetEventsCache() events.Cache {
	return s.EventsCache
}

func (s *ContextImpl) GetLogger() log.Logger {
	return s.logger
}

func (s *ContextImpl) GetThrottledLogger() log.Logger {
	return s.throttledLogger
}

func (s *ContextImpl) getRangeID() int64 {
	return s.shardInfo.GetRangeId()
}

func (s *ContextImpl) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}

func (s *ContextImpl) closeShard() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	s.logger.Info("Close shard")

	go func() {
		s.closeCallback(s.shardID, s.shardItem)
	}()

	// fails any writes that may start after this point.
	s.shardInfo.RangeId = -1
}

func (s *ContextImpl) generateTransferTaskIDLocked() (int64, error) {
	if err := s.updateRangeIfNeededLocked(); err != nil {
		return -1, err
	}

	taskID := s.transferSequenceNumber
	s.transferSequenceNumber++

	return taskID, nil
}

func (s *ContextImpl) updateRangeIfNeededLocked() error {
	if s.transferSequenceNumber < s.maxTransferSequenceNumber {
		return nil
	}

	return s.renewRangeLocked(false)
}

func (s *ContextImpl) renewRangeLocked(isStealing bool) error {
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
	s.shardInfo = updatedShardInfo

	return nil
}

func (s *ContextImpl) updateMaxReadLevelLocked(rl int64) {
	if rl > s.transferMaxReadLevel {
		s.logger.Debug("Updating MaxTaskID", tag.MaxLevel(rl))
		s.transferMaxReadLevel = rl
	}
}

func (s *ContextImpl) updateShardInfoLocked() error {
	if s.isStopped() {
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

func (s *ContextImpl) emitShardInfoMetricsLogsLocked() {
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

		s.logger.Warn("Shard ack levels diff exceeds warn threshold.",
			tag.ShardTime(s.remoteClusterCurrentTime),
			tag.ShardReplicationAck(s.shardInfo.ReplicationAckLevel),
			tag.ShardTimerAcks(s.shardInfo.ClusterTimerAckLevel),
			tag.ShardTransferAcks(s.shardInfo.ClusterTransferAckLevel))
	}

	s.GetMetricsClient().RecordDistribution(metrics.ShardInfoScope, metrics.ShardInfoTransferDiffTimer, int(diffTransferLevel))
	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerDiffTimer, diffTimerLevel)

	s.GetMetricsClient().RecordDistribution(metrics.ShardInfoScope, metrics.ShardInfoReplicationLagTimer, int(replicationLag))
	s.GetMetricsClient().RecordDistribution(metrics.ShardInfoScope, metrics.ShardInfoTransferLagTimer, int(transferLag))
	s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerLagTimer, timerLag)

	s.GetMetricsClient().RecordDistribution(metrics.ShardInfoScope, metrics.ShardInfoTransferFailoverInProgressTimer, transferFailoverInProgress)
	s.GetMetricsClient().RecordDistribution(metrics.ShardInfoScope, metrics.ShardInfoTimerFailoverInProgressTimer, timerFailoverInProgress)
}

func (s *ContextImpl) allocateTaskIDsLocked(
	namespaceEntry *cache.NamespaceCacheEntry,
	workflowID string,
	transferTasks []persistence.Task,
	replicationTasks []persistence.Task,
	timerTasks []persistence.Task,
	visibilityTasks []persistence.Task,
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
	if err := s.allocateTransferIDsLocked(
		visibilityTasks,
		transferMaxReadLevel); err != nil {
		return err
	}
	return s.allocateTimerIDsLocked(
		namespaceEntry,
		workflowID,
		timerTasks)
}

func (s *ContextImpl) allocateTransferIDsLocked(
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
func (s *ContextImpl) allocateTimerIDsLocked(
	namespaceEntry *cache.NamespaceCacheEntry,
	workflowID string,
	timerTasks []persistence.Task,
) error {

	// assign IDs for the timer tasks. They need to be assigned under shard lock.
	currentCluster := s.GetClusterMetadata().GetCurrentClusterName()
	for _, task := range timerTasks {
		ts := task.GetVisibilityTime()
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
			task.SetVisibilityTime(s.timerMaxReadLevelMap[currentCluster].Add(time.Millisecond))
		}

		seqNum, err := s.generateTransferTaskIDLocked()
		if err != nil {
			return err
		}
		task.SetTaskID(seqNum)
		visibilityTs := task.GetVisibilityTime()
		s.logger.Debug("Assigning new timer",
			tag.Timestamp(visibilityTs), tag.TaskID(task.GetTaskID()), tag.AckLevel(s.shardInfo.TimerAckLevelTime))
	}
	return nil
}

func (s *ContextImpl) SetCurrentTime(cluster string, currentTime time.Time) {
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

func (s *ContextImpl) GetCurrentTime(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()
	if cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		return s.remoteClusterCurrentTime[cluster]
	}
	return s.GetTimeSource().Now().UTC()
}

func (s *ContextImpl) GetLastUpdatedTime() time.Time {
	s.RLock()
	defer s.RUnlock()
	return s.lastUpdated
}

func (s *ContextImpl) handleError(err error) error {
	switch err.(type) {
	case nil:
		return nil

	case *persistence.CurrentWorkflowConditionFailedError,
		*persistence.WorkflowConditionFailedError,
		*persistence.ConditionFailedError,
		*serviceerror.ResourceExhausted:
		// No special handling required for these errors
		return err

	case *persistence.ShardOwnershipLostError:
		// Shard is stolen, trigger shutdown of history engine
		s.closeShard()
		return err

	default:
		// We have no idea if the write failed or will eventually make it to
		// persistence. Increment RangeID to guarantee that subsequent reads
		// will either see that write, or know for certain that it failed.
		// This allows the callers to reliably check the outcome by performing
		// a read.
		if err := s.renewRangeLocked(false); err != nil {
			// At this point we have no choice but to unload the shard, so that it
			// gets a new RangeID when it's reloaded.
			s.closeShard()
		}
		return err
	}
}

func (s *ContextImpl) Lock() {
	scope := metrics.ShardInfoScope
	s.metricsClient.IncCounter(scope, metrics.LockRequests)
	sw := s.metricsClient.StartTimer(scope, metrics.LockLatency)
	defer sw.Stop()

	s.rwLock.Lock()
}

func (s *ContextImpl) RLock() {
	scope := metrics.ShardInfoScope
	s.metricsClient.IncCounter(scope, metrics.LockRequests)
	sw := s.metricsClient.StartTimer(scope, metrics.LockLatency)
	defer sw.Stop()

	s.rwLock.RLock()
}

func (s *ContextImpl) Unlock() {
	s.rwLock.Unlock()
}

func (s *ContextImpl) RUnlock() {
	s.rwLock.RUnlock()
}

func acquireShard(
	shardItem *historyShardsItem,
	closeCallback func(int32, *historyShardsItem),
) (Context, error) {

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
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: shardItem.shardID,
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

	shardContext := &ContextImpl{
		Resource: shardItem.Resource,
		// shard context does not have background processing logic
		status:                         common.DaemonStatusStarted,
		shardItem:                      shardItem,
		shardID:                        shardItem.shardID,
		executionManager:               executionMgr,
		metricsClient:                  shardItem.GetMetricsClient(),
		shardInfo:                      updatedShardInfo,
		closeCallback:                  closeCallback,
		config:                         shardItem.config,
		remoteClusterCurrentTime:       remoteClusterCurrentTime,
		timerMaxReadLevelMap:           timerMaxReadLevelMap, // use ack to init read level
		logger:                         shardItem.logger,
		throttledLogger:                shardItem.throttledLogger,
		previousShardOwnerWasDifferent: ownershipChanged,
	}
	shardContext.EventsCache = events.NewEventsCache(
		shardContext.GetShardID(),
		shardContext.GetConfig().EventsCacheInitialSize(),
		shardContext.GetConfig().EventsCacheMaxSize(),
		shardContext.GetConfig().EventsCacheTTL(),
		shardContext.GetHistoryManager(),
		false,
		shardContext.GetLogger(),
		shardContext.GetMetricsClient(),
	)

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
	clusterReplicationDLQLevel := make(map[string]int64)
	for k, v := range shardInfo.ReplicationDlqAckLevel {
		clusterReplicationDLQLevel[k] = v
	}
	shardInfoCopy := &persistence.ShardInfoWithFailover{
		ShardInfo: &persistencespb.ShardInfo{
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
			ReplicationDlqAckLevel:       clusterReplicationDLQLevel,
			UpdateTime:                   shardInfo.UpdateTime,
			VisibilityAckLevel:           shardInfo.VisibilityAckLevel,
		},
		TransferFailoverLevels: transferFailoverLevels,
		TimerFailoverLevels:    timerFailoverLevels,
	}

	return shardInfoCopy
}
