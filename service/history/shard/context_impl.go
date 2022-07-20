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
	"context"
	"fmt"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/api/adminservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
)

const (
	// See transitionLocked for overview of state transitions.
	// These are the possible values of ContextImpl.state:
	contextStateInitialized contextState = iota
	contextStateAcquiring
	contextStateAcquired
	contextStateStopping
	contextStateStopped
)

const (
	shardIOTimeout = 5 * time.Second
)

type (
	contextState int32

	ContextImpl struct {
		// These fields are constant:
		shardID             int32
		executionManager    persistence.ExecutionManager
		metricsClient       metrics.Client
		metricsHandler      metrics.MetricsHandler
		eventsCache         events.Cache
		closeCallback       func(*ContextImpl)
		config              *configs.Config
		contextTaggedLogger log.Logger
		throttledLogger     log.Logger
		engineFactory       EngineFactory

		persistenceShardManager persistence.ShardManager
		clientBean              client.Bean
		historyClient           historyservice.HistoryServiceClient
		payloadSerializer       serialization.Serializer
		timeSource              clock.TimeSource
		namespaceRegistry       namespace.Registry
		saProvider              searchattribute.Provider
		saMapper                searchattribute.Mapper
		clusterMetadata         cluster.Metadata
		archivalMetadata        archiver.ArchivalMetadata
		hostInfoProvider        membership.HostInfoProvider

		// Context that lives for the lifetime of the shard context
		lifecycleCtx    context.Context
		lifecycleCancel context.CancelFunc

		// All following fields are protected by rwLock, and only valid if state >= Acquiring:
		rwLock                             sync.RWMutex
		state                              contextState
		engineFuture                       *future.FutureImpl[Engine]
		lastUpdated                        time.Time
		shardInfo                          *persistence.ShardInfoWithFailover
		taskSequenceNumber                 int64
		maxTaskSequenceNumber              int64
		immediateTaskExclusiveMaxReadLevel int64
		scheduledTaskMaxReadLevelMap       map[string]time.Time // cluster -> scheduledTaskMaxReadLevel

		// exist only in memory
		remoteClusterInfos map[string]*remoteClusterInfo
		handoverNamespaces map[string]*namespaceHandOverInfo // keyed on namespace name
	}

	remoteClusterInfo struct {
		CurrentTime               time.Time
		AckedReplicationTaskID    int64
		AckedReplicationTimestamp time.Time
	}

	namespaceHandOverInfo struct {
		MaxReplicationTaskID int64
		NotificationVersion  int64
	}

	// These are the requests that can be passed to transitionLocked to change state:
	contextRequest interface{}

	contextRequestAcquire    struct{}
	contextRequestAcquired   struct{}
	contextRequestLost       struct{}
	contextRequestStop       struct{}
	contextRequestFinishStop struct{}
)

var _ Context = (*ContextImpl)(nil)

var (
	// ErrShardClosed is returned when shard is closed and a req cannot be processed
	ErrShardClosed = serviceerror.NewUnavailable("shard closed")

	// ErrShardStatusUnknown means we're not sure if we have the shard lock or not. This may be returned
	// during short windows at initialization and if we've lost the connection to the database.
	ErrShardStatusUnknown = serviceerror.NewUnavailable("shard status unknown")

	// errStoppingContext is an internal error used to abort acquireShard
	errStoppingContext = serviceerror.NewUnavailable("stopping context")
)

const (
	logWarnImmediateTaskLevelDiff = 3000000 // 3 million
	logWarnScheduledTaskLevelDiff = time.Duration(30 * time.Minute)
	historySizeLogThreshold       = 10 * 1024 * 1024
	minContextTimeout             = 2 * time.Second
)

func (s *ContextImpl) String() string {
	// constant from initialization, no need for locks
	return fmt.Sprintf("Shard(%d)", s.shardID)
}

func (s *ContextImpl) GetShardID() int32 {
	// constant from initialization, no need for locks
	return s.shardID
}

func (s *ContextImpl) GetExecutionManager() persistence.ExecutionManager {
	// constant from initialization, no need for locks
	return s.executionManager
}

func (s *ContextImpl) GetEngine(
	ctx context.Context,
) (Engine, error) {
	return s.engineFuture.Get(ctx)
}

func (s *ContextImpl) GetMaxTaskIDForCurrentRangeID() int64 {
	s.rLock()
	defer s.rUnlock()
	// maxTaskSequenceNumber is the exclusive upper bound of task ID for current range.
	return s.maxTaskSequenceNumber - 1
}

func (s *ContextImpl) AssertOwnership(
	ctx context.Context,
) error {
	s.wLock()
	defer s.wUnlock()

	if err := s.errorByStateLocked(); err != nil {
		return err
	}

	rangeID := s.getRangeIDLocked()
	err := s.persistenceShardManager.AssertShardOwnership(ctx, &persistence.AssertShardOwnershipRequest{
		ShardID: s.shardID,
		RangeID: rangeID,
	})
	if err = s.handleWriteErrorAndUpdateMaxReadLevelLocked(err, 0); err != nil {
		return err
	}
	return nil
}

func (s *ContextImpl) NewVectorClock() (*clockspb.VectorClock, error) {
	s.wLock()
	defer s.wUnlock()

	clock, err := s.generateTaskIDLocked()
	if err != nil {
		return nil, err
	}
	return vclock.NewVectorClock(s.clusterMetadata.GetClusterID(), s.shardID, clock), nil
}

func (s *ContextImpl) CurrentVectorClock() *clockspb.VectorClock {
	s.rLock()
	defer s.rUnlock()

	clock := s.taskSequenceNumber
	return vclock.NewVectorClock(s.clusterMetadata.GetClusterID(), s.shardID, clock)
}

func (s *ContextImpl) GenerateTaskID() (int64, error) {
	s.wLock()
	defer s.wUnlock()

	return s.generateTaskIDLocked()
}

func (s *ContextImpl) GenerateTaskIDs(number int) ([]int64, error) {
	s.wLock()
	defer s.wUnlock()

	result := []int64{}
	for i := 0; i < number; i++ {
		id, err := s.generateTaskIDLocked()
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}

func (s *ContextImpl) GetQueueExclusiveHighReadWatermark(
	category tasks.Category,
	cluster string,
) tasks.Key {
	switch categoryType := category.Type(); categoryType {
	case tasks.CategoryTypeImmediate:
		return s.getImmediateTaskExclusiveMaxReadLevel()
	case tasks.CategoryTypeScheduled:
		return s.updateScheduledTaskMaxReadLevel(cluster)
	default:
		panic(fmt.Sprintf("invalid task category type: %v", categoryType))
	}
}

func (s *ContextImpl) getImmediateTaskExclusiveMaxReadLevel() tasks.Key {
	s.rLock()
	defer s.rUnlock()
	return tasks.NewImmediateKey(s.immediateTaskExclusiveMaxReadLevel)
}

func (s *ContextImpl) getScheduledTaskMaxReadLevel(cluster string) tasks.Key {
	s.rLock()
	defer s.rUnlock()

	if _, ok := s.scheduledTaskMaxReadLevelMap[cluster]; !ok {
		s.scheduledTaskMaxReadLevelMap[cluster] = tasks.DefaultFireTime
	}

	return tasks.NewKey(s.scheduledTaskMaxReadLevelMap[cluster], 0)
}

func (s *ContextImpl) updateScheduledTaskMaxReadLevel(cluster string) tasks.Key {
	s.wLock()
	defer s.wUnlock()

	if _, ok := s.scheduledTaskMaxReadLevelMap[cluster]; !ok {
		s.scheduledTaskMaxReadLevelMap[cluster] = tasks.DefaultFireTime
	}

	currentTime := s.timeSource.Now()
	if cluster != "" && cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		currentTime = s.getOrUpdateRemoteClusterInfoLocked(cluster).CurrentTime
	}

	newMaxReadLevel := currentTime.Add(s.config.TimerProcessorMaxTimeShift()).Truncate(time.Millisecond)
	s.scheduledTaskMaxReadLevelMap[cluster] = util.MaxTime(s.scheduledTaskMaxReadLevelMap[cluster], newMaxReadLevel)
	return tasks.NewKey(s.scheduledTaskMaxReadLevelMap[cluster], 0)
}

// NOTE: the ack level returned is inclusive for immediate task category (acked),
// but exclusive for scheduled task category (not acked).
func (s *ContextImpl) GetQueueAckLevel(category tasks.Category) tasks.Key {
	s.rLock()
	defer s.rUnlock()

	return s.getQueueAckLevelLocked(category)
}

func (s *ContextImpl) getQueueAckLevelLocked(category tasks.Category) tasks.Key {
	if queueAckLevel, ok := s.shardInfo.QueueAckLevels[category.ID()]; ok && queueAckLevel.AckLevel != 0 {
		return convertPersistenceAckLevelToTaskKey(category.Type(), queueAckLevel.AckLevel)
	}

	return tasks.NewKey(tasks.DefaultFireTime, 0)
}

func (s *ContextImpl) UpdateQueueAckLevel(
	category tasks.Category,
	ackLevel tasks.Key,
) error {
	s.wLock()
	defer s.wUnlock()

	// the ack level is already the min ack level across all types of
	// queue processors: active, passive, failover

	categoryID := category.ID()
	if _, ok := s.shardInfo.QueueAckLevels[categoryID]; !ok {
		s.shardInfo.QueueAckLevels[categoryID] = &persistencespb.QueueAckLevel{
			ClusterAckLevel: make(map[string]int64),
		}
	}
	persistenceAckLevel := convertTaskKeyToPersistenceAckLevel(category.Type(), ackLevel)
	s.shardInfo.QueueAckLevels[categoryID].AckLevel = persistenceAckLevel

	// if cluster ack level is less than the overall ack level, update cluster ack level
	// as well to prevent loading too many tombstones if the cluster ack level is used later
	// this may happen when adding back a removed cluster or rolling back the change for using
	// single queue in timer/transfer queue processor
	clusterAckLevel := s.shardInfo.QueueAckLevels[categoryID].ClusterAckLevel
	for clusterName, persistenceClusterAckLevel := range clusterAckLevel {
		if persistenceClusterAckLevel < persistenceAckLevel {
			clusterAckLevel[clusterName] = persistenceAckLevel
		}
	}

	// when this method is called, it means multi-cursor is disabled.
	// clear processor state field, so that next time multi-cursor is enabled,
	// it won't start from an very old state
	delete(s.shardInfo.QueueStates, categoryID)

	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetQueueClusterAckLevel(
	category tasks.Category,
	cluster string,
) tasks.Key {
	s.rLock()
	defer s.rUnlock()

	return s.getQueueClusterAckLevelLocked(category, cluster)
}

func (s *ContextImpl) getQueueClusterAckLevelLocked(
	category tasks.Category,
	cluster string,
) tasks.Key {
	if queueAckLevel, ok := s.shardInfo.QueueAckLevels[category.ID()]; ok {
		if ackLevel, ok := queueAckLevel.ClusterAckLevel[cluster]; ok {
			return convertPersistenceAckLevelToTaskKey(category.Type(), ackLevel)
		}
	}

	// otherwise, default to existing ack level, which belongs to local cluster
	// this can happen if you add more clusters or a new category
	return s.getQueueAckLevelLocked(category)
}

func (s *ContextImpl) UpdateQueueClusterAckLevel(
	category tasks.Category,
	cluster string,
	ackLevel tasks.Key,
) error {
	s.wLock()
	defer s.wUnlock()

	if levels, ok := s.shardInfo.FailoverLevels[category]; ok && len(levels) != 0 {
		// do not move ack level when there's failover queue
		// so that after shard reload we can re-create the failover queue
		return nil
	}

	if _, ok := s.shardInfo.QueueAckLevels[category.ID()]; !ok {
		s.shardInfo.QueueAckLevels[category.ID()] = &persistencespb.QueueAckLevel{
			ClusterAckLevel: make(map[string]int64),
		}
	}
	s.shardInfo.QueueAckLevels[category.ID()].ClusterAckLevel[cluster] = convertTaskKeyToPersistenceAckLevel(category.Type(), ackLevel)

	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetQueueState(
	category tasks.Category,
) (*persistencespb.QueueState, bool) {
	s.rLock()
	defer s.rUnlock()

	queueState, ok := s.shardInfo.QueueStates[category.ID()]
	return queueState, ok
}

func (s *ContextImpl) UpdateQueueState(
	category tasks.Category,
	state *persistencespb.QueueState,
) error {
	s.wLock()
	defer s.wUnlock()

	categoryID := category.ID()
	s.shardInfo.QueueStates[categoryID] = state

	// for compatability, update ack level and cluster ack level as well
	// so after rollback or disabling the feature, we won't load too many tombstones
	minAckLevel := tasks.MaximumKey
	for _, readerState := range state.ReaderStates {
		if len(readerState.Scopes) != 0 {
			minAckLevel = tasks.MinKey(
				minAckLevel,
				convertFromPersistenceTaskKey(readerState.Scopes[0].Range.InclusiveMin),
			)
		}
	}

	if category.Type() == tasks.CategoryTypeImmediate && minAckLevel.TaskID > 0 {
		// for immediate task type, the ack level is inclusive
		// for scheduled task type, the ack level is exclusive
		minAckLevel = minAckLevel.Prev()
	}
	persistenceAckLevel := convertTaskKeyToPersistenceAckLevel(category.Type(), minAckLevel)

	if _, ok := s.shardInfo.QueueAckLevels[categoryID]; !ok {
		s.shardInfo.QueueAckLevels[categoryID] = &persistencespb.QueueAckLevel{
			ClusterAckLevel: make(map[string]int64),
		}
	}
	s.shardInfo.QueueAckLevels[categoryID].AckLevel = persistenceAckLevel

	clusterAckLevel := s.shardInfo.QueueAckLevels[categoryID].ClusterAckLevel
	for clusterName := range clusterAckLevel {
		clusterAckLevel[clusterName] = persistenceAckLevel
	}

	s.shardInfo.StolenSinceRenew = 0
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) UpdateRemoteClusterInfo(
	cluster string,
	ackTaskID int64,
	ackTimestamp time.Time,
) {
	s.wLock()
	defer s.wUnlock()

	remoteClusterInfo := s.getOrUpdateRemoteClusterInfoLocked(cluster)
	remoteClusterInfo.AckedReplicationTaskID = ackTaskID
	remoteClusterInfo.AckedReplicationTimestamp = ackTimestamp
}

func (s *ContextImpl) GetReplicatorDLQAckLevel(sourceCluster string) int64 {
	s.rLock()
	defer s.rUnlock()

	if ackLevel, ok := s.shardInfo.ReplicationDlqAckLevel[sourceCluster]; ok {
		return ackLevel
	}
	return -1
}

func (s *ContextImpl) UpdateReplicatorDLQAckLevel(
	sourceCluster string,
	ackLevel int64,
) error {
	s.wLock()
	defer s.wUnlock()

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

func (s *ContextImpl) UpdateFailoverLevel(category tasks.Category, failoverID string, level persistence.FailoverLevel) error {
	s.wLock()
	defer s.wUnlock()

	if _, ok := s.shardInfo.FailoverLevels[category]; !ok {
		s.shardInfo.FailoverLevels[category] = make(map[string]persistence.FailoverLevel)
	}
	s.shardInfo.FailoverLevels[category][failoverID] = level

	return s.updateShardInfoLocked()
}

func (s *ContextImpl) DeleteFailoverLevel(category tasks.Category, failoverID string) error {
	s.wLock()
	defer s.wUnlock()

	if levels, ok := s.shardInfo.FailoverLevels[category]; ok {
		if level, ok := levels[failoverID]; ok {
			scope := s.GetMetricsClient().Scope(metrics.ShardInfoScope)
			switch category {
			case tasks.CategoryTransfer:
				scope.RecordTimer(metrics.ShardInfoTransferFailoverLatencyTimer, time.Since(level.StartTime))
			case tasks.CategoryTimer:
				scope.RecordTimer(metrics.ShardInfoTimerFailoverLatencyTimer, time.Since(level.StartTime))
			}
			delete(levels, failoverID)
		}
	}
	return s.updateShardInfoLocked()
}

func (s *ContextImpl) GetAllFailoverLevels(category tasks.Category) map[string]persistence.FailoverLevel {
	s.rLock()
	defer s.rUnlock()

	ret := map[string]persistence.FailoverLevel{}
	if levels, ok := s.shardInfo.FailoverLevels[category]; ok {
		maps.Copy(ret, levels)
	}

	return ret
}

func (s *ContextImpl) GetNamespaceNotificationVersion() int64 {
	s.rLock()
	defer s.rUnlock()

	return s.shardInfo.NamespaceNotificationVersion
}

func (s *ContextImpl) UpdateNamespaceNotificationVersion(namespaceNotificationVersion int64) error {
	s.wLock()
	defer s.wUnlock()

	// update namespace notification version.
	if s.shardInfo.NamespaceNotificationVersion < namespaceNotificationVersion {
		s.shardInfo.NamespaceNotificationVersion = namespaceNotificationVersion
		return s.updateShardInfoLocked()
	}

	return nil
}

func (s *ContextImpl) UpdateHandoverNamespaces(namespaces []*namespace.Namespace, maxRepTaskID int64) {
	s.wLock()
	defer s.wUnlock()

	newHandoverNamespaces := make(map[string]struct{})
	for _, ns := range namespaces {
		if ns.IsGlobalNamespace() && ns.ReplicationState() == enums.REPLICATION_STATE_HANDOVER {
			nsName := ns.Name().String()
			newHandoverNamespaces[nsName] = struct{}{}
			if handover, ok := s.handoverNamespaces[nsName]; ok {
				if handover.NotificationVersion < ns.NotificationVersion() {
					handover.NotificationVersion = ns.NotificationVersion()
					handover.MaxReplicationTaskID = maxRepTaskID
				}
			} else {
				s.handoverNamespaces[nsName] = &namespaceHandOverInfo{
					NotificationVersion:  ns.NotificationVersion(),
					MaxReplicationTaskID: maxRepTaskID,
				}
			}
		}
	}
	// delete old handover ns
	for k := range s.handoverNamespaces {
		if _, ok := newHandoverNamespaces[k]; !ok {
			delete(s.handoverNamespaces, k)
		}
	}
}

func (s *ContextImpl) AddTasks(
	ctx context.Context,
	request *persistence.AddHistoryTasksRequest,
) error {
	ctx, cancel, err := s.ensureMinContextTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()

	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.NamespaceID)
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	engine, err := s.GetEngine(ctx)
	if err != nil {
		return err
	}

	s.wLock()
	if err := s.errorByStateLocked(); err != nil {
		s.wUnlock()
		return err
	}

	err = s.addTasksLocked(ctx, request, namespaceEntry)
	s.wUnlock()

	if OperationPossiblySucceeded(err) {
		engine.NotifyNewTasks(namespaceEntry.ActiveClusterName(), request.Tasks)
	}

	return err
}

func (s *ContextImpl) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {
	ctx, cancel, err := s.ensureMinContextTimeout(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId)
	workflowID := request.NewWorkflowSnapshot.ExecutionInfo.WorkflowId
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	s.wLock()
	defer s.wUnlock()

	if err := s.errorByStateLocked(); err != nil {
		return nil, err
	}

	transferExclusiveMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.NewWorkflowSnapshot.Tasks,
		&transferExclusiveMaxReadLevel,
	); err != nil {
		return nil, err
	}

	currentRangeID := s.getRangeIDLocked()
	request.RangeID = currentRangeID
	resp, err := s.executionManager.CreateWorkflowExecution(ctx, request)
	if err = s.handleWriteErrorAndUpdateMaxReadLevelLocked(err, transferExclusiveMaxReadLevel); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	ctx, cancel, err := s.ensureMinContextTimeout(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId)
	workflowID := request.UpdateWorkflowMutation.ExecutionInfo.WorkflowId
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	s.wLock()
	defer s.wUnlock()

	if err := s.errorByStateLocked(); err != nil {
		return nil, err
	}

	transferExclusiveMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.UpdateWorkflowMutation.Tasks,
		&transferExclusiveMaxReadLevel,
	); err != nil {
		return nil, err
	}
	s.updateCloseTaskIDs(request.UpdateWorkflowMutation.ExecutionInfo, request.UpdateWorkflowMutation.Tasks)
	if request.NewWorkflowSnapshot != nil {
		if err := s.allocateTaskIDsLocked(
			namespaceEntry,
			workflowID,
			request.NewWorkflowSnapshot.Tasks,
			&transferExclusiveMaxReadLevel,
		); err != nil {
			return nil, err
		}
		s.updateCloseTaskIDs(request.NewWorkflowSnapshot.ExecutionInfo, request.NewWorkflowSnapshot.Tasks)
	}

	currentRangeID := s.getRangeIDLocked()
	request.RangeID = currentRangeID
	resp, err := s.executionManager.UpdateWorkflowExecution(ctx, request)
	if err = s.handleWriteErrorAndUpdateMaxReadLevelLocked(err, transferExclusiveMaxReadLevel); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) updateCloseTaskIDs(executionInfo *persistencespb.WorkflowExecutionInfo, tasksByCategory map[tasks.Category][]tasks.Task) {
	for _, t := range tasksByCategory[tasks.CategoryTransfer] {
		if t.GetType() == enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION {
			executionInfo.CloseTransferTaskId = t.GetTaskID()
			break
		}
	}
	for _, t := range tasksByCategory[tasks.CategoryVisibility] {
		if t.GetType() == enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION {
			executionInfo.CloseVisibilityTaskId = t.GetTaskID()
			break
		}
	}
}

func (s *ContextImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
	ctx, cancel, err := s.ensureMinContextTimeout(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId)
	workflowID := request.ResetWorkflowSnapshot.ExecutionInfo.WorkflowId
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	s.wLock()
	defer s.wUnlock()

	if err := s.errorByStateLocked(); err != nil {
		return nil, err
	}

	transferExclusiveMaxReadLevel := int64(0)
	if request.CurrentWorkflowMutation != nil {
		if err := s.allocateTaskIDsLocked(
			namespaceEntry,
			workflowID,
			request.CurrentWorkflowMutation.Tasks,
			&transferExclusiveMaxReadLevel,
		); err != nil {
			return nil, err
		}
	}
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.ResetWorkflowSnapshot.Tasks,
		&transferExclusiveMaxReadLevel,
	); err != nil {
		return nil, err
	}
	if request.NewWorkflowSnapshot != nil {
		if err := s.allocateTaskIDsLocked(
			namespaceEntry,
			workflowID,
			request.NewWorkflowSnapshot.Tasks,
			&transferExclusiveMaxReadLevel,
		); err != nil {
			return nil, err
		}
	}

	currentRangeID := s.getRangeIDLocked()
	request.RangeID = currentRangeID
	resp, err := s.executionManager.ConflictResolveWorkflowExecution(ctx, request)
	if err = s.handleWriteErrorAndUpdateMaxReadLevelLocked(err, transferExclusiveMaxReadLevel); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.SetWorkflowExecutionRequest,
) (*persistence.SetWorkflowExecutionResponse, error) {
	ctx, cancel, err := s.ensureMinContextTimeout(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()

	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.SetWorkflowSnapshot.ExecutionInfo.NamespaceId)
	workflowID := request.SetWorkflowSnapshot.ExecutionInfo.WorkflowId
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	s.wLock()
	defer s.wUnlock()

	if err := s.errorByStateLocked(); err != nil {
		return nil, err
	}

	transferExclusiveMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		workflowID,
		request.SetWorkflowSnapshot.Tasks,
		&transferExclusiveMaxReadLevel,
	); err != nil {
		return nil, err
	}

	currentRangeID := s.getRangeIDLocked()
	request.RangeID = currentRangeID
	resp, err := s.executionManager.SetWorkflowExecution(ctx, request)
	if err = s.handleWriteErrorAndUpdateMaxReadLevelLocked(err, transferExclusiveMaxReadLevel); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.GetCurrentExecutionResponse, error) {
	if err := s.errorByState(); err != nil {
		return nil, err
	}

	resp, err := s.executionManager.GetCurrentExecution(ctx, request)
	if err = s.handleReadError(err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {
	if err := s.errorByState(); err != nil {
		return nil, err
	}

	resp, err := s.executionManager.GetWorkflowExecution(ctx, request)
	if err = s.handleReadError(err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) addTasksLocked(
	ctx context.Context,
	request *persistence.AddHistoryTasksRequest,
	namespaceEntry *namespace.Namespace,
) error {
	transferExclusiveMaxReadLevel := int64(0)
	if err := s.allocateTaskIDsLocked(
		namespaceEntry,
		request.WorkflowID,
		request.Tasks,
		&transferExclusiveMaxReadLevel,
	); err != nil {
		return err
	}

	request.RangeID = s.getRangeIDLocked()
	err := s.executionManager.AddHistoryTasks(ctx, request)
	return s.handleWriteErrorAndUpdateMaxReadLevelLocked(err, transferExclusiveMaxReadLevel)
}

func (s *ContextImpl) AppendHistoryEvents(
	ctx context.Context,
	request *persistence.AppendHistoryNodesRequest,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
) (int, error) {
	if err := s.errorByState(); err != nil {
		return 0, err
	}

	request.ShardID = s.shardID

	size := 0
	defer func() {
		// N.B. - Dual emit here makes sense so that we can see aggregate timer stats across all
		// namespaces along with the individual namespaces stats
		s.GetMetricsClient().RecordDistribution(metrics.SessionStatsScope, metrics.HistorySize, size)
		if entry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID); err == nil && entry != nil {
			s.GetMetricsClient().Scope(
				metrics.SessionStatsScope,
				metrics.NamespaceTag(entry.Name().String()),
			).RecordDistribution(metrics.HistorySize, size)
		}
		if size >= historySizeLogThreshold {
			s.throttledLogger.Warn("history size threshold breached",
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowHistorySizeBytes(size))
		}
	}()
	resp, err0 := s.GetExecutionManager().AppendHistoryNodes(ctx, request)
	if resp != nil {
		size = resp.Size
	}
	return size, err0
}

func (s *ContextImpl) DeleteWorkflowExecution(
	ctx context.Context,
	key definition.WorkflowKey,
	branchToken []byte,
	startTime *time.Time,
	closeTime *time.Time,
) (retErr error) {
	// DeleteWorkflowExecution is a 4-steps process (order is very important and should not be changed):
	// 1. Add visibility delete task, i.e. schedule visibility record delete,
	// 2. Delete current workflow execution pointer,
	// 3. Delete workflow mutable state,
	// 4. Delete history branch.

	// This function is called from task processor and should not be called directly.
	// It may fail at any step and task processor will retry. All steps are idempotent.

	// If process fails after step 1 then workflow execution becomes invisible but mutable state is still there and task can be safely retried.
	// Step 2 doesn't affect mutable state neither and doesn't block retry.
	// After step 3 task can't be retried because mutable state is gone and this might leave history branch in DB.
	// The history branch won't be accessible (because mutable state is deleted) and special garbage collection workflow will delete it eventually.
	// Step 4 shouldn't be done earlier because if this func fails after it, workflow execution will be accessible but won't have history (inconsistent state).

	ctx, cancel, err := s.ensureMinContextTimeout(ctx)
	if err != nil {
		return err
	}
	defer cancel()

	engine, err := s.GetEngine(ctx)
	if err != nil {
		return err
	}

	// Do not get namespace cache within shard lock.
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(key.NamespaceID))
	deleteVisibilityRecord := true
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
			// If namespace is not found, skip visibility record delete but proceed with other deletions.
			// This case might happen during namespace deletion.
			deleteVisibilityRecord = false
		} else {
			return err
		}
	}

	var newTasks map[tasks.Category][]tasks.Task
	defer func() {
		if OperationPossiblySucceeded(retErr) && newTasks != nil {
			engine.NotifyNewTasks(namespaceEntry.ActiveClusterName(), newTasks)
		}
	}()

	// Wrap step 1 and 2 with function to release the lock with defer after step 2.
	err = func() error {
		s.wLock()
		defer s.wUnlock()

		if err := s.errorByStateLocked(); err != nil {
			return err
		}

		// Step 1. Delete visibility.
		if deleteVisibilityRecord {
			// TODO: move to existing task generator logic
			newTasks = map[tasks.Category][]tasks.Task{
				tasks.CategoryVisibility: {
					&tasks.DeleteExecutionVisibilityTask{
						// TaskID is set by addTasksLocked
						WorkflowKey:         key,
						VisibilityTimestamp: s.timeSource.Now(),
						StartTime:           startTime,
						CloseTime:           closeTime,
					},
				},
			}
			addTasksRequest := &persistence.AddHistoryTasksRequest{
				ShardID:     s.shardID,
				NamespaceID: key.NamespaceID,
				WorkflowID:  key.WorkflowID,
				RunID:       key.RunID,

				Tasks: newTasks,
			}
			err = s.addTasksLocked(ctx, addTasksRequest, namespaceEntry)
			if err != nil {
				return err
			}
		}

		// Step 2. Delete current workflow execution pointer.
		delCurRequest := &persistence.DeleteCurrentWorkflowExecutionRequest{
			ShardID:     s.shardID,
			NamespaceID: key.NamespaceID,
			WorkflowID:  key.WorkflowID,
			RunID:       key.RunID,
		}
		err = s.GetExecutionManager().DeleteCurrentWorkflowExecution(ctx, delCurRequest)
		return err
	}()

	if err != nil {
		return err
	}

	// Step 3. Delete workflow mutable state.
	delRequest := &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     s.shardID,
		NamespaceID: key.NamespaceID,
		WorkflowID:  key.WorkflowID,
		RunID:       key.RunID,
	}
	err = s.GetExecutionManager().DeleteWorkflowExecution(ctx, delRequest)
	if err != nil {
		return err
	}

	// Step 4. Delete history branch.
	if branchToken != nil {
		delHistoryRequest := &persistence.DeleteHistoryBranchRequest{
			BranchToken: branchToken,
			ShardID:     s.shardID,
		}
		err = s.GetExecutionManager().DeleteHistoryBranch(ctx, delHistoryRequest)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *ContextImpl) GetConfig() *configs.Config {
	// constant from initialization, no need for locks
	return s.config
}

func (s *ContextImpl) GetEventsCache() events.Cache {
	// constant from initialization (except for tests), no need for locks
	return s.eventsCache
}

func (s *ContextImpl) GetLogger() log.Logger {
	// constant from initialization, no need for locks
	return s.contextTaggedLogger
}

func (s *ContextImpl) GetThrottledLogger() log.Logger {
	// constant from initialization, no need for locks
	return s.throttledLogger
}

func (s *ContextImpl) getRangeIDLocked() int64 {
	return s.shardInfo.GetRangeId()
}

func (s *ContextImpl) errorByState() error {
	s.rLock()
	defer s.rUnlock()

	return s.errorByStateLocked()
}

func (s *ContextImpl) errorByStateLocked() error {
	switch s.state {
	case contextStateInitialized, contextStateAcquiring:
		return ErrShardStatusUnknown
	case contextStateAcquired:
		return nil
	case contextStateStopping, contextStateStopped:
		return ErrShardClosed
	default:
		panic("invalid state")
	}
}

func (s *ContextImpl) generateTaskIDLocked() (int64, error) {
	if err := s.updateRangeIfNeededLocked(); err != nil {
		return -1, err
	}

	taskID := s.taskSequenceNumber
	s.taskSequenceNumber++

	return taskID, nil
}

func (s *ContextImpl) updateRangeIfNeededLocked() error {
	if s.taskSequenceNumber < s.maxTaskSequenceNumber {
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

	ctx, cancel := context.WithTimeout(s.lifecycleCtx, shardIOTimeout)
	defer cancel()
	err := s.persistenceShardManager.UpdateShard(ctx, &persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo.ShardInfo,
		PreviousRangeID: s.shardInfo.GetRangeId(),
	})
	if err != nil {
		// Failure in updating shard to grab new RangeID
		s.contextTaggedLogger.Error("Persistent store operation failure",
			tag.StoreOperationUpdateShard,
			tag.Error(err),
			tag.ShardRangeID(updatedShardInfo.GetRangeId()),
			tag.PreviousShardRangeID(s.shardInfo.GetRangeId()),
		)
		return s.handleWriteErrorLocked(err)
	}

	// Range is successfully updated in cassandra now update shard context to reflect new range
	s.contextTaggedLogger.Info("Range updated for shardID",
		tag.ShardRangeID(updatedShardInfo.RangeId),
		tag.PreviousShardRangeID(s.shardInfo.RangeId),
		tag.Number(s.taskSequenceNumber),
		tag.NextNumber(s.maxTaskSequenceNumber),
	)

	s.taskSequenceNumber = updatedShardInfo.GetRangeId() << s.config.RangeSizeBits
	s.maxTaskSequenceNumber = (updatedShardInfo.GetRangeId() + 1) << s.config.RangeSizeBits
	s.immediateTaskExclusiveMaxReadLevel = s.taskSequenceNumber
	s.shardInfo = updatedShardInfo

	return nil
}

func (s *ContextImpl) updateMaxReadLevelLocked(rl int64) {
	if rl > s.immediateTaskExclusiveMaxReadLevel {
		s.contextTaggedLogger.Debug("Updating MaxTaskID", tag.MaxLevel(rl))
		s.immediateTaskExclusiveMaxReadLevel = rl
	}
}

func (s *ContextImpl) updateShardInfoLocked() error {
	if err := s.errorByStateLocked(); err != nil {
		return err
	}

	var err error
	now := clock.NewRealTimeSource().Now()
	if s.lastUpdated.Add(s.config.ShardUpdateMinInterval()).After(now) {
		return nil
	}
	updatedShardInfo := copyShardInfo(s.shardInfo)
	s.emitShardInfoMetricsLogsLocked()

	ctx, cancel := context.WithTimeout(s.lifecycleCtx, shardIOTimeout)
	defer cancel()
	err = s.persistenceShardManager.UpdateShard(ctx, &persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo.ShardInfo,
		PreviousRangeID: s.shardInfo.GetRangeId(),
	})
	if err != nil {
		return s.handleWriteErrorLocked(err)
	}

	s.lastUpdated = now
	return nil
}

// TODO: Instead of having separate metric definition for each task category, we should
// use one metrics (or two, one for immedidate task, one for scheduled task),
// and add tags indicating the task category.
func (s *ContextImpl) emitShardInfoMetricsLogsLocked() {
	currentCluster := s.GetClusterMetadata().GetCurrentClusterName()
	clusterInfo := s.GetClusterMetadata().GetAllClusterInfo()

	minTransferLevel := s.getQueueClusterAckLevelLocked(tasks.CategoryTransfer, currentCluster) // s.shardInfo.ClusterTransferAckLevel[currentCluster]
	maxTransferLevel := minTransferLevel
	minTimerLevel := s.getQueueClusterAckLevelLocked(tasks.CategoryTimer, currentCluster)
	maxTimerLevel := minTimerLevel
	for clusterName, info := range clusterInfo {
		if !info.Enabled {
			continue
		}

		clusterTransferLevel := s.getQueueClusterAckLevelLocked(tasks.CategoryTransfer, clusterName)
		if clusterTransferLevel.CompareTo(minTransferLevel) < 0 {
			minTransferLevel = clusterTransferLevel
		}
		if clusterTransferLevel.CompareTo(maxTransferLevel) > 0 {
			maxTransferLevel = clusterTransferLevel
		}

		clusterTimerLevel := s.getQueueClusterAckLevelLocked(tasks.CategoryTimer, clusterName)
		if clusterTimerLevel.CompareTo(minTimerLevel) < 0 {
			minTimerLevel = clusterTimerLevel
		}
		if clusterTimerLevel.CompareTo(maxTimerLevel) > 0 {
			maxTimerLevel = clusterTimerLevel
		}
	}

	diffTransferLevel := maxTransferLevel.TaskID - minTransferLevel.TaskID
	diffTimerLevel := maxTimerLevel.FireTime.Sub(minTimerLevel.FireTime)

	replicationLag := s.immediateTaskExclusiveMaxReadLevel - s.getQueueAckLevelLocked(tasks.CategoryReplication).TaskID - 1
	transferLag := s.immediateTaskExclusiveMaxReadLevel - s.getQueueAckLevelLocked(tasks.CategoryTransfer).TaskID - 1
	timerLag := s.timeSource.Now().Sub(s.getQueueAckLevelLocked(tasks.CategoryTimer).FireTime)
	visibilityLag := s.immediateTaskExclusiveMaxReadLevel - s.getQueueAckLevelLocked(tasks.CategoryVisibility).TaskID - 1

	transferFailoverInProgress := len(s.shardInfo.FailoverLevels[tasks.CategoryTransfer])
	timerFailoverInProgress := len(s.shardInfo.FailoverLevels[tasks.CategoryTimer])

	if s.config.EmitShardDiffLog() &&
		(logWarnImmediateTaskLevelDiff < diffTransferLevel ||
			logWarnScheduledTaskLevelDiff < diffTimerLevel ||
			logWarnImmediateTaskLevelDiff < transferLag ||
			logWarnScheduledTaskLevelDiff < timerLag ||
			logWarnImmediateTaskLevelDiff < visibilityLag ||
			logWarnImmediateTaskLevelDiff < replicationLag) {

		ackLevelTags := make([]tag.Tag, 0, len(s.shardInfo.QueueAckLevels))
		for categoryID, ackLevel := range s.shardInfo.QueueAckLevels {
			category, ok := tasks.GetCategoryByID(categoryID)
			if !ok {
				continue
			}
			ackLevelTags = append(ackLevelTags, tag.ShardQueueAcks(category.Name(), ackLevel))
		}
		s.contextTaggedLogger.Warn("Shard ack levels diff exceeds warn threshold.", ackLevelTags...)
	}

	metricsScope := s.GetMetricsClient().Scope(metrics.ShardInfoScope)
	metricsScope.RecordDistribution(metrics.ShardInfoTransferDiffHistogram, int(diffTransferLevel))
	metricsScope.RecordTimer(metrics.ShardInfoTimerDiffTimer, diffTimerLevel)

	metricsScope.RecordDistribution(metrics.ShardInfoReplicationLagHistogram, int(replicationLag))
	metricsScope.RecordDistribution(metrics.ShardInfoTransferLagHistogram, int(transferLag))
	metricsScope.RecordTimer(metrics.ShardInfoTimerLagTimer, timerLag)
	metricsScope.RecordDistribution(metrics.ShardInfoVisibilityLagHistogram, int(visibilityLag))

	metricsScope.RecordDistribution(metrics.ShardInfoTransferFailoverInProgressHistogram, transferFailoverInProgress)
	metricsScope.RecordDistribution(metrics.ShardInfoTimerFailoverInProgressHistogram, timerFailoverInProgress)
}

func (s *ContextImpl) allocateTaskIDsLocked(
	namespaceEntry *namespace.Namespace,
	workflowID string,
	newTasks map[tasks.Category][]tasks.Task,
	transferExclusiveMaxReadLevel *int64,
) error {
	currentCluster := s.GetClusterMetadata().GetCurrentClusterName()
	for category, tasksByCategory := range newTasks {
		for _, task := range tasksByCategory {
			// set taskID
			id, err := s.generateTaskIDLocked()
			if err != nil {
				return err
			}
			s.contextTaggedLogger.Debug("Assigning task ID", tag.TaskID(id))
			task.SetTaskID(id)
			*transferExclusiveMaxReadLevel = id + 1

			// if scheduled task, check if fire time is in the past
			if category.Type() == tasks.CategoryTypeScheduled {
				ts := task.GetVisibilityTime()
				if task.GetVersion() != common.EmptyVersion {
					// cannot use version to determine the corresponding cluster for timer task
					// this is because during failover, timer task should be created as active
					// or otherwise, failover + active processing logic may not pick up the task.
					currentCluster = namespaceEntry.ActiveClusterName()
				}
				readCursorTS := s.scheduledTaskMaxReadLevelMap[currentCluster]
				if ts.Before(readCursorTS) {
					// This can happen if shard move and new host have a time SKU, or there is db write delay.
					// We generate a new timer ID using timerMaxReadLevel.
					s.contextTaggedLogger.Debug("New timer generated is less than read level",
						tag.WorkflowNamespaceID(namespaceEntry.ID().String()),
						tag.WorkflowID(workflowID),
						tag.Timestamp(ts),
						tag.CursorTimestamp(readCursorTS),
						tag.ValueShardAllocateTimerBeforeRead)
					task.SetVisibilityTime(s.scheduledTaskMaxReadLevelMap[currentCluster].Add(time.Millisecond))
				}

				visibilityTs := task.GetVisibilityTime()
				s.contextTaggedLogger.Debug("Assigning new timer",
					tag.Timestamp(visibilityTs), tag.TaskID(task.GetTaskID()), tag.MaxQueryLevel(readCursorTS))
			}
		}
	}
	return nil
}

func (s *ContextImpl) SetCurrentTime(cluster string, currentTime time.Time) {
	s.wLock()
	defer s.wUnlock()
	if cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		prevTime := s.getOrUpdateRemoteClusterInfoLocked(cluster).CurrentTime
		if prevTime.Before(currentTime) {
			s.getOrUpdateRemoteClusterInfoLocked(cluster).CurrentTime = currentTime
		}
	} else {
		panic("Cannot set current time for current cluster")
	}
}

func (s *ContextImpl) GetCurrentTime(cluster string) time.Time {
	if cluster != s.GetClusterMetadata().GetCurrentClusterName() {
		s.wLock()
		defer s.wUnlock()
		return s.getOrUpdateRemoteClusterInfoLocked(cluster).CurrentTime
	}
	return s.timeSource.Now().UTC()
}

func (s *ContextImpl) GetLastUpdatedTime() time.Time {
	s.rLock()
	defer s.rUnlock()
	return s.lastUpdated
}

func (s *ContextImpl) handleReadError(err error) error {
	switch err.(type) {
	case nil:
		return nil

	case *persistence.ShardOwnershipLostError:
		// Shard is stolen, trigger shutdown of history engine.
		// Handling of max read level doesn't matter here.
		s.Unload()
		return err

	default:
		return err
	}
}

func (s *ContextImpl) handleWriteErrorLocked(err error) error {
	// We can use 0 here since updateMaxReadLevelLocked ensures that the read level never goes backwards.
	return s.handleWriteErrorAndUpdateMaxReadLevelLocked(err, 0)
}

func (s *ContextImpl) handleWriteErrorAndUpdateMaxReadLevelLocked(err error, newMaxReadLevel int64) error {
	switch err.(type) {
	case nil:
		// Persistence success: update max read level
		s.updateMaxReadLevelLocked(newMaxReadLevel)
		return nil

	case *persistence.CurrentWorkflowConditionFailedError,
		*persistence.WorkflowConditionFailedError,
		*persistence.ConditionFailedError,
		*serviceerror.ResourceExhausted:
		// Persistence failure that means the write was definitely not committed:
		// No special handling required for these errors.
		return err

	case *persistence.ShardOwnershipLostError:
		// Shard is stolen, trigger shutdown of history engine.
		// Handling of max read level doesn't matter here.
		s.transitionLocked(contextRequestStop{})
		return err

	default:
		// We have no idea if the write failed or will eventually make it to persistence. Try to re-acquire
		// the shard in the background. If successful, we'll get a new RangeID, to guarantee that subsequent
		// reads will either see that write, or know for certain that it failed. This allows the callers to
		// reliably check the outcome by performing a read. If we fail, we'll shut down the shard.
		// Note that reacquiring the shard will cause the max read level to be updated
		// to the new range (i.e. past newMaxReadLevel).
		s.transitionLocked(contextRequestLost{})
		return err
	}
}

func (s *ContextImpl) maybeRecordShardAcquisitionLatency(ownershipChanged bool) {
	if ownershipChanged {
		s.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardContextAcquisitionLatency,
			s.GetCurrentTime(s.GetClusterMetadata().GetCurrentClusterName()).Sub(s.GetLastUpdatedTime()))
	}
}

func (s *ContextImpl) createEngine() Engine {
	s.contextTaggedLogger.Info("", tag.LifeCycleStarting, tag.ComponentShardEngine)
	engine := s.engineFactory.CreateEngine(s)
	engine.Start()
	s.contextTaggedLogger.Info("", tag.LifeCycleStarted, tag.ComponentShardEngine)
	return engine
}

// start should only be called by the controller.
func (s *ContextImpl) start() {
	s.wLock()
	defer s.wUnlock()
	s.transitionLocked(contextRequestAcquire{})
}

func (s *ContextImpl) Unload() {
	s.wLock()
	defer s.wUnlock()
	s.transitionLocked(contextRequestStop{})
}

// finishStop should only be called by the controller.
func (s *ContextImpl) finishStop() {
	// Do this again in case we skipped the stopping state, which could happen
	// when calling CloseShardByID or the controller is shutting down.
	s.lifecycleCancel()

	s.wLock()
	s.transitionLocked(contextRequestFinishStop{})

	// use a context that we know is cancelled so that this doesn't block
	engine, _ := s.engineFuture.Get(s.lifecycleCtx)
	s.wUnlock()

	// Stop the engine if it was running (outside the lock but before returning)
	if engine != nil {
		s.contextTaggedLogger.Info("", tag.LifeCycleStopping, tag.ComponentShardEngine)
		engine.Stop()
		s.contextTaggedLogger.Info("", tag.LifeCycleStopped, tag.ComponentShardEngine)
	}
}

func (s *ContextImpl) isValid() bool {
	s.rLock()
	defer s.rUnlock()
	return s.state < contextStateStopping
}

func (s *ContextImpl) wLock() {
	scope := metrics.ShardInfoScope
	s.metricsClient.IncCounter(scope, metrics.LockRequests)
	sw := s.metricsClient.StartTimer(scope, metrics.LockLatency)
	defer sw.Stop()

	s.rwLock.Lock()
}

func (s *ContextImpl) rLock() {
	scope := metrics.ShardInfoScope
	s.metricsClient.IncCounter(scope, metrics.LockRequests)
	sw := s.metricsClient.StartTimer(scope, metrics.LockLatency)
	defer sw.Stop()

	s.rwLock.RLock()
}

func (s *ContextImpl) wUnlock() {
	s.rwLock.Unlock()
}

func (s *ContextImpl) rUnlock() {
	s.rwLock.RUnlock()
}

func (s *ContextImpl) transitionLocked(request contextRequest) {
	/* State transitions:

	The normal pattern:
		Initialized
			controller calls start()
		Acquiring
			acquireShard gets the shard
		Acquired

	If we get a transient error from persistence:
		Acquired
			transient error: handleErrorLocked calls transitionLocked(contextRequestLost)
		Acquiring
			acquireShard gets the shard
		Acquired

	If we get shard ownership lost:
		Acquired
			ShardOwnershipLostError: handleErrorLocked calls transitionLocked(contextRequestStop)
		Stopping
			controller removes from map and calls finishStop()
		Stopped

	Stopping can be triggered internally (if we get a ShardOwnershipLostError, or fail to acquire the rangeid
	lock after several minutes) or externally (from controller, e.g. controller shutting down or admin force-
	unload shard). If it's triggered internally, we transition to Stopping, then make an asynchronous callback
	to controller, which will remove us from the map and call finishStop(), which will transition to Stopped and
	stop the engine. If it's triggered externally, we'll skip over Stopping and go straight to Stopped.

	If we want to stop, and the acquireShard goroutine is still running, we can't kill it, but we need a
	mechanism to make sure it doesn't make any persistence calls or state transitions. We make acquireShard
	check the state each time it acquires the lock, and do nothing if the state has changed to Stopping (or
	Stopped).

	Invariants:
	- Once state is Stopping, it can only go to Stopped.
	- Once state is Stopped, it can't go anywhere else.
	- At the start of acquireShard, state must be Acquiring.
	- By the end of acquireShard, state must not be Acquiring: either acquireShard set it to Acquired, or the
	  controller set it to Stopped.
	- If state is Acquiring, acquireShard should be running in the background.
	- Only acquireShard can use contextRequestAcquired (i.e. transition from Acquiring to Acquired).
	- Once state has reached Acquired at least once, and not reached Stopped, engine must be non-nil.
	- Only the controller may call start() and finishStop().
	- The controller must call finishStop() for every ContextImpl it creates.

	*/

	setStateAcquiring := func() {
		s.state = contextStateAcquiring
		go s.acquireShard()
	}

	setStateStopping := func() {
		s.state = contextStateStopping
		// The change in state should cause all write methods to fail, but just in case, set this also,
		// which will cause failures at the persistence level. (Note that if persistence is unavailable
		// and we couldn't even load the shard metadata, shardInfo may still be nil here.)
		if s.shardInfo != nil {
			s.shardInfo.RangeId = -1
		}
		// Cancel lifecycle context as soon as we know we're shutting down
		s.lifecycleCancel()
		// This will cause the controller to remove this shard from the map and then call s.finishStop()
		go s.closeCallback(s)
	}

	setStateStopped := func() {
		s.state = contextStateStopped
	}

	switch s.state {
	case contextStateInitialized:
		switch request.(type) {
		case contextRequestAcquire:
			setStateAcquiring()
			return
		case contextRequestStop:
			setStateStopping()
			return
		case contextRequestFinishStop:
			setStateStopped()
			return
		}
	case contextStateAcquiring:
		switch request.(type) {
		case contextRequestAcquire:
			return // nothing to do, already acquiring
		case contextRequestAcquired:
			s.state = contextStateAcquired
			return
		case contextRequestLost:
			return // nothing to do, already acquiring
		case contextRequestStop:
			setStateStopping()
			return
		case contextRequestFinishStop:
			setStateStopped()
			return
		}
	case contextStateAcquired:
		switch request.(type) {
		case contextRequestAcquire:
			return // nothing to to do, already acquired
		case contextRequestLost:
			setStateAcquiring()
			return
		case contextRequestStop:
			setStateStopping()
			return
		case contextRequestFinishStop:
			setStateStopped()
			return
		}
	case contextStateStopping:
		switch request.(type) {
		case contextRequestStop:
			// nothing to do, already stopping
			return
		case contextRequestFinishStop:
			setStateStopped()
			return
		}
	}
	s.contextTaggedLogger.Warn("invalid state transition request",
		tag.ShardContextState(int(s.state)),
		tag.ShardContextStateRequest(fmt.Sprintf("%T", request)),
	)
}

func (s *ContextImpl) loadShardMetadata(ownershipChanged *bool) error {
	// Only have to do this once, we can just re-acquire the rangeid lock after that
	s.rLock()

	if s.state >= contextStateStopping {
		s.rUnlock()
		return errStoppingContext
	}

	if s.shardInfo != nil {
		s.rUnlock()
		return nil
	}

	s.rUnlock()

	// We don't have any shardInfo yet, load it (outside of context rwlock)
	ctx, cancel := context.WithTimeout(s.lifecycleCtx, shardIOTimeout)
	defer cancel()
	resp, err := s.persistenceShardManager.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID:          s.shardID,
		LifecycleContext: s.lifecycleCtx,
	})
	if err != nil {
		s.contextTaggedLogger.Error("Failed to load shard", tag.Error(err))
		return err
	}
	shardInfo := &persistence.ShardInfoWithFailover{ShardInfo: resp.ShardInfo}

	// shardInfo is a fresh value, so we don't really need to copy, but
	// copyShardInfo also ensures that all maps are non-nil
	updatedShardInfo := copyShardInfo(shardInfo)
	*ownershipChanged = shardInfo.Owner != s.hostInfoProvider.HostInfo().Identity()
	updatedShardInfo.Owner = s.hostInfoProvider.HostInfo().Identity()

	// initialize the cluster current time to be the same as ack level
	remoteClusterInfos := make(map[string]*remoteClusterInfo)
	scheduledTaskMaxReadLevelMap := make(map[string]time.Time)
	currentClusterName := s.GetClusterMetadata().GetCurrentClusterName()
	taskCategories := tasks.GetCategories()
	for clusterName, info := range s.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		maxReadTime := tasks.DefaultFireTime
		for categoryID, queueAckLevels := range shardInfo.QueueAckLevels {
			category, ok := taskCategories[categoryID]
			if !ok || category.Type() != tasks.CategoryTypeScheduled {
				continue
			}

			if queueAckLevels.AckLevel != 0 {
				currentTime := timestamp.UnixOrZeroTime(queueAckLevels.AckLevel)
				maxReadTime = util.MaxTime(maxReadTime, currentTime)
			}

			if queueAckLevels.ClusterAckLevel != nil {
				if ackLevel, ok := queueAckLevels.ClusterAckLevel[clusterName]; ok {
					currentTime := timestamp.UnixOrZeroTime(ackLevel)
					maxReadTime = util.MaxTime(maxReadTime, currentTime)
				}
			}
		}

		for categoryID, queueState := range shardInfo.QueueStates {
			category, ok := taskCategories[categoryID]
			if !ok || category.Type() != tasks.CategoryTypeScheduled {
				continue
			}

			maxReadTime = util.MaxTime(maxReadTime, timestamp.TimeValue(queueState.ExclusiveReaderHighWatermark.FireTime))
		}

		scheduledTaskMaxReadLevelMap[clusterName] = maxReadTime.Truncate(time.Millisecond)

		if clusterName != currentClusterName {
			remoteClusterInfos[clusterName] = &remoteClusterInfo{CurrentTime: maxReadTime}
		}
	}

	s.wLock()
	defer s.wUnlock()

	if s.state >= contextStateStopping {
		return errStoppingContext
	}

	s.shardInfo = updatedShardInfo
	s.remoteClusterInfos = remoteClusterInfos
	s.scheduledTaskMaxReadLevelMap = scheduledTaskMaxReadLevelMap

	return nil
}

func (s *ContextImpl) GetReplicationStatus(cluster []string) (map[string]*historyservice.ShardReplicationStatusPerCluster, map[string]*historyservice.HandoverNamespaceInfo, error) {
	remoteClusters := make(map[string]*historyservice.ShardReplicationStatusPerCluster)
	handoverNamespaces := make(map[string]*historyservice.HandoverNamespaceInfo)
	s.rLock()
	defer s.rUnlock()

	if len(cluster) == 0 {
		// remote acked info for all known remote clusters
		for k, v := range s.remoteClusterInfos {
			remoteClusters[k] = &historyservice.ShardReplicationStatusPerCluster{
				AckedTaskId:             v.AckedReplicationTaskID,
				AckedTaskVisibilityTime: timestamp.TimePtr(v.AckedReplicationTimestamp),
			}
		}
	} else {
		for _, k := range cluster {
			if v, ok := s.remoteClusterInfos[k]; ok {
				remoteClusters[k] = &historyservice.ShardReplicationStatusPerCluster{
					AckedTaskId:             v.AckedReplicationTaskID,
					AckedTaskVisibilityTime: timestamp.TimePtr(v.AckedReplicationTimestamp),
				}
			}
		}
	}

	for k, v := range s.handoverNamespaces {
		handoverNamespaces[k] = &historyservice.HandoverNamespaceInfo{
			HandoverReplicationTaskId: v.MaxReplicationTaskID,
		}
	}

	return remoteClusters, handoverNamespaces, nil
}

func (s *ContextImpl) getOrUpdateRemoteClusterInfoLocked(clusterName string) *remoteClusterInfo {
	if info, ok := s.remoteClusterInfos[clusterName]; ok {
		return info
	}
	info := &remoteClusterInfo{
		AckedReplicationTaskID: persistence.EmptyQueueMessageID,
	}
	s.remoteClusterInfos[clusterName] = info
	return info
}

func (s *ContextImpl) acquireShard() {
	// This is called in two contexts: initially acquiring the rangeid lock, and trying to
	// re-acquire it after a persistence error. In both cases, we retry the acquire operation
	// (renewRangeLocked) for 5 minutes. Each individual attempt uses shardIOTimeout (10s) as
	// the timeout. This lets us handle a few minutes of persistence unavailability without
	// dropping and reloading the whole shard context, which is relatively expensive (includes
	// caches that would have to be refilled, etc.).
	//
	// We stop retrying on any of:
	// 1. We succeed in acquiring the rangeid lock.
	// 2. We get any error other than transient errors.
	// 3. The state changes to Stopping or Stopped.
	//
	// If the shard controller sees that service resolver has assigned ownership to someone
	// else, it will call finishStop, which will trigger case 3 above.
	policy := backoff.NewExponentialRetryPolicy(1 * time.Second)
	policy.SetExpirationInterval(5 * time.Minute)

	// Remember this value across attempts
	ownershipChanged := false

	op := func() error {
		// Initial load of shard metadata
		err := s.loadShardMetadata(&ownershipChanged)
		if err != nil {
			return err
		}

		s.wLock()
		defer s.wUnlock()

		// Check that we should still be running
		if s.state >= contextStateStopping {
			return errStoppingContext
		}

		// Try to acquire RangeID lock. If this gets a persistence error, it may call:
		// transitionLocked(contextRequestStop) for ShardOwnershipLostError:
		//   This will transition to Stopping right here, and the transitionLocked call at the end of the
		//   outer function will do nothing, since the state was already changed.
		// transitionLocked(contextRequestLost) for other transient errors:
		//   This will do nothing, since state is already Acquiring.
		err = s.renewRangeLocked(true)
		if err != nil {
			return err
		}

		s.contextTaggedLogger.Info("Acquired shard")

		// The first time we get the shard, we have to create the engine. We have to release the lock to
		// create the engine, and then reacquire it. This is safe because:
		// 1. We know we're currently in the Acquiring state. The only thing we can transition to (without
		//    doing it ourselves) is Stopped. In that case, we'll have to stop the engine that we just
		//    created, since the stop transition didn't do it.
		// 2. We don't have an engine yet, so no one should be calling any of our methods that mutate things.
		if !s.engineFuture.Ready() {
			s.wUnlock()
			s.maybeRecordShardAcquisitionLatency(ownershipChanged)
			engine := s.createEngine()
			s.wLock()
			if s.state >= contextStateStopping {
				engine.Stop()
				s.engineFuture.Set(nil, errStoppingContext)
				return errStoppingContext
			}
			s.engineFuture.Set(engine, nil)
		}

		s.transitionLocked(contextRequestAcquired{})
		return nil
	}

	err := backoff.ThrottleRetry(op, policy, common.IsPersistenceTransientError)
	if err == errStoppingContext {
		// State changed since this goroutine started, exit silently.
		return
	} else if err != nil {
		// We got an unretryable error (perhaps ShardOwnershipLostError) or timed out.
		s.contextTaggedLogger.Error("Couldn't acquire shard", tag.Error(err))

		// If there's been another state change since we started (e.g. to Stopping), then don't do anything
		// here. But if not (i.e. timed out or error), initiate shutting down the shard.
		s.wLock()
		defer s.wUnlock()
		if s.state >= contextStateStopping {
			return
		}
		s.transitionLocked(contextRequestStop{})
	}
}

func newContext(
	shardID int32,
	factory EngineFactory,
	config *configs.Config,
	closeCallback func(*ContextImpl),
	logger log.Logger,
	throttledLogger log.Logger,
	persistenceExecutionManager persistence.ExecutionManager,
	persistenceShardManager persistence.ShardManager,
	clientBean client.Bean,
	historyClient historyservice.HistoryServiceClient,
	metricsClient metrics.Client,
	metricsHandler metrics.MetricsHandler,
	payloadSerializer serialization.Serializer,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	saProvider searchattribute.Provider,
	saMapper searchattribute.Mapper,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	hostInfoProvider membership.HostInfoProvider,
) (*ContextImpl, error) {
	hostIdentity := hostInfoProvider.HostInfo().Identity()

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())

	shardContext := &ContextImpl{
		state:                   contextStateInitialized,
		shardID:                 shardID,
		executionManager:        persistenceExecutionManager,
		metricsClient:           metricsClient,
		metricsHandler:          metricsHandler,
		closeCallback:           closeCallback,
		config:                  config,
		contextTaggedLogger:     log.With(logger, tag.ShardID(shardID), tag.Address(hostIdentity)),
		throttledLogger:         log.With(throttledLogger, tag.ShardID(shardID), tag.Address(hostIdentity)),
		engineFactory:           factory,
		persistenceShardManager: persistenceShardManager,
		clientBean:              clientBean,
		historyClient:           historyClient,
		payloadSerializer:       payloadSerializer,
		timeSource:              timeSource,
		namespaceRegistry:       namespaceRegistry,
		saProvider:              saProvider,
		saMapper:                saMapper,
		clusterMetadata:         clusterMetadata,
		archivalMetadata:        archivalMetadata,
		hostInfoProvider:        hostInfoProvider,
		handoverNamespaces:      make(map[string]*namespaceHandOverInfo),
		lifecycleCtx:            lifecycleCtx,
		lifecycleCancel:         lifecycleCancel,
		engineFuture:            future.NewFuture[Engine](),
	}
	shardContext.eventsCache = events.NewEventsCache(
		shardContext.GetShardID(),
		shardContext.GetConfig().EventsCacheInitialSize(),
		shardContext.GetConfig().EventsCacheMaxSize(),
		shardContext.GetConfig().EventsCacheTTL(),
		shardContext.GetExecutionManager(),
		false,
		shardContext.GetLogger(),
		shardContext.GetMetricsClient(),
	)

	return shardContext, nil
}

// TODO: why do we need a deep copy here?
func copyShardInfo(shardInfo *persistence.ShardInfoWithFailover) *persistence.ShardInfoWithFailover {
	failoverLevels := make(map[tasks.Category]map[string]persistence.FailoverLevel)
	for category, levels := range shardInfo.FailoverLevels {
		failoverLevels[category] = maps.Clone(levels)
	}

	queueAckLevels := make(map[int32]*persistencespb.QueueAckLevel)
	for category, ackLevels := range shardInfo.QueueAckLevels {
		queueAckLevels[category] = &persistencespb.QueueAckLevel{
			AckLevel:        ackLevels.AckLevel,
			ClusterAckLevel: maps.Clone(ackLevels.ClusterAckLevel),
		}
	}

	shardInfoCopy := &persistence.ShardInfoWithFailover{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                      shardInfo.ShardId,
			Owner:                        shardInfo.Owner,
			RangeId:                      shardInfo.RangeId,
			StolenSinceRenew:             shardInfo.StolenSinceRenew,
			NamespaceNotificationVersion: shardInfo.NamespaceNotificationVersion,
			ReplicationDlqAckLevel:       maps.Clone(shardInfo.ReplicationDlqAckLevel),
			UpdateTime:                   shardInfo.UpdateTime,
			QueueAckLevels:               queueAckLevels,
			QueueStates:                  shardInfo.QueueStates,
		},
		FailoverLevels: failoverLevels,
	}

	return shardInfoCopy
}

func (s *ContextImpl) GetRemoteAdminClient(cluster string) (adminservice.AdminServiceClient, error) {
	return s.clientBean.GetRemoteAdminClient(cluster)
}

func (s *ContextImpl) GetPayloadSerializer() serialization.Serializer {
	return s.payloadSerializer
}

func (s *ContextImpl) GetHistoryClient() historyservice.HistoryServiceClient {
	return s.historyClient
}

func (s *ContextImpl) GetMetricsClient() metrics.Client {
	return s.metricsClient
}

func (s *ContextImpl) GetMetricsHandler() metrics.MetricsHandler {
	return s.metricsHandler
}

func (s *ContextImpl) GetTimeSource() clock.TimeSource {
	return s.timeSource
}

func (s *ContextImpl) GetNamespaceRegistry() namespace.Registry {
	return s.namespaceRegistry
}

func (s *ContextImpl) GetSearchAttributesProvider() searchattribute.Provider {
	return s.saProvider
}

func (s *ContextImpl) GetSearchAttributesMapper() searchattribute.Mapper {
	return s.saMapper
}

func (s *ContextImpl) GetClusterMetadata() cluster.Metadata {
	return s.clusterMetadata
}

func (s *ContextImpl) GetArchivalMetadata() archiver.ArchivalMetadata {
	return s.archivalMetadata
}

func (s *ContextImpl) ensureMinContextTimeout(
	ctx context.Context,
) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	deadline, ok := ctx.Deadline()
	if !ok || deadline.Sub(s.GetTimeSource().Now()) >= minContextTimeout {
		return ctx, func() {}, nil
	}

	newContext, cancel := context.WithTimeout(s.lifecycleCtx, minContextTimeout)
	return newContext, cancel, nil
}

func OperationPossiblySucceeded(err error) bool {
	if err == consts.ErrConflict {
		return false
	}

	switch err.(type) {
	case *persistence.CurrentWorkflowConditionFailedError,
		*persistence.WorkflowConditionFailedError,
		*persistence.ConditionFailedError,
		*persistence.ShardOwnershipLostError,
		*persistence.InvalidPersistenceRequestError,
		*persistence.TransactionSizeLimitError,
		*serviceerror.ResourceExhausted,
		*serviceerror.NotFound,
		*serviceerror.NamespaceNotFound:
		// Persistence failure that means that write was definitely not committed.
		return false
	default:
		return true
	}
}

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

func convertFromPersistenceTaskKey(
	key *persistencespb.TaskKey,
) tasks.Key {
	return tasks.NewKey(
		timestamp.TimeValue(key.FireTime),
		key.TaskId,
	)
}
