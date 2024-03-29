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
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/adminservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/backoff"
	cclock "go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
)

const (
	// See transition for overview of state transitions.
	// These are the possible values of ContextImpl.state:
	contextStateInitialized contextState = iota
	contextStateAcquiring
	contextStateAcquired
	contextStateStopping
	contextStateStopped
)

const (
	shardIOTimeout = 5 * time.Second * debug.TimeoutMultiplier
	// ShardUpdateQueueMetricsInterval is the minimum amount of time between updates to a shard's queue metrics
	queueMetricUpdateInterval = 5 * time.Minute

	pendingMaxReplicationTaskID = math.MaxInt64
)

var (
	shardContextSequenceID int64 = 0
)

type (
	contextState int32

	ContextImpl struct {
		// These fields are constant:
		shardID             int32
		owner               string
		stringRepr          string
		executionManager    persistence.ExecutionManager
		metricsHandler      metrics.Handler
		eventsCache         events.Cache
		closeCallback       CloseCallback
		config              *configs.Config
		contextTaggedLogger log.Logger
		throttledLogger     log.Logger
		engineFactory       EngineFactory
		engineFuture        *future.FutureImpl[Engine]
		queueMetricEmitter  sync.Once

		persistenceShardManager persistence.ShardManager
		clientBean              client.Bean
		historyClient           historyservice.HistoryServiceClient
		payloadSerializer       serialization.Serializer
		timeSource              cclock.TimeSource
		namespaceRegistry       namespace.Registry
		saProvider              searchattribute.Provider
		saMapperProvider        searchattribute.MapperProvider
		clusterMetadata         cluster.Metadata
		archivalMetadata        archiver.ArchivalMetadata
		hostInfoProvider        membership.HostInfoProvider
		taskCategoryRegistry    tasks.TaskCategoryRegistry

		// Context that lives for the lifetime of the shard context
		lifecycleCtx    context.Context
		lifecycleCancel context.CancelFunc

		// ioSemaphore is used to control the concurrency of shard I/O requests.
		// Despite its name, this semaphore is only applied to persistence requests that
		// could cause potentially contention/conflict with each other.
		// For cassandra, this basically means requests that use LWT.
		// It's ok to use semaphore by its own or lock rwLock within the semaphore.
		// But DO NOT try to acquire ioSemaphore while holding rwLock, as it may cause deadlock.
		ioSemaphore *semaphore.Weighted

		// state is protected by stateLock
		stateLock  sync.Mutex
		state      contextState
		stopReason stopReason

		// All following fields are protected by rwLock, and only valid if state >= Acquiring:
		rwLock                        sync.RWMutex
		lastUpdated                   time.Time
		tasksCompletedSinceLastUpdate int
		shardInfo                     *persistencespb.ShardInfo

		// All methods of the taskKeyManager, except the completionFn returned by
		// setAndTrackTaskKeys, must be invoked within rwLock.
		// NOTE: The completionFn must be invoked outside rwLock because when
		// renewing rangeID, the rwLock will be held when waiting for in-flight
		// requests to complete.
		taskKeyManager *taskKeyManager

		// exist only in memory
		remoteClusterInfos      map[string]*remoteClusterInfo
		handoverNamespaces      map[namespace.Name]*namespaceHandOverInfo // keyed on namespace name
		acquireShardRetryPolicy backoff.RetryPolicy

		stateMachineRegistry *hsm.Registry
	}

	remoteClusterInfo struct {
		CurrentTime                time.Time
		AckedReplicationTaskIDs    map[int32]int64
		AckedReplicationTimestamps map[int32]time.Time
	}

	namespaceHandOverInfo struct {
		MaxReplicationTaskID int64
		NotificationVersion  int64
	}

	// These are the requests that can be passed to transition to change state:
	contextRequest interface{}

	contextRequestAcquire    struct{}
	contextRequestAcquired   struct{ engine Engine }
	contextRequestLost       struct{}
	contextRequestStop       struct{ reason stopReason }
	contextRequestFinishStop struct{}

	stopReason int
)

const (
	stopReasonUnspecified stopReason = iota
	stopReasonOwnershipLost
)

var _ Context = (*ContextImpl)(nil)

var (
	// ErrShardStatusUnknown means we're not sure if we have the shard lock or not. This may be returned
	// during short windows at initialization and if we've lost the connection to the database.
	ErrShardStatusUnknown = serviceerror.NewUnavailable("shard status unknown")

	// errInvalidTransition is an internal error used for acquireShard and transition
	errInvalidTransition = errors.New("invalid state transition request")
)

const (
	logWarnImmediateTaskLag = 3000000 // 3 million
	logWarnScheduledTaskLag = time.Duration(30 * time.Minute)
	historySizeLogThreshold = 10 * 1024 * 1024
	minContextTimeout       = 2 * time.Second * debug.TimeoutMultiplier
)

func (s *ContextImpl) String() string {
	// constant from initialization, no need for locks
	return s.stringRepr
}

func (s *ContextImpl) GetShardID() int32 {
	// constant from initialization, no need for locks
	return s.shardID
}

func (s *ContextImpl) GetRangeID() int64 {
	s.rLock()
	defer s.rUnlock()

	return s.getRangeIDLocked()
}

func (s *ContextImpl) GetOwner() string {
	// constant from initialization, no need for locks
	return s.owner
}

func (s *ContextImpl) GetExecutionManager() persistence.ExecutionManager {
	// constant from initialization, no need for locks
	return s.executionManager
}

func (s *ContextImpl) GetPingChecks() []common.PingCheck {
	return []common.PingCheck{
		{
			Name: s.String() + "-shard-lock",
			// rwLock may be held for the duration of renewing shard rangeID, which are called with a
			// timeout of shardIOTimeout. add a few more seconds for reliability.
			Timeout: shardIOTimeout + 5*time.Second,
			Ping: func() []common.Pingable {
				// call rwLock.Lock directly to bypass metrics since this isn't a real request
				s.rwLock.Lock()
				//nolint:staticcheck // SA2001 just checking if we can acquire the lock
				s.rwLock.Unlock()
				return nil
			},
			MetricsName: metrics.DDShardLockLatency.Name(),
		},
		{
			Name: s.String() + "-io-semaphore",
			// ioSemaphore is for the duration of a persistence op which has a persistence connection timeout
			// of 10 sec.
			Timeout: 10 * time.Second,
			Ping: func() []common.Pingable {
				_ = s.ioSemaphore.Acquire(context.Background(), 1)
				s.ioSemaphore.Release(1)
				return nil
			},
			MetricsName: metrics.DDShardIOSemaphoreLatency.Name(),
		},
	}
}

func (s *ContextImpl) GetEngine(
	ctx context.Context,
) (Engine, error) {
	return s.engineFuture.Get(ctx)
}

func (s *ContextImpl) AssertOwnership(
	ctx context.Context,
) error {
	if err := s.ioSemaphoreAcquire(ctx); err != nil {
		return err
	}
	defer s.ioSemaphoreRelease()

	s.wLock()

	// timeout check should be done within the shard lock, in case of shard lock contention
	ctx, cancel, err := s.newDetachedContext(ctx)
	if err != nil {
		s.wUnlock()
		return err
	}
	defer cancel()

	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return err
	}

	request := &persistence.AssertShardOwnershipRequest{
		ShardID: s.shardID,
		RangeID: s.getRangeIDLocked(),
	}
	s.wUnlock()

	err = s.persistenceShardManager.AssertShardOwnership(ctx, request)
	return s.handleWriteError(request.RangeID, err)
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

	nextTaskKey := s.taskKeyManager.peekTaskKey(tasks.CategoryTransfer)
	return vclock.NewVectorClock(s.clusterMetadata.GetClusterID(), s.shardID, nextTaskKey.TaskID)
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
) tasks.Key {
	s.wLock()
	defer s.wUnlock()

	return s.taskKeyManager.getExclusiveReaderHighWatermark(category)
}

func (s *ContextImpl) GetQueueState(
	category tasks.Category,
) (*persistencespb.QueueState, bool) {
	s.rLock()
	defer s.rUnlock()

	queueState, ok := s.shardInfo.QueueStates[int32(category.ID())]
	if !ok {
		return nil, false
	}
	// need to make a deep copy, in case UpdateReplicationQueueReaderState does a partial update
	blob, _ := serialization.QueueStateToBlob(queueState)
	queueState, _ = serialization.QueueStateFromBlob(blob.Data, blob.EncodingType.String())
	return queueState, ok
}

func (s *ContextImpl) SetQueueState(
	category tasks.Category,
	tasksCompleted int,
	state *persistencespb.QueueState,
) error {
	return s.updateShardInfo(tasksCompleted,
		func() {
			categoryID := category.ID()
			s.shardInfo.QueueStates[int32(categoryID)] = state
		})
}

func (s *ContextImpl) UpdateReplicationQueueReaderState(
	readerID int64,
	readerState *persistencespb.QueueReaderState,
) error {
	// TODO(timods): Determine whether this makes sense for replication
	return s.updateShardInfo(0, func() {
		categoryID := tasks.CategoryReplication.ID()
		queueState, ok := s.shardInfo.QueueStates[int32(categoryID)]
		if !ok {
			queueState = &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: nil,
				ReaderStates:                 make(map[int64]*persistencespb.QueueReaderState),
			}
			s.shardInfo.QueueStates[int32(categoryID)] = queueState
		}
		queueState.ReaderStates[readerID] = readerState
	})
}

// UpdateRemoteClusterInfo deprecated
// Deprecated use UpdateRemoteReaderInfo in the future instead
func (s *ContextImpl) UpdateRemoteClusterInfo(
	clusterName string,
	ackTaskID int64,
	ackTimestamp time.Time,
) {
	s.wLock()
	defer s.wUnlock()

	clusterInfo := s.clusterMetadata.GetAllClusterInfo()
	remoteClusterInfo := s.getOrUpdateRemoteClusterInfoLocked(clusterName)
	for _, remoteShardID := range common.MapShardID(
		clusterInfo[s.clusterMetadata.GetCurrentClusterName()].ShardCount,
		clusterInfo[clusterName].ShardCount,
		s.shardID,
	) {
		remoteClusterInfo.AckedReplicationTaskIDs[remoteShardID] = ackTaskID
		remoteClusterInfo.AckedReplicationTimestamps[remoteShardID] = ackTimestamp
	}
}

// UpdateRemoteReaderInfo do not use streaming replication until remoteClusterInfo is updated to allow both
// streaming & pull based replication
func (s *ContextImpl) UpdateRemoteReaderInfo(
	readerID int64,
	ackTaskID int64,
	ackTimestamp time.Time,
) error {
	clusterID, shardID := ReplicationReaderIDToClusterShardID(readerID)
	clusterName, _, ok := clusterNameInfoFromClusterID(s.clusterMetadata.GetAllClusterInfo(), clusterID)
	if !ok {
		// cluster is not present in cluster metadata map
		return serviceerror.NewInternal(fmt.Sprintf("unknown cluster ID: %v", clusterID))
	}

	s.wLock()
	defer s.wUnlock()

	remoteClusterInfo := s.getOrUpdateRemoteClusterInfoLocked(clusterName)
	remoteClusterInfo.AckedReplicationTaskIDs[shardID] = ackTaskID
	remoteClusterInfo.AckedReplicationTimestamps[shardID] = ackTimestamp
	return nil
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
	// TODO(timods): Determine whether this makes sense for replication
	if err := s.updateShardInfo(0, func() {
		s.shardInfo.ReplicationDlqAckLevel[sourceCluster] = ackLevel
	}); err != nil {
		return err
	}

	metrics.ReplicationDLQAckLevelGauge.With(s.GetMetricsHandler()).
		Record(float64(ackLevel),
			metrics.OperationTag(metrics.ReplicationDLQStatsScope),
			metrics.TargetClusterTag(sourceCluster),
			metrics.InstanceTag(convert.Int32ToString(s.shardID)))
	return nil
}

func (s *ContextImpl) UpdateHandoverNamespace(ns *namespace.Namespace, deletedFromDb bool) {
	nsName := ns.Name()
	// NOTE: replication state field won't be replicated and currently we only update a namespace
	// to handover state from active cluster, so the second condition will always be true. Adding
	// it here to be more safe in case above assumption no longer holds in the future.
	isHandoverNamespace := ns.IsGlobalNamespace() &&
		ns.ActiveInCluster(s.GetClusterMetadata().GetCurrentClusterName()) &&
		ns.ReplicationState() == enums.REPLICATION_STATE_HANDOVER

	s.wLock()
	if deletedFromDb || !isHandoverNamespace {
		delete(s.handoverNamespaces, ns.Name())
		s.wUnlock()
		return
	}

	maxReplicationTaskID := s.taskKeyManager.getExclusiveReaderHighWatermark(tasks.CategoryReplication).TaskID - 1
	if s.errorByState() != nil {
		// if shard state is not acquired, we don't know that's the max taskID
		// as there might be in-flight requests
		maxReplicationTaskID = pendingMaxReplicationTaskID
	}

	if handover, ok := s.handoverNamespaces[nsName]; ok {
		if handover.NotificationVersion < ns.NotificationVersion() {
			handover.NotificationVersion = ns.NotificationVersion()
			handover.MaxReplicationTaskID = maxReplicationTaskID
		}
	} else {
		s.handoverNamespaces[nsName] = &namespaceHandOverInfo{
			NotificationVersion:  ns.NotificationVersion(),
			MaxReplicationTaskID: maxReplicationTaskID,
		}
	}

	s.wUnlock()

	if maxReplicationTaskID != pendingMaxReplicationTaskID {
		// notification is for making sure replication queue is able to
		// ack to the recorded taskID. If the taskID is pending, then
		// don't notify. Otherwise, replication queue will think (for a period of time)
		// that the max generated taskID is pendingMaxReplicationTaskID which is MaxInt64.
		s.notifyReplicationQueueProcessor(maxReplicationTaskID)
	}
}

func (s *ContextImpl) AddTasks(
	ctx context.Context,
	request *persistence.AddHistoryTasksRequest,
) error {
	engine, err := s.GetEngine(ctx)
	if err != nil {
		return err
	}

	if err := s.ioSemaphoreAcquire(ctx); err != nil {
		return err
	}
	defer s.ioSemaphoreRelease()

	err = s.addTasksSemaphoreAcquired(ctx, request)
	if OperationPossiblySucceeded(err) {
		engine.NotifyNewTasks(request.Tasks)
	}
	return err
}

func (s *ContextImpl) AddSpeculativeWorkflowTaskTimeoutTask(
	task *tasks.WorkflowTaskTimeoutTask,
) error {
	// Use a cancelled context to avoid blocking if engineFuture is not ready.
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// err should never be returned here. engineFuture must always be ready.
	engine, err := s.engineFuture.Get(cancelledCtx)
	if err != nil {
		return err
	}

	engine.AddSpeculativeWorkflowTaskTimeoutTask(task)

	return nil
}

func (s *ContextImpl) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {

	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.NewWorkflowSnapshot.ExecutionInfo.NamespaceId)
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if err := s.ioSemaphoreAcquire(ctx); err != nil {
		return nil, err
	}
	defer s.ioSemaphoreRelease()

	s.wLock()

	// timeout check should be done within the shard lock, in case of shard lock contention
	ctx, cancel, err := s.newDetachedContext(ctx)
	if err != nil {
		s.wUnlock()
		return nil, err
	}
	defer cancel()

	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return nil, err
	}

	if err := s.errorByNamespaceStateLocked(namespaceEntry.Name()); err != nil {
		s.wUnlock()
		return nil, err
	}

	requestCompletionFn, err := s.taskKeyManager.setAndTrackTaskKeys(
		request.NewWorkflowSnapshot.Tasks,
	)
	if err != nil {
		s.wUnlock()
		return nil, err
	}
	s.updateCloseTaskIDs(request.NewWorkflowSnapshot.ExecutionInfo, request.NewWorkflowSnapshot.Tasks)

	currentRangeID := s.getRangeIDLocked()
	request.RangeID = currentRangeID

	s.wUnlock()
	resp, err := s.executionManager.CreateWorkflowExecution(ctx, request)
	requestCompletionFn(err)

	if err = s.handleWriteError(request.RangeID, err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.UpdateWorkflowMutation.ExecutionInfo.NamespaceId)
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if err := s.ioSemaphoreAcquire(ctx); err != nil {
		return nil, err
	}
	defer s.ioSemaphoreRelease()

	s.wLock()

	// timeout check should be done within the shard lock, in case of shard lock contention
	ctx, cancel, err := s.newDetachedContext(ctx)
	if err != nil {
		s.wUnlock()
		return nil, err
	}
	defer cancel()

	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return nil, err
	}

	if err := s.errorByNamespaceStateLocked(namespaceEntry.Name()); err != nil {
		s.wUnlock()
		return nil, err
	}

	taskMaps := make([]map[tasks.Category][]tasks.Task, 0, 2)
	taskMaps = append(taskMaps, request.UpdateWorkflowMutation.Tasks)
	if request.NewWorkflowSnapshot != nil {
		taskMaps = append(taskMaps, request.NewWorkflowSnapshot.Tasks)
	}
	requestCompletionFn, err := s.taskKeyManager.setAndTrackTaskKeys(taskMaps...)
	if err != nil {
		s.wUnlock()
		return nil, err
	}
	s.updateCloseTaskIDs(request.UpdateWorkflowMutation.ExecutionInfo, request.UpdateWorkflowMutation.Tasks)
	if request.NewWorkflowSnapshot != nil {
		s.updateCloseTaskIDs(request.NewWorkflowSnapshot.ExecutionInfo, request.NewWorkflowSnapshot.Tasks)
	}

	request.RangeID = s.getRangeIDLocked()
	s.wUnlock()

	resp, err := s.executionManager.UpdateWorkflowExecution(ctx, request)
	requestCompletionFn(err)
	if err = s.handleWriteError(request.RangeID, err); err != nil {
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
	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.ResetWorkflowSnapshot.ExecutionInfo.NamespaceId)
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if err := s.ioSemaphoreAcquire(ctx); err != nil {
		return nil, err
	}
	defer s.ioSemaphoreRelease()

	s.wLock()

	// timeout check should be done within the shard lock, in case of shard lock contention
	ctx, cancel, err := s.newDetachedContext(ctx)
	if err != nil {
		s.wUnlock()
		return nil, err
	}
	defer cancel()

	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return nil, err
	}

	if err := s.errorByNamespaceStateLocked(namespaceEntry.Name()); err != nil {
		s.wUnlock()
		return nil, err
	}

	taskMaps := make([]map[tasks.Category][]tasks.Task, 0, 3)
	if request.CurrentWorkflowMutation != nil {
		taskMaps = append(taskMaps, request.CurrentWorkflowMutation.Tasks)
	}
	taskMaps = append(taskMaps, request.ResetWorkflowSnapshot.Tasks)
	if request.NewWorkflowSnapshot != nil {
		taskMaps = append(taskMaps, request.NewWorkflowSnapshot.Tasks)
	}

	requestCompletionFn, err := s.taskKeyManager.setAndTrackTaskKeys(taskMaps...)
	if err != nil {
		s.wUnlock()
		return nil, err
	}

	request.RangeID = s.getRangeIDLocked()
	s.wUnlock()

	resp, err := s.executionManager.ConflictResolveWorkflowExecution(ctx, request)
	requestCompletionFn(err)
	if err = s.handleWriteError(request.RangeID, err); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ContextImpl) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.SetWorkflowExecutionRequest,
) (*persistence.SetWorkflowExecutionResponse, error) {
	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.SetWorkflowSnapshot.ExecutionInfo.NamespaceId)
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if err := s.ioSemaphoreAcquire(ctx); err != nil {
		return nil, err
	}
	defer s.ioSemaphoreRelease()

	s.wLock()

	// timeout check should be done within the shard lock, in case of shard lock contention
	ctx, cancel, err := s.newDetachedContext(ctx)
	if err != nil {
		s.wUnlock()
		return nil, err
	}
	defer cancel()

	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return nil, err
	}

	if err := s.errorByNamespaceStateLocked(namespaceEntry.Name()); err != nil {
		s.wUnlock()
		return nil, err
	}

	snapShotRequestCompletionFn, err := s.taskKeyManager.setAndTrackTaskKeys(
		request.SetWorkflowSnapshot.Tasks,
	)
	if err != nil {
		s.wUnlock()
		return nil, err
	}

	request.RangeID = s.getRangeIDLocked()
	s.wUnlock()

	resp, err := s.executionManager.SetWorkflowExecution(ctx, request)
	snapShotRequestCompletionFn(err)
	if err = s.handleWriteError(request.RangeID, err); err != nil {
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
		// also return resp, for RebuildMutableState API
		return resp, err
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
		// also return resp, for RebuildMutableState API
		return resp, err
	}
	return resp, nil
}

func (s *ContextImpl) addTasksSemaphoreAcquired(
	ctx context.Context,
	request *persistence.AddHistoryTasksRequest,
) error {

	// do not try to get namespace cache within shard lock
	namespaceID := namespace.ID(request.NamespaceID)
	namespaceEntry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	s.wLock()

	// timeout check should be done within the shard lock, in case of shard lock contention
	ctx, cancel, err := s.newDetachedContext(ctx)
	if err != nil {
		s.wUnlock()
		return err
	}
	defer cancel()

	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return err
	}
	if err := s.errorByNamespaceStateLocked(namespaceEntry.Name()); err != nil {
		s.wUnlock()
		return err
	}

	requestCompletionFn, err := s.taskKeyManager.setAndTrackTaskKeys(
		request.Tasks,
	)
	if err != nil {
		s.wUnlock()
		return err
	}

	request.RangeID = s.getRangeIDLocked()
	s.wUnlock()

	err = s.executionManager.AddHistoryTasks(ctx, request)
	requestCompletionFn(err)
	return s.handleWriteError(request.RangeID, err)
}

func (s *ContextImpl) AppendHistoryEvents(
	ctx context.Context,
	request *persistence.AppendHistoryNodesRequest,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
) (int, error) {
	if err := s.errorByState(); err != nil {
		return 0, err
	}

	request.ShardID = s.shardID

	size := 0
	defer func() {
		// N.B. - Dual emit here makes sense so that we can see aggregate timer stats across all
		// namespaces along with the individual namespaces stats
		handler := s.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.SessionStatsScope))
		metrics.HistorySize.With(handler).Record(int64(size))
		if entry, err := s.GetNamespaceRegistry().GetNamespaceByID(namespaceID); err == nil && entry != nil {
			metrics.HistorySize.With(handler).
				Record(int64(size), metrics.NamespaceTag(entry.Name().String()))
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
	closeVisibilityTaskId int64,
	stage *tasks.DeleteWorkflowExecutionStage,
) (retErr error) {
	// DeleteWorkflowExecution is a 4 stages process (order is very important and should not be changed):
	// 1. Add visibility delete task, i.e. schedule visibility record delete,
	// 2. Delete current workflow execution pointer,
	// 3. Delete workflow mutable state,
	// 4. Delete history branch.

	// This function is called from task processor and should not be called directly.
	// It may fail at any stage and task processor will retry. All stages are idempotent.

	// If process fails after stage 1 then workflow execution becomes invisible but mutable state is still there and task can be safely retried.
	// Stage 2 doesn't affect mutable state neither and doesn't block retry.
	// After stage 3 task can't be retried because mutable state is gone and this might leave history branch in DB.
	// The history branch won't be accessible (because mutable state is deleted) and special garbage collection workflow will delete it eventually.
	// Stage 4 shouldn't be done earlier because if this func fails after it, workflow execution will be accessible but won't have history (inconsistent state).

	engine, err := s.GetEngine(ctx)
	if err != nil {
		return err
	}

	// Do not get namespace cache within shard lock.
	_, err = s.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(key.NamespaceID))
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

	validateCtxAndShardState := func() (context.Context, context.CancelFunc, error) {
		s.wLock()
		defer s.wUnlock()

		ctx, cancel, err := s.newDetachedContext(ctx)
		if err != nil {
			return nil, nil, err
		}

		if err := s.errorByState(); err != nil {
			cancel()
			return nil, nil, err
		}

		return ctx, cancel, nil
	}

	// Don't acquire shard lock or io semaphore if all stages that require lock are already processed.
	if !stage.IsProcessed(
		tasks.DeleteWorkflowExecutionStageVisibility |
			tasks.DeleteWorkflowExecutionStageCurrent |
			tasks.DeleteWorkflowExecutionStageMutableState) {

		// Wrap stage 1, 2, and 3 with function to release io semaphore with defer after stage 3.
		if err = func() error {

			if err := s.ioSemaphoreAcquire(ctx); err != nil {
				return err
			}
			defer s.ioSemaphoreRelease()

			// Stage 1. Delete visibility.
			if deleteVisibilityRecord && !stage.IsProcessed(tasks.DeleteWorkflowExecutionStageVisibility) {
				// TODO: move to existing task generator logic
				newTasks := map[tasks.Category][]tasks.Task{
					tasks.CategoryVisibility: {
						&tasks.DeleteExecutionVisibilityTask{
							// TaskID is set by addTasks
							WorkflowKey:                    key,
							VisibilityTimestamp:            s.timeSource.Now(),
							CloseExecutionVisibilityTaskID: closeVisibilityTaskId,
						},
					},
				}
				addTasksRequest := &persistence.AddHistoryTasksRequest{
					ShardID:     s.shardID,
					NamespaceID: key.NamespaceID,
					WorkflowID:  key.WorkflowID,

					Tasks: newTasks,
				}
				err := s.addTasksSemaphoreAcquired(ctx, addTasksRequest)
				if OperationPossiblySucceeded(err) {
					engine.NotifyNewTasks(newTasks)
				}
				if err != nil {
					return err
				}
			}
			stage.MarkProcessed(tasks.DeleteWorkflowExecutionStageVisibility)

			// Stage 2. Delete current workflow execution pointer.
			if !stage.IsProcessed(tasks.DeleteWorkflowExecutionStageCurrent) {
				ctx, cancel, err := validateCtxAndShardState()
				if err != nil {
					return err
				}
				defer cancel()
				delCurRequest := &persistence.DeleteCurrentWorkflowExecutionRequest{
					ShardID:     s.shardID,
					NamespaceID: key.NamespaceID,
					WorkflowID:  key.WorkflowID,
					RunID:       key.RunID,
				}
				if err := s.GetExecutionManager().DeleteCurrentWorkflowExecution(
					ctx,
					delCurRequest,
				); err != nil {
					return err
				}
			}
			stage.MarkProcessed(tasks.DeleteWorkflowExecutionStageCurrent)

			// Stage 3. Delete workflow mutable state.
			if !stage.IsProcessed(tasks.DeleteWorkflowExecutionStageMutableState) {
				ctx, cancel, err := validateCtxAndShardState()
				if err != nil {
					return err
				}
				defer cancel()
				delRequest := &persistence.DeleteWorkflowExecutionRequest{
					ShardID:     s.shardID,
					NamespaceID: key.NamespaceID,
					WorkflowID:  key.WorkflowID,
					RunID:       key.RunID,
				}
				if err = s.GetExecutionManager().DeleteWorkflowExecution(ctx, delRequest); err != nil {
					return err
				}
			}
			stage.MarkProcessed(tasks.DeleteWorkflowExecutionStageMutableState)

			return nil
		}(); err != nil {
			return err
		}
	}

	// Stage 4. Delete history branch.
	if branchToken != nil && !stage.IsProcessed(tasks.DeleteWorkflowExecutionStageHistory) {
		delHistoryRequest := &persistence.DeleteHistoryBranchRequest{
			BranchToken: branchToken,
			ShardID:     s.shardID,
		}
		err = s.GetExecutionManager().DeleteHistoryBranch(ctx, delHistoryRequest)
		if err != nil {
			return err
		}
	}
	stage.MarkProcessed(tasks.DeleteWorkflowExecutionStageHistory)
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
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	switch s.state {
	case contextStateInitialized, contextStateAcquiring:
		return ErrShardStatusUnknown
	case contextStateAcquired:
		return nil
	case contextStateStopping, contextStateStopped:
		return s.newShardClosedErrorWithShardID()
	default:
		panic("invalid state")
	}
}

func (s *ContextImpl) errorByNamespaceStateLocked(
	namespaceName namespace.Name,
) error {
	if _, ok := s.handoverNamespaces[namespaceName]; ok {
		return consts.ErrNamespaceHandover
	}
	return nil
}

func (s *ContextImpl) generateTaskIDLocked() (int64, error) {
	taskKey, err := s.taskKeyManager.generateTaskKey(tasks.CategoryTransfer)
	if err != nil {
		return -1, err
	}
	return taskKey.TaskID, nil
}

func (s *ContextImpl) renewRangeLocked(isStealing bool) error {
	// We must drain all in-flight requests before updating the rangeID.
	// This is because requests are conditioned on rangeID, if rangeID
	// is updated before draining them, those requests could fail.
	// This also means renew rangeID will be the only in-flight request
	// when it's issued, so it doesn't matter if semaphore is acquired or not
	// before calling this method.
	s.taskKeyManager.drainTaskRequests()

	updatedShardInfo := trimShardInfo(s.clusterMetadata.GetAllClusterInfo(), copyShardInfo(s.shardInfo))
	updatedShardInfo.RangeId++
	if isStealing {
		updatedShardInfo.StolenSinceRenew++
	}

	ctx, cancel := s.newIOContext()
	defer cancel()

	previousRangeID := s.getRangeIDLocked()
	err := s.persistenceShardManager.UpdateShard(ctx, &persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: previousRangeID,
	})
	if err != nil {
		// Failure in updating shard to grab new RangeID
		s.contextTaggedLogger.Error("Persistent store operation failure",
			tag.StoreOperationUpdateShard,
			tag.Error(err),
			tag.ShardRangeID(updatedShardInfo.GetRangeId()),
			tag.PreviousShardRangeID(previousRangeID),
		)
		return s.handleWriteErrorLocked(previousRangeID, err)
	}

	// Range is successfully updated in cassandra now update shard context to reflect new range
	s.contextTaggedLogger.Info("Range updated for shardID",
		tag.ShardRangeID(updatedShardInfo.RangeId),
		tag.PreviousShardRangeID(s.shardInfo.RangeId),
	)

	s.shardInfo = trimShardInfo(s.clusterMetadata.GetAllClusterInfo(), copyShardInfo(updatedShardInfo))
	s.taskKeyManager.setRangeID(s.shardInfo.RangeId)

	return nil
}

func (s *ContextImpl) monitorQueueMetrics() {
	timer := time.NewTimer(queueMetricUpdateInterval)
	defer timer.Stop()

	done := s.lifecycleCtx.Done()
	for {
		select {
		case <-done:
			return
		case <-timer.C:
			s.emitShardInfoMetricsLogs()
			// We reset the timer (rather than using a ticker) so that delays in grabbing the shard lock
			// don't cause us to pile up
			timer.Reset(queueMetricUpdateInterval)
		}
	}
}

func (s *ContextImpl) updateShardInfo(
	tasksCompleted int,
	updateFnLocked func(),
) error {
	s.wLock()
	if err := s.errorByState(); err != nil {
		s.wUnlock()
		return err
	}

	s.tasksCompletedSinceLastUpdate += tasksCompleted
	updateFnLocked()
	s.shardInfo.StolenSinceRenew = 0

	now := s.timeSource.Now()
	tooEarly := s.lastUpdated.Add(s.config.ShardUpdateMinInterval()).After(now)
	minTasksUntilUpdate := s.config.ShardUpdateMinTasksCompleted()
	// If ShardUpdateMinTasksCompleted is set to 0 then we only care about whether enough time has passed
	tooFewTasksCompleted := minTasksUntilUpdate <= 0 || s.tasksCompletedSinceLastUpdate < minTasksUntilUpdate
	if tooFewTasksCompleted && tooEarly {
		s.wUnlock()
		return nil
	}

	// update lastUpdate here so that we don't have to grab shard lock again if UpdateShard is successful
	previousLastUpdate := s.lastUpdated
	metrics.TasksCompletedPerShardInfoUpdate.With(s.metricsHandler).Record(int64(s.tasksCompletedSinceLastUpdate))
	metrics.TimeBetweenShardInfoUpdates.With(s.metricsHandler).Record(now.Sub(previousLastUpdate))

	s.lastUpdated = now
	s.tasksCompletedSinceLastUpdate = 0

	updatedShardInfo := trimShardInfo(s.clusterMetadata.GetAllClusterInfo(), copyShardInfo(s.shardInfo))
	request := &persistence.UpdateShardRequest{
		ShardInfo:       updatedShardInfo,
		PreviousRangeID: s.shardInfo.GetRangeId(),
	}
	s.wUnlock()

	if err := s.ioSemaphoreAcquire(s.lifecycleCtx); err != nil {
		return err
	}
	defer s.ioSemaphoreRelease()

	ctx, cancel := s.newIOContext()
	defer cancel()

	err := s.persistenceShardManager.UpdateShard(ctx, request)
	if err != nil {
		s.wLock()
		defer s.wUnlock()
		// revert lastUpdated time so that operation can be retried
		s.lastUpdated = util.MaxTime(previousLastUpdate, s.lastUpdated)
		return s.handleWriteErrorLocked(request.PreviousRangeID, err)
	}

	return nil
}

// Take the shard lock and update queue metrics
func (s *ContextImpl) emitShardInfoMetricsLogs() {
	s.rLock()
	defer s.rUnlock()

	queueStates := trimShardInfo(s.clusterMetadata.GetAllClusterInfo(), copyShardInfo(s.shardInfo)).QueueStates
	emitShardLagLog := s.config.EmitShardLagLog()

	metricsHandler := s.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.ShardInfoScope))

Loop:
	for categoryID, queueState := range queueStates {
		category, ok := s.taskCategoryRegistry.GetCategoryByID(int(categoryID))
		if !ok {
			continue Loop
		}

		switch category.Type() {
		case tasks.CategoryTypeImmediate:
			minTaskKey := getMinTaskKey(queueState)
			if minTaskKey == nil {
				continue Loop
			}
			lag := s.taskKeyManager.getExclusiveReaderHighWatermark(category).TaskID - minTaskKey.TaskID
			if emitShardLagLog && lag > logWarnImmediateTaskLag {
				s.contextTaggedLogger.Warn(
					"Shard queue lag exceeds warn threshold.",
					tag.ShardQueueAcks(category.Name(), minTaskKey.TaskID),
				)
			}
			metrics.ShardInfoImmediateQueueLagHistogram.
				With(metricsHandler).
				Record(lag, metrics.TaskCategoryTag(category.Name()))

		case tasks.CategoryTypeScheduled:
			minTaskKey := getMinTaskKey(queueState)
			if minTaskKey == nil {
				continue Loop
			}
			lag := s.taskKeyManager.getExclusiveReaderHighWatermark(category).FireTime.Sub(minTaskKey.FireTime)
			if emitShardLagLog && lag > logWarnScheduledTaskLag {
				s.contextTaggedLogger.Warn(
					"Shard queue lag exceeds warn threshold.",
					tag.ShardQueueAcks(category.Name(), minTaskKey.FireTime),
				)
			}
			metrics.ShardInfoScheduledQueueLagTimer.With(metricsHandler).
				Record(lag, metrics.TaskCategoryTag(category.Name()))
		default:
			s.contextTaggedLogger.Error("Unknown task category type", tag.NewStringTag("task-category", category.Type().String()))
		}
	}
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

func (s *ContextImpl) getLastUpdatedTime() time.Time {
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
		_ = s.transition(contextRequestStop{reason: stopReasonOwnershipLost})
		return err

	default:
		return err
	}
}

func (s *ContextImpl) handleWriteError(
	requestRangeID int64,
	err error,
) error {
	s.wLock()
	defer s.wUnlock()

	return s.handleWriteErrorLocked(requestRangeID, err)
}

func (s *ContextImpl) handleWriteErrorLocked(
	requestRangeID int64,
	err error,
) error {

	if requestRangeID != s.getRangeIDLocked() {
		return err
	}

	if valid := s.IsValid(); !valid {
		return err
	}
	switch err.(type) {
	case nil:
		// Persistence success: update max read level
		return nil

	case *persistence.AppendHistoryTimeoutError:
		// append history can be blindly retried
		return err

	case *persistence.CurrentWorkflowConditionFailedError,
		*persistence.WorkflowConditionFailedError,
		*persistence.ConditionFailedError,
		*persistence.InvalidPersistenceRequestError,
		*persistence.TransactionSizeLimitError,
		*serviceerror.ResourceExhausted,
		*serviceerror.NotFound,
		*serviceerror.NamespaceNotFound:
		// Persistence failure that means the write was definitely not committed:
		// No special handling required for these errors.
		return err

	case *persistence.ShardOwnershipLostError:
		// Shard is stolen, trigger shutdown of history engine.
		// Handling of max read level doesn't matter here.
		_ = s.transition(contextRequestStop{reason: stopReasonOwnershipLost})
		return err

	default:
		// We have no idea if the write failed or will eventually make it to persistence. Try to re-acquire
		// the shard in the background. If successful, we'll get a new RangeID, to guarantee that subsequent
		// reads will either see that write, or know for certain that it failed. This allows the callers to
		// reliably check the outcome by performing a read. If we fail, we'll shut down the shard.
		// Note that reacquiring the shard will cause the max read level to be updated
		// to the new range (i.e. past newMaxReadLevel).
		_ = s.transition(contextRequestLost{})
		return err
	}
}

func (s *ContextImpl) maybeRecordShardAcquisitionLatency(ownershipChanged bool) {
	if ownershipChanged {
		metrics.ShardContextAcquisitionLatency.With(s.GetMetricsHandler()).
			Record(s.GetCurrentTime(s.GetClusterMetadata().GetCurrentClusterName()).Sub(s.getLastUpdatedTime()),
				metrics.OperationTag(metrics.ShardInfoScope),
			)
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
	_ = s.transition(contextRequestAcquire{})
}

func (s *ContextImpl) UnloadForOwnershipLost() {
	_ = s.transition(contextRequestStop{reason: stopReasonOwnershipLost})
}

// FinishStop should only be called by the controller.
func (s *ContextImpl) FinishStop() {
	// After this returns, engineFuture.Set may not be called anymore, so if we don't get see
	// an Engine here, we won't ever have one.
	_ = s.transition(contextRequestFinishStop{})

	// use a context that we know is cancelled so that this doesn't block
	engine, _ := s.engineFuture.Get(s.lifecycleCtx)

	// Stop the engine if it was running (outside the lock but before returning)
	if engine != nil {
		s.contextTaggedLogger.Info("", tag.LifeCycleStopping, tag.ComponentShardEngine)
		engine.Stop()
		s.contextTaggedLogger.Info("", tag.LifeCycleStopped, tag.ComponentShardEngine)
	}
}

func (s *ContextImpl) IsValid() bool {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.state < contextStateStopping
}

func (s *ContextImpl) stoppedForOwnershipLost() bool {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	return s.state >= contextStateStopping && s.stopReason == stopReasonOwnershipLost
}

func (s *ContextImpl) wLock() {
	handler := s.metricsHandler.WithTags(metrics.OperationTag(metrics.ShardInfoScope))
	metrics.LockRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.LockLatency.With(handler).Record(time.Since(startTime)) }()

	s.rwLock.Lock()
}

func (s *ContextImpl) rLock() {
	handler := s.metricsHandler.WithTags(metrics.OperationTag(metrics.ShardInfoScope))
	metrics.LockRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() { metrics.LockLatency.With(handler).Record(time.Since(startTime)) }()

	s.rwLock.RLock()
}

func (s *ContextImpl) wUnlock() {
	s.rwLock.Unlock()
}

func (s *ContextImpl) rUnlock() {
	s.rwLock.RUnlock()
}

func (s *ContextImpl) ioSemaphoreAcquire(
	ctx context.Context,
) (retErr error) {
	handler := s.metricsHandler.WithTags(metrics.OperationTag(metrics.ShardInfoScope))
	metrics.SemaphoreRequests.With(handler).Record(1)
	startTime := time.Now().UTC()
	defer func() {
		metrics.SemaphoreLatency.With(handler).Record(time.Since(startTime))
		if retErr != nil {
			metrics.SemaphoreFailures.With(handler).Record(1)
		}
	}()

	return s.ioSemaphore.Acquire(ctx, 1)
}

func (s *ContextImpl) ioSemaphoreRelease() {
	s.ioSemaphore.Release(1)
}

func (s *ContextImpl) transition(request contextRequest) error {
	/* State transitions:

	The normal pattern:
		Initialized
			controller calls start()
		Acquiring
			acquireShard gets the shard
		Acquired

	If we get a transient error from persistence:
		Acquired
			transient error: handleErrorLocked calls transition(contextRequestLost)
		Acquiring
			acquireShard gets the shard
		Acquired

	If we get shard ownership lost:
		Acquired
			ShardOwnershipLostError: handleErrorLocked calls transition(contextRequestStop)
		Stopping
			controller removes from map and calls FinishStop()
		Stopped

	Stopping can be triggered internally (if we get a ShardOwnershipLostError, or fail to acquire the rangeid
	lock after several minutes) or externally (from controller, e.g. controller shutting down or admin force-
	unload shard). If it's triggered internally, we transition to Stopping, then make an asynchronous callback
	to controller, which will remove us from the map and call FinishStop(), which will transition to Stopped and
	stop the engine. If it's triggered externally, we'll skip over Stopping and go straight to Stopped.

	If we transition externally to Stopped, and the acquireShard goroutine is still running, we can't kill it,
	but we should make sure that it can't do anything: the context it uses for persistence ops will be
	canceled, and if it tries to transition states, it will fail.

	Invariants:
	- Once state is Stopping, it can only go to Stopped.
	- Once state is Stopped, it can't go anywhere else.
	- At the start of acquireShard, state must be Acquiring.
	- By the end of acquireShard, state must not be Acquiring: either acquireShard set it to Acquired, or the
	  controller set it to Stopped.
	- If state is Acquiring, acquireShard should be running in the background.
	- Only acquireShard can use contextRequestAcquired (i.e. transition from Acquiring to Acquired).
	- Once state has reached Acquired at least once, and not reached Stopped, engineFuture must be set.
	- Only the controller may call start() and FinishStop().
	- The controller must call FinishStop() for every ContextImpl it creates.

	*/

	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	setStateAcquiring := func() {
		s.state = contextStateAcquiring
		s.contextTaggedLogger.Info("", tag.LifeCycleStarted, tag.ComponentShardContext)
		go s.acquireShard()
	}

	setStateStopping := func(request contextRequestStop) {
		s.state = contextStateStopping
		s.stopReason = request.reason
		s.contextTaggedLogger.Info("", tag.LifeCycleStopping, tag.ComponentShardContext)
		// Cancel lifecycle context as soon as we know we're shutting down
		s.lifecycleCancel()
		// This will cause the controller to remove this shard from the map and then call s.FinishStop()
		if s.closeCallback != nil {
			go s.closeCallback(s)
		}
	}

	setStateStopped := func() {
		s.state = contextStateStopped
		s.contextTaggedLogger.Info("", tag.LifeCycleStopped, tag.ComponentShardContext)
		// Do this again in case we skipped the stopping state, which could happen
		// when calling CloseShardByID or the controller is shutting down.
		s.lifecycleCancel()
	}

	switch s.state {
	case contextStateInitialized:
		switch request := request.(type) {
		case contextRequestAcquire:
			setStateAcquiring()
			return nil
		case contextRequestStop:
			setStateStopping(request)
			return nil
		case contextRequestFinishStop:
			setStateStopped()
			return nil
		}
	case contextStateAcquiring:
		switch request := request.(type) {
		case contextRequestAcquire:
			return nil // nothing to do, already acquiring
		case contextRequestAcquired:
			s.state = contextStateAcquired
			if request.engine != nil {
				// engineFuture.Set should only be called inside stateLock when state is
				// Acquiring, so that other code (i.e. FinishStop) can know that after a state
				// transition to Stopping/Stopped, engineFuture cannot be Set.
				if s.engineFuture.Ready() {
					// defensive check, this should never happen
					s.contextTaggedLogger.Warn("transition to acquired with engine set twice")
					return errInvalidTransition
				}
				s.engineFuture.Set(request.engine, nil)
			}
			if !s.engineFuture.Ready() {
				// we should either have an engine from a previous transition, or set one now
				s.contextTaggedLogger.Warn("transition to acquired but no engine set")
				return errInvalidTransition
			}

			return nil
		case contextRequestLost:
			return nil // nothing to do, already acquiring
		case contextRequestStop:
			setStateStopping(request)
			return nil
		case contextRequestFinishStop:
			setStateStopped()
			return nil
		}
	case contextStateAcquired:
		switch request := request.(type) {
		case contextRequestAcquire:
			return nil // nothing to to do, already acquired
		case contextRequestLost:
			setStateAcquiring()
			return nil
		case contextRequestStop:
			setStateStopping(request)
			return nil
		case contextRequestFinishStop:
			setStateStopped()
			return nil
		}
	case contextStateStopping:
		switch request.(type) {
		case contextRequestStop:
			// nothing to do, already stopping
			return nil
		case contextRequestFinishStop:
			setStateStopped()
			return nil
		}
	case contextStateStopped:
		switch request.(type) {
		case contextRequestStop, contextRequestFinishStop:
			// nothing to do, already stopped
			return nil
		}
	}
	s.contextTaggedLogger.Warn("invalid state transition request",
		tag.ShardContextState(int(s.state)),
		tag.ShardContextStateRequest(fmt.Sprintf("%T", request)),
	)
	return errInvalidTransition
}

// notifyQueueProcessor sends notification to all queue processors for triggering a load
// NOTE: this method assumes engineFuture is already in a ready state.
func (s *ContextImpl) notifyQueueProcessor() {
	// use a cancelled ctx so the method won't be blocked if engineFuture is not ready
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// we will get the engine when the Future is ready
	engine, err := s.engineFuture.Get(cancelledCtx)
	if err != nil {
		s.contextTaggedLogger.Warn("tried to notify queue processor when engine is not ready")
		return
	}

	now := s.timeSource.Now()
	fakeTasks := make(map[tasks.Category][]tasks.Task)
	for _, category := range s.taskCategoryRegistry.GetCategories() {
		fakeTasks[category] = []tasks.Task{tasks.NewFakeTask(definition.WorkflowKey{}, category, now)}
	}

	engine.NotifyNewTasks(fakeTasks)
}

func (s *ContextImpl) updateHandoverNamespacePendingTaskID() {
	s.wLock()

	if s.errorByState() != nil {
		// if not in acquired state, this function will be called again
		// later when shard is re-acquired.
		s.wUnlock()
		return
	}

	maxReplicationTaskID := s.taskKeyManager.getExclusiveReaderHighWatermark(tasks.CategoryReplication).TaskID - 1
	for namespaceName, handoverInfo := range s.handoverNamespaces {
		if handoverInfo.MaxReplicationTaskID == pendingMaxReplicationTaskID {
			s.handoverNamespaces[namespaceName].MaxReplicationTaskID = maxReplicationTaskID
		}
	}
	s.wUnlock()

	s.notifyReplicationQueueProcessor(maxReplicationTaskID)
}

func (s *ContextImpl) notifyReplicationQueueProcessor(taskID int64) {
	// Replication ack level won't exceed the max taskID it received via task notification.
	// Since here we want it's ack level to advance to at least the input taskID, we need to
	// trigger an fake notification.

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	engine, err := s.engineFuture.Get(cancelledCtx)
	if err != nil {
		s.contextTaggedLogger.Warn("tried to notify replication queue processor when engine is not ready")
		return
	}

	fakeReplicationTask := tasks.NewFakeTask(definition.WorkflowKey{}, tasks.CategoryReplication, tasks.MinimumKey.FireTime)
	fakeReplicationTask.SetTaskID(taskID)

	engine.NotifyNewTasks(map[tasks.Category][]tasks.Task{
		tasks.CategoryReplication: {fakeReplicationTask},
	})
}

func (s *ContextImpl) loadShardMetadata(ownershipChanged *bool) error {
	// Only have to do this once, we can just re-acquire the rangeid lock after that
	s.rLock()
	if s.shardInfo != nil {
		s.rUnlock()
		return nil
	}
	s.rUnlock()

	// We don't have any shardInfo yet, load it (outside of context rwlock)
	ctx, cancel := s.newIOContext()
	defer cancel()
	resp, err := s.persistenceShardManager.GetOrCreateShard(ctx, &persistence.GetOrCreateShardRequest{
		ShardID:          s.shardID,
		LifecycleContext: s.lifecycleCtx,
	})
	if err != nil {
		s.contextTaggedLogger.Error("Failed to load shard", tag.Error(err))
		return err
	}
	*ownershipChanged = resp.ShardInfo.Owner != s.owner
	shardInfo := trimShardInfo(s.clusterMetadata.GetAllClusterInfo(), copyShardInfo(resp.ShardInfo))
	shardInfo.Owner = s.owner

	// initialize the cluster current time to be the same as ack level
	remoteClusterInfos := make(map[string]*remoteClusterInfo)
	var taskMinScheduledTime time.Time
	currentClusterName := s.GetClusterMetadata().GetCurrentClusterName()
	taskCategories := s.taskCategoryRegistry.GetCategories()
	for clusterName, info := range s.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		exclusiveMaxReadTime := tasks.DefaultFireTime
		for categoryID, queueState := range shardInfo.QueueStates {
			category, ok := taskCategories[int(categoryID)]
			if !ok || category.Type() != tasks.CategoryTypeScheduled {
				continue
			}

			exclusiveMaxReadTime = util.MaxTime(exclusiveMaxReadTime, timestamp.TimeValue(queueState.ExclusiveReaderHighWatermark.FireTime))
		}

		// we only need to make sure min scheduled time >= persisted ack level/exclusiveReaderHighWatermark
		// Add().Truncate() here is just to make sure min scheduled time has the same precision as the old logic
		// in case existing code can't work correctly with precision higher than 1ms.
		// Once we validate the rest of the code can worker correctly with higher precision, the code should simply be
		// taskMinScheduledTime = util.MaxTime(taskMinScheduledTime, maxReadTime)
		taskMinScheduledTime = util.MaxTime(
			taskMinScheduledTime,
			exclusiveMaxReadTime.Add(persistence.ScheduledTaskMinPrecision).Truncate(persistence.ScheduledTaskMinPrecision),
		)

		if clusterName != currentClusterName {
			remoteClusterInfos[clusterName] = &remoteClusterInfo{
				CurrentTime:                exclusiveMaxReadTime,
				AckedReplicationTaskIDs:    make(map[int32]int64),
				AckedReplicationTimestamps: make(map[int32]time.Time),
			}
		}
	}

	s.wLock()
	defer s.wUnlock()

	s.shardInfo = shardInfo
	s.remoteClusterInfos = remoteClusterInfos
	s.taskKeyManager.setTaskMinScheduledTime(taskMinScheduledTime)

	return nil
}

func (s *ContextImpl) GetReplicationStatus(clusterNames []string) (map[string]*historyservice.ShardReplicationStatusPerCluster, map[string]*historyservice.HandoverNamespaceInfo, error) {
	remoteClusters := make(map[string]*historyservice.ShardReplicationStatusPerCluster)
	handoverNamespaces := make(map[string]*historyservice.HandoverNamespaceInfo)
	clusterInfo := s.clusterMetadata.GetAllClusterInfo()
	s.rLock()
	defer s.rUnlock()

	if len(clusterNames) == 0 {
		for clusterName := range clusterInfo {
			clusterNames = append(clusterNames, clusterName)
		}
	}

	for _, clusterName := range clusterNames {
		if _, ok := clusterInfo[clusterName]; !ok {
			continue
		}
		v, ok := s.remoteClusterInfos[clusterName]
		if !ok {
			continue
		}

		for _, remoteShardID := range common.MapShardID(
			clusterInfo[s.clusterMetadata.GetCurrentClusterName()].ShardCount,
			clusterInfo[clusterName].ShardCount,
			s.shardID,
		) {
			ackTaskID := v.AckedReplicationTaskIDs[remoteShardID] // default to 0
			ackTimestamp := v.AckedReplicationTimestamps[remoteShardID]
			if ackTimestamp.IsZero() {
				ackTimestamp = time.Unix(0, 0)
			}
			if record, ok := remoteClusters[clusterName]; !ok {
				remoteClusters[clusterName] = &historyservice.ShardReplicationStatusPerCluster{
					AckedTaskId:             ackTaskID,
					AckedTaskVisibilityTime: timestamppb.New(ackTimestamp),
				}
			} else if record.AckedTaskId > ackTaskID {
				remoteClusters[clusterName] = &historyservice.ShardReplicationStatusPerCluster{
					AckedTaskId:             ackTaskID,
					AckedTaskVisibilityTime: timestamppb.New(ackTimestamp),
				}
			}
		}
	}

	for k, v := range s.handoverNamespaces {
		handoverNamespaces[k.String()] = &historyservice.HandoverNamespaceInfo{
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
		AckedReplicationTaskIDs:    make(map[int32]int64),
		AckedReplicationTimestamps: make(map[int32]time.Time),
	}
	s.remoteClusterInfos[clusterName] = info
	return info
}

func (s *ContextImpl) acquireShard() {
	// This is called in two contexts: initially acquiring the rangeid lock, and trying to
	// re-acquire it after a persistence error. In both cases, we retry the acquire operation
	// (renewRangeLocked) for 5 minutes. Each individual attempt uses shardIOTimeout (5s) as
	// the timeout. This lets us handle a few minutes of persistence unavailability without
	// dropping and reloading the whole shard context, which is relatively expensive (includes
	// caches that would have to be refilled, etc.).
	//
	// We stop retrying on any of:
	// 1. We succeed in acquiring the rangeid lock.
	// 2. We get ShardOwnershipLostError or lifecycleCtx ended.
	// 3. The state changes to Stopping or Stopped.
	//
	// If the shard controller sees that service resolver has assigned ownership to someone
	// else, it will call FinishStop, which will trigger case 3 above, and also cancel
	// lifecycleCtx. The persistence operations called here use lifecycleCtx as their context,
	// so if we were blocked in any of them, they should return immediately with a context
	// canceled error.
	policy := s.acquireShardRetryPolicy
	if policy == nil {
		policy = backoff.NewExponentialRetryPolicy(1 * time.Second).WithExpirationInterval(5 * time.Minute)
	}

	// Remember this value across attempts
	ownershipChanged := false

	op := func() error {
		if !s.IsValid() {
			return s.newShardClosedErrorWithShardID()
		}

		// Initial load of shard metadata
		err := s.loadShardMetadata(&ownershipChanged)
		if err != nil {
			return err
		}

		// Try to acquire RangeID lock. If this gets a persistence error, it may call:
		// transition(contextRequestStop) for ShardOwnershipLostError:
		//   This will transition to Stopping right here, and the transition call at the end of the
		//   outer function will do nothing, since the state was already changed.
		// transition(contextRequestLost) for other transient errors:
		//   This will do nothing, since state is already Acquiring.
		//
		// We don't need to acquire the semaphore here, because renewRangeLocked will drain
		// in-flight requests before making the call. So it's guaranteed that the renew rangeID
		// UpdateShard call is the only one in flight.
		s.wLock()
		err = s.renewRangeLocked(true)
		s.wUnlock()
		if err != nil {
			return err
		}

		s.contextTaggedLogger.Info("Acquired shard")

		// The first time we get the shard, we have to create the engine
		var engine Engine
		if !s.engineFuture.Ready() {
			s.maybeRecordShardAcquisitionLatency(ownershipChanged)
			engine = s.createEngine()
		}

		// NOTE: engine is created & started before setting shard state to acquired.
		// -> namespace handover callback is registered & called before shard is able to serve traffic
		// -> information for handover namespace is recorded before shard can servce traffic
		// -> upon shard reload, no history api or task can go through for ns in handover state
		err = s.transition(contextRequestAcquired{engine: engine})

		if err != nil {
			if engine != nil {
				// We tried to set the engine but the context was already stopped
				engine.Stop()
			}
			return err
		}

		// we know engineFuture must be ready here, and we can notify queue processor
		// to trigger a load as queue max level can be updated to a newer value
		s.notifyQueueProcessor()
		// This runs until the lifecycleCtx is cancelled, so we only need to start it once
		s.queueMetricEmitter.Do(func() {
			go s.monitorQueueMetrics()
		})

		s.updateHandoverNamespacePendingTaskID()

		return nil
	}

	// keep retrying except ShardOwnershipLostError or lifecycle context ended
	acquireShardRetryable := func(err error) (isRetryable bool) {
		defer func() {
			s.contextTaggedLogger.Error(
				"Error acquiring shard",
				tag.Error(err),
				tag.IsRetryable(isRetryable),
			)
		}()
		if s.lifecycleCtx.Err() != nil {
			return false
		}
		switch err.(type) {
		case *persistence.ShardOwnershipLostError:
			return false
		}
		return true
	}
	err := backoff.ThrottleRetry(op, policy, acquireShardRetryable)
	if err != nil {
		// We got an non-retryable error, e.g. ShardOwnershipLostError
		s.contextTaggedLogger.Error("Couldn't acquire shard", tag.Error(err))

		reason := stopReasonUnspecified
		if IsShardOwnershipLostError(err) {
			reason = stopReasonOwnershipLost
		}
		// On any error, initiate shutting down the shard. If we already changed state
		// because we got a ShardOwnershipLostError, this won't do anything.
		_ = s.transition(contextRequestStop{reason: reason})
	}
}

func newContext(
	shardID int32,
	factory EngineFactory,
	historyConfig *configs.Config,
	persistenceConfig config.Persistence,
	closeCallback CloseCallback,
	logger log.Logger,
	throttledLogger log.Logger,
	persistenceExecutionManager persistence.ExecutionManager,
	persistenceShardManager persistence.ShardManager,
	clientBean client.Bean,
	historyClient historyservice.HistoryServiceClient,
	metricsHandler metrics.Handler,
	payloadSerializer serialization.Serializer,
	timeSource cclock.TimeSource,
	namespaceRegistry namespace.Registry,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	hostInfoProvider membership.HostInfoProvider,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	eventsCache events.Cache,
	stateMachineRegistry *hsm.Registry,
) (*ContextImpl, error) {
	hostIdentity := hostInfoProvider.HostInfo().Identity()
	sequenceID := atomic.AddInt64(&shardContextSequenceID, 1)

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())

	ioConcurrency := historyConfig.ShardIOConcurrency()
	if ioConcurrency != 1 && persistenceConfig.DataStores[persistenceConfig.DefaultStore].Cassandra != nil {
		throttledLogger.Warn(
			fmt.Sprintf("Cassandra persistence implementation only supports %v == 1", dynamicconfig.ShardIOConcurrency),
			tag.NewInt("shard-io-concurrency", ioConcurrency),
		)
		ioConcurrency = 1
	}

	shardContext := &ContextImpl{
		state:                   contextStateInitialized,
		shardID:                 shardID,
		owner:                   fmt.Sprintf("%s-%v-%v", hostIdentity, sequenceID, uuid.New()),
		stringRepr:              fmt.Sprintf("Shard(%d)", shardID),
		executionManager:        persistenceExecutionManager,
		metricsHandler:          metricsHandler,
		closeCallback:           closeCallback,
		config:                  historyConfig,
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
		saMapperProvider:        saMapperProvider,
		clusterMetadata:         clusterMetadata,
		archivalMetadata:        archivalMetadata,
		hostInfoProvider:        hostInfoProvider,
		taskCategoryRegistry:    taskCategoryRegistry,
		handoverNamespaces:      make(map[namespace.Name]*namespaceHandOverInfo),
		lifecycleCtx:            lifecycleCtx,
		lifecycleCancel:         lifecycleCancel,
		engineFuture:            future.NewFuture[Engine](),
		queueMetricEmitter:      sync.Once{},
		ioSemaphore:             semaphore.NewWeighted(int64(ioConcurrency)),
		stateMachineRegistry:    stateMachineRegistry,
	}
	shardContext.taskKeyManager = newTaskKeyManager(
		shardContext.taskCategoryRegistry,
		timeSource,
		historyConfig,
		shardContext.GetLogger(),
		func() error {
			return shardContext.renewRangeLocked(false)
		},
	)
	if shardContext.GetConfig().EnableHostLevelEventsCache() {
		shardContext.eventsCache = eventsCache
	} else {
		shardContext.eventsCache = events.NewShardLevelEventsCache(
			shardContext.executionManager,
			shardContext.config,
			shardContext.metricsHandler,
			shardContext.contextTaggedLogger,
			false,
		)
	}
	return shardContext, nil
}

// TODO: why do we need a deep copy here?
func copyShardInfo(shardInfo *persistencespb.ShardInfo) *persistencespb.ShardInfo {
	// need to ser/de to make a deep copy of queue state
	queueStates := make(map[int32]*persistencespb.QueueState, len(shardInfo.QueueStates))
	for k, v := range shardInfo.QueueStates {
		blob, _ := serialization.QueueStateToBlob(v)
		queueState, _ := serialization.QueueStateFromBlob(blob.Data, blob.EncodingType.String())
		queueStates[k] = queueState
	}

	return &persistencespb.ShardInfo{
		ShardId:                shardInfo.ShardId,
		Owner:                  shardInfo.Owner,
		RangeId:                shardInfo.RangeId,
		StolenSinceRenew:       shardInfo.StolenSinceRenew,
		ReplicationDlqAckLevel: maps.Clone(shardInfo.ReplicationDlqAckLevel),
		UpdateTime:             shardInfo.UpdateTime,
		QueueStates:            queueStates,
	}
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

func (s *ContextImpl) GetMetricsHandler() metrics.Handler {
	return s.metricsHandler
}

func (s *ContextImpl) GetTimeSource() cclock.TimeSource {
	return s.timeSource
}

func (s *ContextImpl) GetNamespaceRegistry() namespace.Registry {
	return s.namespaceRegistry
}

func (s *ContextImpl) GetSearchAttributesProvider() searchattribute.Provider {
	return s.saProvider
}

func (s *ContextImpl) GetSearchAttributesMapperProvider() searchattribute.MapperProvider {
	return s.saMapperProvider
}

func (s *ContextImpl) GetClusterMetadata() cluster.Metadata {
	return s.clusterMetadata
}

func (s *ContextImpl) GetArchivalMetadata() archiver.ArchivalMetadata {
	return s.archivalMetadata
}

func (s *ContextImpl) StateMachineRegistry() *hsm.Registry {
	return s.stateMachineRegistry
}

// newDetachedContext creates a detached context with the same deadline
// and values from the given context. Detached context won't be affected
// if the context it bases on is cancelled.
// Use this to perform operations that should not be interrupted by caller
// cancellation. E.g. workflow operations that, if interrupted, could result in
// shard renewal or even ownership lost.
func (s *ContextImpl) newDetachedContext(
	ctx context.Context,
) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	detachedContext := rpc.CopyContextValues(s.lifecycleCtx, ctx)

	var cancel context.CancelFunc
	deadline, ok := ctx.Deadline()
	if ok {
		timeout := deadline.Sub(s.GetTimeSource().Now())
		if timeout < minContextTimeout {
			timeout = minContextTimeout
		}
		detachedContext, cancel = context.WithTimeout(detachedContext, timeout)
	} else {
		cancel = func() {}
	}

	return detachedContext, cancel, nil
}

func (s *ContextImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(s.lifecycleCtx, shardIOTimeout)
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)

	return ctx, cancel
}

// newShardClosedErrorWithShardID when shard is closed and a req cannot be processed
func (s *ContextImpl) newShardClosedErrorWithShardID() *persistence.ShardOwnershipLostError {
	return &persistence.ShardOwnershipLostError{
		ShardID: s.shardID, // immutable
		Msg:     "shard closed",
	}
}

func OperationPossiblySucceeded(err error) bool {
	switch err.(type) {
	case *persistence.CurrentWorkflowConditionFailedError,
		*persistence.WorkflowConditionFailedError,
		*persistence.ConditionFailedError,
		*persistence.ShardOwnershipLostError,
		*persistence.InvalidPersistenceRequestError,
		*persistence.TransactionSizeLimitError,
		*persistence.AppendHistoryTimeoutError, // this means task operations is not started
		*serviceerror.ResourceExhausted,
		*serviceerror.NotFound,
		*serviceerror.NamespaceNotFound:
		// Persistence failure that means that write was definitely not committed.
		return false
	default:
		return true
	}
}

func trimShardInfo(
	allClusterInfo map[string]cluster.ClusterInformation,
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	if shardInfo.QueueStates != nil && shardInfo.QueueStates[int32(tasks.CategoryIDReplication)] != nil {
		for readerID := range shardInfo.QueueStates[int32(tasks.CategoryIDReplication)].ReaderStates {
			clusterID, _ := ReplicationReaderIDToClusterShardID(readerID)
			_, clusterInfo, found := clusterNameInfoFromClusterID(allClusterInfo, clusterID)
			if !found || !clusterInfo.Enabled {
				delete(shardInfo.QueueStates[int32(tasks.CategoryIDReplication)].ReaderStates, readerID)
			}
		}
		if len(shardInfo.QueueStates[int32(tasks.CategoryIDReplication)].ReaderStates) == 0 {
			delete(shardInfo.QueueStates, int32(tasks.CategoryIDReplication))
		}
	}
	return shardInfo
}

func clusterNameInfoFromClusterID(
	allClusterInfo map[string]cluster.ClusterInformation,
	clusterID int64,
) (string, cluster.ClusterInformation, bool) {
	for name, info := range allClusterInfo {
		if info.InitialFailoverVersion == clusterID {
			return name, info, true
		}
	}
	return "", cluster.ClusterInformation{}, false
}
