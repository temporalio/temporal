package replication

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	dlqSizeCheckInterval = time.Minute * 5
)

type (
	// taskProcessorManagerImpl is to manage replication task processors
	taskProcessorManagerImpl struct {
		config                        *configs.Config
		deleteMgr                     deletemanager.DeleteManager
		engine                        historyi.Engine
		eventSerializer               serialization.Serializer
		shard                         historyi.ShardContext
		status                        int32
		replicationTaskFetcherFactory TaskFetcherFactory
		workflowCache                 wcache.Cache
		removeHistoryFetcher          eventhandler.HistoryPaginatedFetcher
		taskExecutorProvider          TaskExecutorProvider
		taskPollerManager             pollerManager
		metricsHandler                metrics.Handler
		logger                        log.Logger
		dlqWriter                     DLQWriter

		enableFetcher     bool
		taskProcessorLock sync.RWMutex
		taskProcessors    map[string][]TaskProcessor // cluster name - processor
		minTxAckedTaskID  int64
		shutdownChan      chan struct{}
	}
)

func NewTaskProcessorManager(
	config *configs.Config,
	shardContext historyi.ShardContext,
	engine historyi.Engine,
	workflowCache wcache.Cache,
	workflowDeleteManager deletemanager.DeleteManager,
	clientBean client.Bean,
	eventSerializer serialization.Serializer,
	replicationTaskFetcherFactory TaskFetcherFactory,
	taskExecutorProvider TaskExecutorProvider,
	dlqWriter DLQWriter,
) *taskProcessorManagerImpl {
	historyFetcher := eventhandler.NewHistoryPaginatedFetcher(shardContext.GetNamespaceRegistry(), clientBean, eventSerializer, shardContext.GetLogger())
	return &taskProcessorManagerImpl{
		config:                        config,
		deleteMgr:                     workflowDeleteManager,
		engine:                        engine,
		eventSerializer:               eventSerializer,
		shard:                         shardContext,
		status:                        common.DaemonStatusInitialized,
		replicationTaskFetcherFactory: replicationTaskFetcherFactory,
		workflowCache:                 workflowCache,
		removeHistoryFetcher:          historyFetcher,
		logger:                        shardContext.GetLogger(),
		metricsHandler:                shardContext.GetMetricsHandler(),
		dlqWriter:                     dlqWriter,

		enableFetcher:        !config.EnableReplicationStream(),
		taskProcessors:       make(map[string][]TaskProcessor),
		taskExecutorProvider: taskExecutorProvider,
		taskPollerManager:    newPollerManager(shardContext.GetShardID(), shardContext.GetClusterMetadata()),
		minTxAckedTaskID:     persistence.EmptyQueueMessageID,
		shutdownChan:         make(chan struct{}),
	}
}

func (r *taskProcessorManagerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	// Listen to cluster metadata and dynamically update replication processor for remote clusters.
	if r.enableFetcher {
		r.listenToClusterMetadataChange()
	}
	go r.completeReplicationTaskLoop()
	go r.checkReplicationDLQEmptyLoop()
}

func (r *taskProcessorManagerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(r.shutdownChan)

	if r.enableFetcher {
		r.shard.GetClusterMetadata().UnRegisterMetadataChangeCallback(r)
	}
	r.taskProcessorLock.Lock()
	for _, taskProcessors := range r.taskProcessors {
		for _, processor := range taskProcessors {
			processor.Stop()
		}
	}
	r.taskProcessorLock.Unlock()
}

func (r *taskProcessorManagerImpl) listenToClusterMetadataChange() {
	clusterMetadata := r.shard.GetClusterMetadata()
	clusterMetadata.RegisterMetadataChangeCallback(
		r,
		r.handleClusterMetadataUpdate,
	)
}

func (r *taskProcessorManagerImpl) handleClusterMetadataUpdate(
	oldClusterMetadata map[string]*cluster.ClusterInformation,
	newClusterMetadata map[string]*cluster.ClusterInformation,
) {
	r.taskProcessorLock.Lock()
	defer r.taskProcessorLock.Unlock()
	currentClusterName := r.shard.GetClusterMetadata().GetCurrentClusterName()
	// The metadata triggers an update when the following fields update: 1. Enabled 2. Initial Failover Version 3. Cluster address
	// The callback covers three cases:
	// Case 1: Remove a cluster Case 2: Add a new cluster Case 3: Refresh cluster metadata(1 + 2).

	// Case 1 and Case 3
	for clusterName := range oldClusterMetadata {
		if clusterName == currentClusterName {
			continue
		}
		for _, processor := range r.taskProcessors[clusterName] {
			processor.Stop()
		}
		delete(r.taskProcessors, clusterName)
	}

	// Case 2 and Case 3
	for clusterName := range newClusterMetadata {
		if clusterName == currentClusterName {
			continue
		}
		if clusterInfo := newClusterMetadata[clusterName]; clusterInfo == nil || !clusterInfo.Enabled {
			continue
		}
		sourceShardIds, err := r.taskPollerManager.getSourceClusterShardIDs(clusterName)
		if err != nil {
			r.logger.Error("Failed to get source shard id list", tag.Error(err), tag.ClusterName(clusterName))
			continue
		}
		var processors []TaskProcessor
		for _, sourceShardId := range sourceShardIds {
			fetcher := r.replicationTaskFetcherFactory.GetOrCreateFetcher(clusterName)
			replicationTaskProcessor := NewTaskProcessor(
				sourceShardId,
				r.shard,
				r.engine,
				r.config,
				r.shard.GetMetricsHandler(),
				fetcher,
				r.taskExecutorProvider(TaskExecutorParams{
					RemoteCluster:        clusterName,
					Shard:                r.shard,
					RemoteHistoryFetcher: r.removeHistoryFetcher,
					DeleteManager:        r.deleteMgr,
					WorkflowCache:        r.workflowCache,
				}),
				r.eventSerializer,
				r.dlqWriter,
			)
			replicationTaskProcessor.Start()
			processors = append(processors, replicationTaskProcessor)
		}
		r.taskProcessors[clusterName] = processors
	}
}

func (r *taskProcessorManagerImpl) completeReplicationTaskLoop() {
	shardID := r.shard.GetShardID()
	cleanupTimer := time.NewTimer(backoff.Jitter(
		r.config.ReplicationTaskProcessorCleanupInterval(shardID),
		r.config.ReplicationTaskProcessorCleanupJitterCoefficient(shardID),
	))
	defer cleanupTimer.Stop()
	for {
		select {
		case <-cleanupTimer.C:
			if err := r.cleanupReplicationTasks(); err != nil {
				r.logger.Error("Failed to clean up replication messages.", tag.Error(err))
				metrics.ReplicationTaskCleanupFailure.With(r.metricsHandler).Record(
					1,
					metrics.OperationTag(metrics.ReplicationTaskCleanupScope),
				)
			}
			cleanupTimer.Reset(backoff.Jitter(
				r.config.ReplicationTaskProcessorCleanupInterval(shardID),
				r.config.ReplicationTaskProcessorCleanupJitterCoefficient(shardID),
			))
		case <-r.shutdownChan:
			return
		}
	}
}

func (r *taskProcessorManagerImpl) checkReplicationDLQEmptyLoop() {
	for {
		timer := time.NewTimer(backoff.FullJitter(dlqSizeCheckInterval))
		select {
		case <-timer.C:
			if r.config.ReplicationEnableDLQMetrics() {
				r.checkReplicationDLQSize()
			}
		case <-r.shutdownChan:
			timer.Stop()
			return
		}
	}
}

func (r *taskProcessorManagerImpl) cleanupReplicationTasks() error {
	clusterMetadata := r.shard.GetClusterMetadata()
	allClusterInfo := clusterMetadata.GetAllClusterInfo()
	currentClusterName := clusterMetadata.GetCurrentClusterName()

	minAckedTaskID := r.shard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID - 1
	queueStates, ok := r.shard.GetQueueState(tasks.CategoryReplication)
	if !ok {
		queueStates = &persistencespb.QueueState{
			ExclusiveReaderHighWatermark: nil,
			ReaderStates:                 make(map[int64]*persistencespb.QueueReaderState),
		}
	}
	for _, readerID := range targetReaderIDs(
		currentClusterName,
		r.shard.GetShardID(),
		allClusterInfo,
	) {
		readerState, ok := queueStates.ReaderStates[readerID]
		if !ok {
			minAckedTaskID = min(minAckedTaskID, 0)
		} else {
			minAckedTaskID = min(minAckedTaskID, readerState.Scopes[0].Range.InclusiveMin.TaskId-1)
		}
	}

	if minAckedTaskID <= r.minTxAckedTaskID {
		return nil
	}

	r.logger.Debug("cleaning up replication task queue", tag.ReadLevel(minAckedTaskID))
	metrics.ReplicationTaskCleanupCount.With(r.metricsHandler).Record(
		1,
		metrics.OperationTag(metrics.ReplicationTaskCleanupScope),
	)
	metrics.ReplicationTasksLag.With(r.metricsHandler).Record(
		r.shard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID-minAckedTaskID,
		metrics.TargetClusterTag(currentClusterName),
		metrics.OperationTag(metrics.ReplicationTaskCleanupScope),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	ctx = headers.SetCallerInfo(ctx, headers.SystemPreemptableCallerInfo)
	defer cancel()

	err := r.shard.GetExecutionManager().RangeCompleteHistoryTasks(
		ctx,
		&persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             r.shard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(minAckedTaskID + 1),
		},
	)
	if err == nil {
		r.minTxAckedTaskID = minAckedTaskID
	}
	return err
}

func (r *taskProcessorManagerImpl) checkReplicationDLQSize() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	ctx = headers.SetCallerInfo(ctx, headers.SystemPreemptableCallerInfo)
	defer cancel()
	for clusterName := range r.shard.GetClusterMetadata().GetAllClusterInfo() {
		if clusterName == r.shard.GetClusterMetadata().GetCurrentClusterName() {
			continue
		}

		minTaskKey := r.shard.GetReplicatorDLQAckLevel(clusterName)
		isEmpty, err := r.shard.GetExecutionManager().IsReplicationDLQEmpty(ctx, &persistence.GetReplicationTasksFromDLQRequest{
			GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
				ShardID:             r.shard.GetShardID(),
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskKey),
			},
			SourceClusterName: clusterName,
		})
		if err != nil {
			r.logger.Error("Failed to check replication DLQ size.", tag.Error(err))
			return
		}
		if !isEmpty {
			metrics.ReplicationNonEmptyDLQCount.With(r.metricsHandler).Record(1, metrics.OperationTag(metrics.ReplicationDLQStatsScope))
			break
		}
	}
}

func targetReaderIDs(
	currentClusterName string,
	currentShardID int32,
	allClusterInfo map[string]cluster.ClusterInformation,
) []int64 {
	currentShardCount := allClusterInfo[currentClusterName].ShardCount
	var readerIDs []int64
	for clusterName, clusterInfo := range allClusterInfo {
		if clusterName == currentClusterName || !clusterInfo.Enabled {
			continue
		}

		targetClusterID := allClusterInfo[clusterName].InitialFailoverVersion
		targetShardCount := allClusterInfo[clusterName].ShardCount
		for _, targetShardID := range common.MapShardID(
			currentShardCount,
			targetShardCount,
			currentShardID,
		) {
			readerIDs = append(readerIDs, shard.ReplicationReaderIDFromClusterShardID(
				targetClusterID, targetShardID,
			))
		}
	}
	return readerIDs
}
