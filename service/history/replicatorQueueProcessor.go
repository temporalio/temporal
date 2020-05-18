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
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/temporal-proto/common"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"

	commongenpb "github.com/temporalio/temporal/.gen/proto/common"
	eventgenpb "github.com/temporalio/temporal/.gen/proto/event"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/convert"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
)

type (
	replicatorQueueProcessorImpl struct {
		currentClusterName    string
		shard                 ShardContext
		historyCache          *historyCache
		replicationTaskFilter taskFilter
		executionMgr          persistence.ExecutionManager
		historyV2Mgr          persistence.HistoryManager
		replicator            messaging.Producer
		metricsClient         metrics.Client
		options               *QueueProcessorOptions
		logger                log.Logger
		retryPolicy           backoff.RetryPolicy
		// This is the batch size used by pull based RPC replicator.
		fetchTasksBatchSize int
		*queueProcessorBase
		queueAckMgr

		lastShardSyncTimestamp time.Time
	}
)

var (
	errUnknownReplicationTask = errors.New("unknown replication task")
	errHistoryNotFoundTask    = errors.New("history not found")
	defaultHistoryPageSize    = 1000
)

func newReplicatorQueueProcessor(
	shard ShardContext,
	historyCache *historyCache,
	replicator messaging.Producer,
	executionMgr persistence.ExecutionManager,
	historyV2Mgr persistence.HistoryManager,
	logger log.Logger,
) ReplicatorQueueProcessor {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		BatchSize:                           config.ReplicatorTaskBatchSize,
		WorkerCount:                         config.ReplicatorTaskWorkerCount,
		MaxPollRPS:                          config.ReplicatorProcessorMaxPollRPS,
		MaxPollInterval:                     config.ReplicatorProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    config.ReplicatorProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                   config.ReplicatorProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient:  config.ReplicatorProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                       config.ReplicatorTaskMaxRetryCount,
		RedispatchInterval:                  config.ReplicatorProcessorRedispatchInterval,
		RedispatchIntervalJitterCoefficient: config.ReplicatorProcessorRedispatchIntervalJitterCoefficient,
		MaxRedispatchQueueSize:              config.ReplicatorProcessorMaxRedispatchQueueSize,
		EnablePriorityTaskProcessor:         config.ReplicatorProcessorEnablePriorityTaskProcessor,
		MetricScope:                         metrics.ReplicatorQueueProcessorScope,
	}

	logger = logger.WithTags(tag.ComponentReplicatorQueue)

	replicationTaskFilter := func(taskInfo queueTaskInfo) (bool, error) {
		return true, nil
	}

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetMaximumAttempts(10)
	retryPolicy.SetBackoffCoefficient(1)

	processor := &replicatorQueueProcessorImpl{
		currentClusterName:    currentClusterName,
		shard:                 shard,
		historyCache:          historyCache,
		replicationTaskFilter: replicationTaskFilter,
		executionMgr:          executionMgr,
		historyV2Mgr:          historyV2Mgr,
		replicator:            replicator,
		metricsClient:         shard.GetMetricsClient(),
		options:               options,
		logger:                logger,
		retryPolicy:           retryPolicy,
		fetchTasksBatchSize:   config.ReplicatorProcessorFetchTasksBatchSize(),
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetReplicatorAckLevel(), logger)
	queueProcessorBase := newQueueProcessorBase(
		currentClusterName,
		shard,
		options,
		processor,
		nil, // replicator queue processor will soon be deprecated and won't use priority task processor
		queueAckMgr,
		nil, // replicator queue processor will soon be deprecated and won't use redispatch queue
		historyCache,
		nil, // there's no queueTask implementation for replication task
		logger,
		shard.GetMetricsClient().Scope(metrics.ReplicatorQueueProcessorScope),
	)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (p *replicatorQueueProcessorImpl) getTaskFilter() taskFilter {
	return p.replicationTaskFilter
}

func (p *replicatorQueueProcessorImpl) complete(
	taskInfo *taskInfo,
) {
	p.queueProcessorBase.complete(taskInfo.task)
}

func (p *replicatorQueueProcessorImpl) process(
	taskInfo *taskInfo,
) (int, error) {

	task, ok := taskInfo.task.(*persistence.ReplicationTaskInfoWrapper)
	if !ok {
		return metrics.ReplicatorQueueProcessorScope, errUnexpectedQueueTask
	}
	// replication queue should always process all tasks
	// so should not do anything to shouldProcessTask variable

	switch task.TaskType {
	case commongenpb.TaskType_ReplicationSyncActivity:
		err := p.processSyncActivityTask(task.ReplicationTaskInfo)
		if err == nil {
			err = p.executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{TaskID: task.GetTaskId()})
		}
		return metrics.ReplicatorTaskSyncActivityScope, err
	case commongenpb.TaskType_ReplicationHistory:
		err := p.processHistoryReplicationTask(task.ReplicationTaskInfo)
		if _, ok := err.(*serviceerror.NotFound); ok {
			err = errHistoryNotFoundTask
		}
		if err == nil {
			err = p.executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{TaskID: task.GetTaskId()})
		}
		return metrics.ReplicatorTaskHistoryScope, err
	default:
		return metrics.ReplicatorQueueProcessorScope, errUnknownReplicationTask
	}
}

func (p *replicatorQueueProcessorImpl) queueShutdown() error {
	// there is no shutdown specific behavior for replication queue
	return nil
}

func (p *replicatorQueueProcessorImpl) processSyncActivityTask(
	task *persistenceblobs.ReplicationTaskInfo,
) error {

	replicationTask, err := p.generateSyncActivityTask(context.Background(), task)
	if err != nil || replicationTask == nil {
		return err
	}

	return p.replicator.Publish(replicationTask)
}

func (p *replicatorQueueProcessorImpl) processHistoryReplicationTask(
	task *persistenceblobs.ReplicationTaskInfo,
) error {
	replicationTask, err := p.toReplicationTask(context.Background(), &persistence.ReplicationTaskInfoWrapper{task})
	if err != nil || replicationTask == nil {
		return err
	}

	err = p.replicator.Publish(replicationTask)
	if err == messaging.ErrMessageSizeLimit && replicationTask.GetHistoryTaskAttributes() != nil {
		// message size exceeds the server messaging size limit
		// for this specific case, just send out a metadata message and
		// let receiver fetch from source (for the concrete history events)
		err = p.replicator.Publish(p.generateHistoryMetadataTask(replicationTask.GetHistoryTaskAttributes().TargetClusters, task))
	}
	return err
}

func (p *replicatorQueueProcessorImpl) generateHistoryMetadataTask(targetClusters []string, task *persistenceblobs.ReplicationTaskInfo) *replicationgenpb.ReplicationTask {
	return &replicationgenpb.ReplicationTask{
		TaskType: replicationgenpb.ReplicationTaskType_HistoryMetadataTask,
		Attributes: &replicationgenpb.ReplicationTask_HistoryMetadataTaskAttributes{
			HistoryMetadataTaskAttributes: &replicationgenpb.HistoryMetadataTaskAttributes{
				TargetClusters: targetClusters,
				NamespaceId:    task.GetNamespaceId(),
				WorkflowId:     task.GetWorkflowId(),
				RunId:          task.GetRunId(),
				FirstEventId:   task.GetFirstEventId(),
				NextEventId:    task.GetNextEventId(),
			},
		},
	}
}

// GenerateReplicationTask generate replication task
func GenerateReplicationTask(
	targetClusters []string,
	task *persistenceblobs.ReplicationTaskInfo,
	historyV2Mgr persistence.HistoryManager,
	metricsClient metrics.Client,
	history *eventpb.History,
	shardID *int,
) (*replicationgenpb.ReplicationTask, string, error) {
	var err error
	if history == nil {
		history, _, err = GetAllHistory(historyV2Mgr, metricsClient, false,
			task.GetFirstEventId(), task.GetNextEventId(), task.BranchToken, shardID)
		if err != nil {
			return nil, "", err
		}
		for _, event := range history.Events {
			if task.Version != event.GetVersion() {
				return nil, "", nil
			}
		}
	}

	var newRunID string
	var newRunHistory *eventpb.History
	events := history.Events
	if len(events) > 0 {
		lastEvent := events[len(events)-1]
		if lastEvent.GetEventType() == eventpb.EventType_WorkflowExecutionContinuedAsNew {
			// Check if this is replication task for ContinueAsNew event, then retrieve the history for new execution
			newRunID = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
			newRunHistory, _, err = GetAllHistory(
				historyV2Mgr,
				metricsClient,
				false,
				common.FirstEventID,
				common.FirstEventID+1, // [common.FirstEventID to common.FirstEventID+1) will get the first batch
				task.NewRunBranchToken,
				shardID)
			if err != nil {
				return nil, "", err
			}
		}
	}

	ret := &replicationgenpb.ReplicationTask{
		TaskType: replicationgenpb.ReplicationTaskType_HistoryTask,
		Attributes: &replicationgenpb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationgenpb.HistoryTaskAttributes{
				TargetClusters:  targetClusters,
				NamespaceId:     task.GetNamespaceId(),
				WorkflowId:      task.GetWorkflowId(),
				RunId:           task.GetRunId(),
				FirstEventId:    task.GetFirstEventId(),
				NextEventId:     task.GetNextEventId(),
				Version:         task.Version,
				ReplicationInfo: task.LastReplicationInfo,
				History:         history,
				NewRunHistory:   newRunHistory,
				ResetWorkflow:   task.ResetWorkflow,
			},
		},
	}
	return ret, newRunID, nil
}

func (p *replicatorQueueProcessorImpl) readTasks(readLevel int64) ([]queueTaskInfo, bool, error) {
	return p.readTasksWithBatchSize(readLevel, p.options.BatchSize())
}

func (p *replicatorQueueProcessorImpl) updateAckLevel(ackLevel int64) error {
	err := p.shard.UpdateReplicatorAckLevel(ackLevel)

	// this is a hack, since there is not dedicated ticker on the queue processor
	// to periodically send out sync shard message, put it here
	now := clock.NewRealTimeSource().Now()
	if p.lastShardSyncTimestamp.Add(p.shard.GetConfig().ShardSyncMinInterval()).Before(now) {
		syncStatusTask := &replicationgenpb.ReplicationTask{
			TaskType: replicationgenpb.ReplicationTaskType_SyncShardStatusTask,
			Attributes: &replicationgenpb.ReplicationTask_SyncShardStatusTaskAttributes{
				SyncShardStatusTaskAttributes: &replicationgenpb.SyncShardStatusTaskAttributes{
					SourceCluster: p.currentClusterName,
					ShardId:       int64(p.shard.GetShardID()),
					Timestamp:     now.UnixNano(),
				},
			},
		}
		// ignore the error
		if syncErr := p.replicator.Publish(syncStatusTask); syncErr == nil {
			p.lastShardSyncTimestamp = now
		}
	}
	return err
}

// GetAllHistory return history
func GetAllHistory(
	historyV2Mgr persistence.HistoryManager,
	metricsClient metrics.Client,
	byBatch bool,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
	shardID *int,
) (*eventpb.History, []*eventpb.History, error) {

	// overall result
	var historyEvents []*eventpb.HistoryEvent
	var historyBatches []*eventpb.History
	historySize := 0
	var err error

	// variable used for each page
	var pageHistoryEvents []*eventpb.HistoryEvent
	var pageHistoryBatches []*eventpb.History
	var pageToken []byte
	var pageHistorySize int

	for hasMore := true; hasMore; hasMore = len(pageToken) > 0 {
		pageHistoryEvents, pageHistoryBatches, pageToken, pageHistorySize, err = PaginateHistory(
			historyV2Mgr, byBatch,
			branchToken, firstEventID, nextEventID,
			pageToken, defaultHistoryPageSize, shardID,
		)
		if err != nil {
			return nil, nil, err
		}

		historyEvents = append(historyEvents, pageHistoryEvents...)
		historyBatches = append(historyBatches, pageHistoryBatches...)
		historySize += pageHistorySize
	}

	// Emit metric and log for history size
	if metricsClient != nil {
		metricsClient.RecordTimer(metrics.ReplicatorQueueProcessorScope, metrics.HistorySize, time.Duration(historySize))
	}

	history := &eventpb.History{
		Events: historyEvents,
	}
	return history, historyBatches, nil
}

// PaginateHistory return paged history
func PaginateHistory(
	historyV2Mgr persistence.HistoryManager,
	byBatch bool,
	branchToken []byte,
	firstEventID int64,
	nextEventID int64,
	tokenIn []byte,
	pageSize int,
	shardID *int,
) ([]*eventpb.HistoryEvent, []*eventpb.History, []byte, int, error) {

	var historyEvents []*eventpb.HistoryEvent
	var historyBatches []*eventpb.History
	var tokenOut []byte
	var historySize int

	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      pageSize,
		NextPageToken: tokenIn,
		ShardID:       shardID,
	}
	if byBatch {
		response, err := historyV2Mgr.ReadHistoryBranchByBatch(req)
		if err != nil {
			return nil, nil, nil, 0, err
		}

		// Keep track of total history size
		historySize += response.Size
		historyBatches = append(historyBatches, response.History...)
		tokenOut = response.NextPageToken

	} else {
		response, err := historyV2Mgr.ReadHistoryBranch(req)
		if err != nil {
			return nil, nil, nil, 0, err
		}

		// Keep track of total history size
		historySize += response.Size
		historyEvents = append(historyEvents, response.HistoryEvents...)
		tokenOut = response.NextPageToken
	}

	return historyEvents, historyBatches, tokenOut, historySize, nil
}

// TODO deprecate when 3+DC is released
func convertLastReplicationInfo(info map[string]*replicationgenpb.ReplicationInfo) map[string]*replicationgenpb.ReplicationInfo {
	replicationInfoMap := make(map[string]*replicationgenpb.ReplicationInfo)
	for k, v := range info {
		replicationInfoMap[k] = &replicationgenpb.ReplicationInfo{
			Version:     v.Version,
			LastEventId: v.LastEventId,
		}
	}

	return replicationInfoMap
}

// TODO: when kafka deprecation is finished, delete all logic above
//  and move logic below to dedicated replicationTaskAckMgr

func (p *replicatorQueueProcessorImpl) getTasks(
	ctx context.Context,
	pollingCluster string,
	lastReadTaskID int64,
) (*replicationgenpb.ReplicationMessages, error) {

	if lastReadTaskID == emptyMessageID {
		lastReadTaskID = p.shard.GetClusterReplicationLevel(pollingCluster)
	}

	taskInfoList, hasMore, err := p.readTasksWithBatchSize(lastReadTaskID, p.fetchTasksBatchSize)
	if err != nil {
		return nil, err
	}

	var replicationTasks []*replicationgenpb.ReplicationTask
	readLevel := lastReadTaskID
	for _, taskInfo := range taskInfoList {
		var replicationTask *replicationgenpb.ReplicationTask
		op := func() error {
			var err error
			replicationTask, err = p.toReplicationTask(ctx, taskInfo)
			return err
		}

		err = backoff.Retry(op, p.retryPolicy, common.IsPersistenceTransientError)
		if err != nil {
			p.logger.Debug("Failed to get replication task. Return what we have so far.", tag.Error(err))
			hasMore = true
			break
		}
		readLevel = taskInfo.GetTaskId()
		if replicationTask != nil {
			replicationTasks = append(replicationTasks, replicationTask)
		}
	}

	// Note this is a very rough indicator of how much the remote DC is behind on this shard.
	p.metricsClient.RecordTimer(
		metrics.ReplicatorQueueProcessorScope,
		metrics.ReplicationTasksLag,
		time.Duration(p.shard.GetTransferMaxReadLevel()-readLevel),
	)

	p.metricsClient.RecordTimer(
		metrics.ReplicatorQueueProcessorScope,
		metrics.ReplicationTasksFetched,
		time.Duration(len(taskInfoList)),
	)

	p.metricsClient.RecordTimer(
		metrics.ReplicatorQueueProcessorScope,
		metrics.ReplicationTasksReturned,
		time.Duration(len(replicationTasks)),
	)

	if err := p.shard.UpdateClusterReplicationLevel(
		pollingCluster,
		lastReadTaskID,
	); err != nil {
		p.logger.Error("error updating replication level for shard", tag.Error(err), tag.OperationFailed)
	}

	return &replicationgenpb.ReplicationMessages{
		ReplicationTasks:       replicationTasks,
		HasMore:                hasMore,
		LastRetrievedMessageId: readLevel,
	}, nil
}

func (p *replicatorQueueProcessorImpl) getTask(
	ctx context.Context,
	taskInfo *replicationgenpb.ReplicationTaskInfo,
) (*replicationgenpb.ReplicationTask, error) {

	task := &persistenceblobs.ReplicationTaskInfo{
		NamespaceId:  taskInfo.GetNamespaceId(),
		WorkflowId:   taskInfo.GetWorkflowId(),
		RunId:        taskInfo.GetRunId(),
		TaskId:       taskInfo.GetTaskId(),
		TaskType:     taskInfo.GetTaskType(),
		FirstEventId: taskInfo.GetFirstEventId(),
		NextEventId:  taskInfo.GetNextEventId(),
		Version:      taskInfo.GetVersion(),
		ScheduledId:  taskInfo.GetScheduledId(),
	}
	return p.toReplicationTask(ctx, &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: task})
}

func (p *replicatorQueueProcessorImpl) readTasksWithBatchSize(readLevel int64, batchSize int) ([]queueTaskInfo, bool, error) {
	response, err := p.executionMgr.GetReplicationTasks(&persistence.GetReplicationTasksRequest{
		ReadLevel:    readLevel,
		MaxReadLevel: p.shard.GetTransferMaxReadLevel(),
		BatchSize:    batchSize,
	})

	if err != nil {
		return nil, false, err
	}

	tasks := make([]queueTaskInfo, len(response.Tasks))
	for i := range response.Tasks {
		tasks[i] = &persistence.ReplicationTaskInfoWrapper{ReplicationTaskInfo: response.Tasks[i]}
	}

	return tasks, len(response.NextPageToken) != 0, nil
}

func (p *replicatorQueueProcessorImpl) toReplicationTask(
	ctx context.Context,
	qTask queueTaskInfo,
) (*replicationgenpb.ReplicationTask, error) {

	t, ok := qTask.(*persistence.ReplicationTaskInfoWrapper)
	if !ok {
		return nil, errUnexpectedQueueTask
	}

	task := t.ReplicationTaskInfo
	switch task.TaskType {
	case commongenpb.TaskType_ReplicationSyncActivity:
		task, err := p.generateSyncActivityTask(ctx, task)
		if task != nil {
			task.SourceTaskId = qTask.GetTaskId()
		}
		return task, err
	case commongenpb.TaskType_ReplicationHistory:
		task, err := p.generateHistoryReplicationTask(ctx, task)
		if task != nil {
			task.SourceTaskId = qTask.GetTaskId()
		}
		return task, err
	default:
		return nil, errUnknownReplicationTask
	}
}

func (p *replicatorQueueProcessorImpl) generateSyncActivityTask(
	ctx context.Context,
	taskInfo *persistenceblobs.ReplicationTaskInfo,
) (*replicationgenpb.ReplicationTask, error) {
	namespaceID := taskInfo.GetNamespaceId()
	runID := taskInfo.GetRunId()
	return p.processReplication(
		ctx,
		false, // not necessary to send out sync activity task if workflow closed
		namespaceID,
		taskInfo.GetWorkflowId(),
		runID,
		func(mutableState mutableState) (*replicationgenpb.ReplicationTask, error) {
			activityInfo, ok := mutableState.GetActivityInfo(taskInfo.GetScheduledId())
			if !ok {
				return nil, nil
			}

			var startedTime int64
			var heartbeatTime int64
			scheduledTime := activityInfo.ScheduledTime.UnixNano()
			if activityInfo.StartedID != common.EmptyEventID {
				startedTime = activityInfo.StartedTime.UnixNano()
			}
			// LastHeartBeatUpdatedTime must be valid when getting the sync activity replication task
			heartbeatTime = activityInfo.LastHeartBeatUpdatedTime.UnixNano()

			// Version history uses when replicate the sync activity task
			versionHistories := mutableState.GetVersionHistories()
			var versionHistory *eventgenpb.VersionHistory
			if versionHistories != nil {
				rawVersionHistory, err := versionHistories.GetCurrentVersionHistory()
				if err != nil {
					return nil, err
				}
				versionHistory = rawVersionHistory.ToProto()
			}

			return &replicationgenpb.ReplicationTask{
				TaskType: replicationgenpb.ReplicationTaskType_SyncActivityTask,
				Attributes: &replicationgenpb.ReplicationTask_SyncActivityTaskAttributes{
					SyncActivityTaskAttributes: &replicationgenpb.SyncActivityTaskAttributes{
						NamespaceId:        namespaceID,
						WorkflowId:         taskInfo.GetWorkflowId(),
						RunId:              runID,
						Version:            activityInfo.Version,
						ScheduledId:        activityInfo.ScheduleID,
						ScheduledTime:      scheduledTime,
						StartedId:          activityInfo.StartedID,
						StartedTime:        startedTime,
						LastHeartbeatTime:  heartbeatTime,
						Details:            activityInfo.Details,
						Attempt:            activityInfo.Attempt,
						LastFailureReason:  activityInfo.LastFailureReason,
						LastWorkerIdentity: activityInfo.LastWorkerIdentity,
						LastFailureDetails: activityInfo.LastFailureDetails,
						VersionHistory:     versionHistory,
					},
				},
			}, nil
		},
	)
}

func (p *replicatorQueueProcessorImpl) generateHistoryReplicationTask(
	ctx context.Context,
	task *persistenceblobs.ReplicationTaskInfo,
) (*replicationgenpb.ReplicationTask, error) {
	namespaceID := task.GetNamespaceId()
	runID := task.GetRunId()
	return p.processReplication(
		ctx,
		true, // still necessary to send out history replication message if workflow closed
		namespaceID,
		task.GetWorkflowId(),
		runID,
		func(mutableState mutableState) (*replicationgenpb.ReplicationTask, error) {

			versionHistories := mutableState.GetVersionHistories()

			// TODO when 3+DC migration is done, remove this block of code
			if versionHistories == nil {
				namespaceEntry, err := p.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
				if err != nil {
					return nil, err
				}

				var targetClusters []string
				for _, cluster := range namespaceEntry.GetReplicationConfig().Clusters {
					targetClusters = append(targetClusters, cluster)
				}

				replicationTask, newRunID, err := GenerateReplicationTask(
					targetClusters,
					task,
					p.historyV2Mgr,
					p.metricsClient,
					nil,
					convert.IntPtr(p.shard.GetShardID()),
				)
				if err != nil {
					return nil, err
				}
				if newRunID != "" {
					isNDCWorkflow, err := p.isNewRunNDCEnabled(ctx, namespaceID, task.GetWorkflowId(), newRunID)
					if err != nil {
						return nil, err
					}
					replicationTask.GetHistoryTaskAttributes().NewRunNDC = isNDCWorkflow
				}

				return replicationTask, err
			}

			// NDC workflow
			versionHistoryItems, branchToken, err := p.getVersionHistoryItems(
				mutableState,
				task.GetFirstEventId(),
				task.Version,
			)
			if err != nil {
				return nil, err
			}

			// BranchToken will not set in get dlq replication message request
			if len(task.BranchToken) == 0 {
				task.BranchToken = branchToken
			}

			eventsBlob, err := p.getEventsBlob(
				task.BranchToken,
				task.GetFirstEventId(),
				task.GetNextEventId(),
			)
			if err != nil {
				return nil, err
			}

			var newRunEventsBlob *commonpb.DataBlob
			if len(task.NewRunBranchToken) != 0 {
				// only get the first batch
				newRunEventsBlob, err = p.getEventsBlob(
					task.NewRunBranchToken,
					common.FirstEventID,
					common.FirstEventID+1,
				)
				if err != nil {
					return nil, err
				}
			}

			replicationTask := &replicationgenpb.ReplicationTask{
				TaskType: replicationgenpb.ReplicationTaskType_HistoryV2Task,
				Attributes: &replicationgenpb.ReplicationTask_HistoryTaskV2Attributes{
					HistoryTaskV2Attributes: &replicationgenpb.HistoryTaskV2Attributes{
						TaskId:              task.GetFirstEventId(),
						NamespaceId:         namespaceID,
						WorkflowId:          task.GetWorkflowId(),
						RunId:               runID,
						VersionHistoryItems: versionHistoryItems,
						Events:              eventsBlob,
						NewRunEvents:        newRunEventsBlob,
					},
				},
			}
			return replicationTask, nil
		},
	)
}

func (p *replicatorQueueProcessorImpl) getEventsBlob(
	branchToken []byte,
	firstEventID int64,
	nextEventID int64,
) (*commonpb.DataBlob, error) {

	var eventBatchBlobs []*serialization.DataBlob
	var pageToken []byte
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: pageToken,
		ShardID:       convert.IntPtr(p.shard.GetShardID()),
	}

	for {
		resp, err := p.historyV2Mgr.ReadRawHistoryBranch(req)
		if err != nil {
			return nil, err
		}

		req.NextPageToken = resp.NextPageToken
		eventBatchBlobs = append(eventBatchBlobs, resp.HistoryEventBlobs...)

		if len(req.NextPageToken) == 0 {
			break
		}
	}

	if len(eventBatchBlobs) != 1 {
		return nil, serviceerror.NewInternal("replicatorQueueProcessor encounter more than 1 NDC raw event batch")
	}

	return eventBatchBlobs[0].ToProto(), nil
}

func (p *replicatorQueueProcessorImpl) getVersionHistoryItems(
	mutableState mutableState,
	eventID int64,
	version int64,
) ([]*eventgenpb.VersionHistoryItem, []byte, error) {

	versionHistories := mutableState.GetVersionHistories()
	if versionHistories == nil {
		return nil, nil, serviceerror.NewInternal("replicatorQueueProcessor encounter workflow without version histories")
	}

	versionHistoryIndex, err := versionHistories.FindFirstVersionHistoryIndexByItem(
		persistence.NewVersionHistoryItem(
			eventID,
			version,
		),
	)
	if err != nil {
		return nil, nil, err
	}

	versionHistory, err := versionHistories.GetVersionHistory(versionHistoryIndex)
	if err != nil {
		return nil, nil, err
	}
	return versionHistory.ToProto().Items, versionHistory.GetBranchToken(), nil
}

func (p *replicatorQueueProcessorImpl) processReplication(
	ctx context.Context,
	processTaskIfClosed bool,
	namespaceID string,
	workflowID string,
	runID string,
	action func(mutableState) (*replicationgenpb.ReplicationTask, error),
) (retReplicationTask *replicationgenpb.ReplicationTask, retError error) {

	execution := executionpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}

	context, release, err := p.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, execution)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	msBuilder, err := context.loadWorkflowExecution()
	switch err.(type) {
	case nil:
		if !processTaskIfClosed && !msBuilder.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the replication task
			return nil, nil
		}
		return action(msBuilder)
	case *serviceerror.NotFound:
		return nil, nil
	default:
		return nil, err
	}
}

func (p *replicatorQueueProcessorImpl) isNewRunNDCEnabled(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
) (isNDCWorkflow bool, retError error) {

	context, release, err := p.historyCache.getOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	)
	if err != nil {
		return false, err
	}
	defer func() { release(retError) }()

	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return false, err
	}
	return mutableState.GetVersionHistories() != nil, nil
}
