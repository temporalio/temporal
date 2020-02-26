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
	ctx "context"
	"errors"
	"time"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
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
		BatchSize:                          config.ReplicatorTaskBatchSize,
		WorkerCount:                        config.ReplicatorTaskWorkerCount,
		MaxPollRPS:                         config.ReplicatorProcessorMaxPollRPS,
		MaxPollInterval:                    config.ReplicatorProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.ReplicatorProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.ReplicatorProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.ReplicatorProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                      config.ReplicatorTaskMaxRetryCount,
		MetricScope:                        metrics.ReplicatorQueueProcessorScope,
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
	queueProcessorBase := newQueueProcessorBase(currentClusterName, shard, options, processor, queueAckMgr, historyCache, logger)
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

	task, ok := taskInfo.task.(*persistence.ReplicationTaskInfo)
	if !ok {
		return metrics.ReplicatorQueueProcessorScope, errUnexpectedQueueTask
	}
	// replication queue should always process all tasks
	// so should not do anything to shouldProcessTask variable

	switch task.TaskType {
	case persistence.ReplicationTaskTypeSyncActivity:
		err := p.processSyncActivityTask(task)
		if err == nil {
			err = p.executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{TaskID: task.GetTaskID()})
		}
		return metrics.ReplicatorTaskSyncActivityScope, err
	case persistence.ReplicationTaskTypeHistory:
		err := p.processHistoryReplicationTask(task)
		if _, ok := err.(*shared.EntityNotExistsError); ok {
			err = errHistoryNotFoundTask
		}
		if err == nil {
			err = p.executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{TaskID: task.GetTaskID()})
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
	task *persistence.ReplicationTaskInfo,
) error {

	replicationTask, err := p.generateSyncActivityTask(ctx.Background(), task)
	if err != nil || replicationTask == nil {
		return err
	}

	return p.replicator.Publish(replicationTask)
}

func (p *replicatorQueueProcessorImpl) processHistoryReplicationTask(
	task *persistence.ReplicationTaskInfo,
) error {

	replicationTask, err := p.toReplicationTask(ctx.Background(), task)
	if err != nil || replicationTask == nil {
		return err
	}

	err = p.replicator.Publish(replicationTask)
	if err == messaging.ErrMessageSizeLimit && replicationTask.HistoryTaskAttributes != nil {
		// message size exceeds the server messaging size limit
		// for this specific case, just send out a metadata message and
		// let receiver fetch from source (for the concrete history events)
		err = p.replicator.Publish(p.generateHistoryMetadataTask(replicationTask.HistoryTaskAttributes.TargetClusters, task))
	}
	return err
}

func (p *replicatorQueueProcessorImpl) generateHistoryMetadataTask(targetClusters []string, task *persistence.ReplicationTaskInfo) *replicator.ReplicationTask {
	return &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeHistoryMetadata.Ptr(),
		HistoryMetadataTaskAttributes: &replicator.HistoryMetadataTaskAttributes{
			TargetClusters: targetClusters,
			DomainId:       common.StringPtr(task.DomainID),
			WorkflowId:     common.StringPtr(task.WorkflowID),
			RunId:          common.StringPtr(task.RunID),
			FirstEventId:   common.Int64Ptr(task.FirstEventID),
			NextEventId:    common.Int64Ptr(task.NextEventID),
		},
	}
}

// GenerateReplicationTask generate replication task
func GenerateReplicationTask(
	targetClusters []string,
	task *persistence.ReplicationTaskInfo,
	historyV2Mgr persistence.HistoryManager,
	metricsClient metrics.Client,
	history *shared.History,
	shardID *int,
) (*replicator.ReplicationTask, string, error) {
	var err error
	if history == nil {
		history, _, err = GetAllHistory(historyV2Mgr, metricsClient, false,
			task.FirstEventID, task.NextEventID, task.BranchToken, shardID)
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
	var newRunHistory *shared.History
	events := history.Events
	if len(events) > 0 {
		lastEvent := events[len(events)-1]
		if lastEvent.GetEventType() == shared.EventTypeWorkflowExecutionContinuedAsNew {
			// Check if this is replication task for ContinueAsNew event, then retrieve the history for new execution
			newRunID = lastEvent.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()
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

	ret := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeHistory),
		HistoryTaskAttributes: &replicator.HistoryTaskAttributes{
			TargetClusters:  targetClusters,
			DomainId:        common.StringPtr(task.DomainID),
			WorkflowId:      common.StringPtr(task.WorkflowID),
			RunId:           common.StringPtr(task.RunID),
			FirstEventId:    common.Int64Ptr(task.FirstEventID),
			NextEventId:     common.Int64Ptr(task.NextEventID),
			Version:         common.Int64Ptr(task.Version),
			ReplicationInfo: convertLastReplicationInfo(task.LastReplicationInfo),
			History:         history,
			NewRunHistory:   newRunHistory,
			ResetWorkflow:   common.BoolPtr(task.ResetWorkflow),
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
		syncStatusTask := &replicator.ReplicationTask{
			TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncShardStatus),
			SyncShardStatusTaskAttributes: &replicator.SyncShardStatusTaskAttributes{
				SourceCluster: common.StringPtr(p.currentClusterName),
				ShardId:       common.Int64Ptr(int64(p.shard.GetShardID())),
				Timestamp:     common.Int64Ptr(now.UnixNano()),
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
) (*shared.History, []*shared.History, error) {

	// overall result
	var historyEvents []*shared.HistoryEvent
	var historyBatches []*shared.History
	historySize := 0
	var err error

	// variable used for each page
	var pageHistoryEvents []*shared.HistoryEvent
	var pageHistoryBatches []*shared.History
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

	history := &shared.History{
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
) ([]*shared.HistoryEvent, []*shared.History, []byte, int, error) {

	historyEvents := []*shared.HistoryEvent{}
	historyBatches := []*shared.History{}
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
func convertLastReplicationInfo(info map[string]*persistence.ReplicationInfo) map[string]*shared.ReplicationInfo {
	replicationInfoMap := make(map[string]*shared.ReplicationInfo)
	for k, v := range info {
		replicationInfoMap[k] = &shared.ReplicationInfo{
			Version:     common.Int64Ptr(v.Version),
			LastEventId: common.Int64Ptr(v.LastEventID),
		}
	}

	return replicationInfoMap
}

// TODO: when kafka deprecation is finished, delete all logic above
//  and move logic below to dedicated replicationTaskAckMgr

func (p *replicatorQueueProcessorImpl) getTasks(
	ctx ctx.Context,
	pollingCluster string,
	lastReadTaskID int64,
) (*replicator.ReplicationMessages, error) {

	if lastReadTaskID == emptyMessageID {
		lastReadTaskID = p.shard.GetClusterReplicationLevel(pollingCluster)
	}

	taskInfoList, hasMore, err := p.readTasksWithBatchSize(lastReadTaskID, p.fetchTasksBatchSize)
	if err != nil {
		return nil, err
	}

	var replicationTasks []*replicator.ReplicationTask
	readLevel := lastReadTaskID
	for _, taskInfo := range taskInfoList {
		var replicationTask *replicator.ReplicationTask
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
		readLevel = taskInfo.GetTaskID()
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

	return &replicator.ReplicationMessages{
		ReplicationTasks:       replicationTasks,
		HasMore:                common.BoolPtr(hasMore),
		LastRetrievedMessageId: common.Int64Ptr(readLevel),
	}, nil
}

func (p *replicatorQueueProcessorImpl) getTask(
	ctx ctx.Context,
	taskInfo *replicator.ReplicationTaskInfo,
) (*replicator.ReplicationTask, error) {

	task := &persistence.ReplicationTaskInfo{
		DomainID:     taskInfo.GetDomainID(),
		WorkflowID:   taskInfo.GetWorkflowID(),
		RunID:        taskInfo.GetRunID(),
		TaskID:       taskInfo.GetTaskID(),
		TaskType:     int(taskInfo.GetTaskType()),
		FirstEventID: taskInfo.GetFirstEventID(),
		NextEventID:  taskInfo.GetNextEventID(),
		Version:      taskInfo.GetVersion(),
		ScheduledID:  taskInfo.GetScheduledID(),
	}
	return p.toReplicationTask(ctx, task)
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
		tasks[i] = response.Tasks[i]
	}

	return tasks, len(response.NextPageToken) != 0, nil
}

func (p *replicatorQueueProcessorImpl) toReplicationTask(
	ctx ctx.Context,
	qTask queueTaskInfo,
) (*replicator.ReplicationTask, error) {

	task, ok := qTask.(*persistence.ReplicationTaskInfo)
	if !ok {
		return nil, errUnexpectedQueueTask
	}

	switch task.TaskType {
	case persistence.ReplicationTaskTypeSyncActivity:
		task, err := p.generateSyncActivityTask(ctx, task)
		if task != nil {
			task.SourceTaskId = common.Int64Ptr(qTask.GetTaskID())
		}
		return task, err
	case persistence.ReplicationTaskTypeHistory:
		task, err := p.generateHistoryReplicationTask(ctx, task)
		if task != nil {
			task.SourceTaskId = common.Int64Ptr(qTask.GetTaskID())
		}
		return task, err
	default:
		return nil, errUnknownReplicationTask
	}
}

func (p *replicatorQueueProcessorImpl) generateSyncActivityTask(
	ctx ctx.Context,
	taskInfo *persistence.ReplicationTaskInfo,
) (*replicator.ReplicationTask, error) {

	return p.processReplication(
		ctx,
		false, // not necessary to send out sync activity task if workflow closed
		taskInfo.GetDomainID(),
		taskInfo.GetWorkflowID(),
		taskInfo.GetRunID(),
		func(mutableState mutableState) (*replicator.ReplicationTask, error) {
			activityInfo, ok := mutableState.GetActivityInfo(taskInfo.ScheduledID)
			if !ok {
				return nil, nil
			}

			var startedTime *int64
			var heartbeatTime *int64
			scheduledTime := common.Int64Ptr(activityInfo.ScheduledTime.UnixNano())
			if activityInfo.StartedID != common.EmptyEventID {
				startedTime = common.Int64Ptr(activityInfo.StartedTime.UnixNano())
			}
			// LastHeartBeatUpdatedTime must be valid when getting the sync activity replication task
			heartbeatTime = common.Int64Ptr(activityInfo.LastHeartBeatUpdatedTime.UnixNano())

			//Version history uses when replicate the sync activity task
			versionHistories := mutableState.GetVersionHistories()
			var versionHistory *shared.VersionHistory
			if versionHistories != nil {
				rawVersionHistory, err := versionHistories.GetCurrentVersionHistory()
				if err != nil {
					return nil, err
				}
				versionHistory = rawVersionHistory.ToThrift()
			}

			return &replicator.ReplicationTask{
				TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncActivity),
				SyncActivityTaskAttributes: &replicator.SyncActivityTaskAttributes{
					DomainId:           common.StringPtr(taskInfo.GetDomainID()),
					WorkflowId:         common.StringPtr(taskInfo.GetWorkflowID()),
					RunId:              common.StringPtr(taskInfo.GetRunID()),
					Version:            common.Int64Ptr(activityInfo.Version),
					ScheduledId:        common.Int64Ptr(activityInfo.ScheduleID),
					ScheduledTime:      scheduledTime,
					StartedId:          common.Int64Ptr(activityInfo.StartedID),
					StartedTime:        startedTime,
					LastHeartbeatTime:  heartbeatTime,
					Details:            activityInfo.Details,
					Attempt:            common.Int32Ptr(activityInfo.Attempt),
					LastFailureReason:  common.StringPtr(activityInfo.LastFailureReason),
					LastWorkerIdentity: common.StringPtr(activityInfo.LastWorkerIdentity),
					LastFailureDetails: activityInfo.LastFailureDetails,
					VersionHistory:     versionHistory,
				},
			}, nil
		},
	)
}

func (p *replicatorQueueProcessorImpl) generateHistoryReplicationTask(
	ctx ctx.Context,
	task *persistence.ReplicationTaskInfo,
) (*replicator.ReplicationTask, error) {

	return p.processReplication(
		ctx,
		true, // still necessary to send out history replication message if workflow closed
		task.GetDomainID(),
		task.GetWorkflowID(),
		task.GetRunID(),
		func(mutableState mutableState) (*replicator.ReplicationTask, error) {

			versionHistories := mutableState.GetVersionHistories()

			// TODO when 3+DC migration is done, remove this block of code
			if versionHistories == nil {
				domainEntry, err := p.shard.GetDomainCache().GetDomainByID(task.DomainID)
				if err != nil {
					return nil, err
				}

				var targetClusters []string
				for _, cluster := range domainEntry.GetReplicationConfig().Clusters {
					targetClusters = append(targetClusters, cluster.ClusterName)
				}

				replicationTask, newRunID, err := GenerateReplicationTask(
					targetClusters,
					task,
					p.historyV2Mgr,
					p.metricsClient,
					nil,
					common.IntPtr(p.shard.GetShardID()),
				)
				if err != nil {
					return nil, err
				}
				if newRunID != "" {
					isNDCWorkflow, err := p.isNewRunNDCEnabled(ctx, task.DomainID, task.WorkflowID, newRunID)
					if err != nil {
						return nil, err
					}
					replicationTask.HistoryTaskAttributes.NewRunNDC = common.BoolPtr(isNDCWorkflow)
				}

				return replicationTask, err
			}

			// NDC workflow
			versionHistoryItems, branchToken, err := p.getVersionHistoryItems(
				mutableState,
				task.FirstEventID,
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
				task.FirstEventID,
				task.NextEventID,
			)
			if err != nil {
				return nil, err
			}

			var newRunEventsBlob *shared.DataBlob
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

			replicationTask := &replicator.ReplicationTask{
				TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeHistoryV2),
				HistoryTaskV2Attributes: &replicator.HistoryTaskV2Attributes{
					TaskId:              common.Int64Ptr(task.FirstEventID),
					DomainId:            common.StringPtr(task.DomainID),
					WorkflowId:          common.StringPtr(task.WorkflowID),
					RunId:               common.StringPtr(task.RunID),
					VersionHistoryItems: versionHistoryItems,
					Events:              eventsBlob,
					NewRunEvents:        newRunEventsBlob,
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
) (*shared.DataBlob, error) {

	var eventBatchBlobs []*persistence.DataBlob
	var pageToken []byte
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: pageToken,
		ShardID:       common.IntPtr(p.shard.GetShardID()),
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
		return nil, &shared.InternalServiceError{
			Message: "replicatorQueueProcessor encounter more than 1 NDC raw event batch",
		}
	}

	return eventBatchBlobs[0].ToThrift(), nil
}

func (p *replicatorQueueProcessorImpl) getVersionHistoryItems(
	mutableState mutableState,
	eventID int64,
	version int64,
) ([]*shared.VersionHistoryItem, []byte, error) {

	versionHistories := mutableState.GetVersionHistories()
	if versionHistories == nil {
		return nil, nil, &shared.InternalServiceError{
			Message: "replicatorQueueProcessor encounter workflow without version histories",
		}
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
	return versionHistory.ToThrift().Items, versionHistory.GetBranchToken(), nil
}

func (p *replicatorQueueProcessorImpl) processReplication(
	ctx ctx.Context,
	processTaskIfClosed bool,
	domainID string,
	workflowID string,
	runID string,
	action func(mutableState) (*replicator.ReplicationTask, error),
) (retReplicationTask *replicator.ReplicationTask, retError error) {

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}

	context, release, err := p.historyCache.getOrCreateWorkflowExecution(ctx, domainID, execution)
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
	case *shared.EntityNotExistsError:
		return nil, nil
	default:
		return nil, err
	}
}

func (p *replicatorQueueProcessorImpl) isNewRunNDCEnabled(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (isNDCWorkflow bool, retError error) {

	context, release, err := p.historyCache.getOrCreateWorkflowExecution(
		ctx,
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
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
