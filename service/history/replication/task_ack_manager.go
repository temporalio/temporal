// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_ack_manager_mock.go

package replication

import (
	ctx "context"
	"errors"
	"strconv"
	"time"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
	exec "github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

var (
	errUnknownQueueTask       = errors.New("unknown task type")
	errUnknownReplicationTask = errors.New("unknown replication task")
	defaultHistoryPageSize    = 1000
)

type (
	// TaskAckManager is the ack manager for replication tasks
	TaskAckManager interface {
		GetTask(
			ctx ctx.Context,
			taskInfo *replicator.ReplicationTaskInfo,
		) (*replicator.ReplicationTask, error)

		GetTasks(
			ctx ctx.Context,
			pollingCluster string,
			lastReadTaskID int64,
		) (*replicator.ReplicationMessages, error)
	}

	taskAckManagerImpl struct {
		shard            shard.Context
		executionCache   *exec.Cache
		executionManager persistence.ExecutionManager
		historyManager   persistence.HistoryManager
		rateLimiter      *quotas.DynamicRateLimiter
		retryPolicy      backoff.RetryPolicy

		metricsClient metrics.Client
		logger        log.Logger

		// This is the batch size used by pull based RPC replicator.
		fetchTasksBatchSize dynamicconfig.IntPropertyFnWithShardIDFilter
	}
)

var _ TaskAckManager = (*taskAckManagerImpl)(nil)

// NewTaskAckManager initializes a new replication task ack manager
func NewTaskAckManager(
	shard shard.Context,
	executionCache *exec.Cache,
) TaskAckManager {

	config := shard.GetConfig()
	rateLimiter := quotas.NewDynamicRateLimiter(func() float64 {
		return config.ReplicationTaskGenerationQPS()
	})
	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetMaximumAttempts(config.ReplicatorReadTaskMaxRetryCount())
	retryPolicy.SetBackoffCoefficient(1)

	return &taskAckManagerImpl{
		shard:               shard,
		executionCache:      executionCache,
		executionManager:    shard.GetExecutionManager(),
		historyManager:      shard.GetHistoryManager(),
		rateLimiter:         rateLimiter,
		retryPolicy:         retryPolicy,
		metricsClient:       shard.GetMetricsClient(),
		logger:              shard.GetLogger().WithTags(tag.ComponentReplicationAckManager),
		fetchTasksBatchSize: config.ReplicatorProcessorFetchTasksBatchSize,
	}
}

func (t *taskAckManagerImpl) GetTask(
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
	return t.toReplicationTask(ctx, task)
}

func (t *taskAckManagerImpl) GetTasks(
	ctx ctx.Context,
	pollingCluster string,
	lastReadTaskID int64,
) (*replicator.ReplicationMessages, error) {

	if lastReadTaskID == common.EmptyMessageID {
		lastReadTaskID = t.shard.GetClusterReplicationLevel(pollingCluster)
	}

	shardID := t.shard.GetShardID()
	replicationScope := t.metricsClient.Scope(
		metrics.ReplicatorQueueProcessorScope,
		metrics.InstanceTag(strconv.Itoa(shardID)),
	)
	taskGeneratedTimer := replicationScope.StartTimer(metrics.TaskLatency)
	taskInfoList, hasMore, err := t.readTasksWithBatchSize(lastReadTaskID, t.fetchTasksBatchSize(shardID))
	if err != nil {
		return nil, err
	}

	var replicationTasks []*replicator.ReplicationTask
	readLevel := lastReadTaskID
	for _, taskInfo := range taskInfoList {
		_ = t.rateLimiter.Wait(ctx)
		var replicationTask *replicator.ReplicationTask
		op := func() error {
			var err error
			replicationTask, err = t.toReplicationTask(ctx, taskInfo)
			return err
		}

		err = backoff.Retry(op, t.retryPolicy, common.IsPersistenceTransientError)
		if err != nil {
			t.logger.Debug("Failed to get replication task. Return what we have so far.", tag.Error(err))
			hasMore = true
			break
		}
		readLevel = taskInfo.GetTaskID()
		if replicationTask != nil {
			replicationTasks = append(replicationTasks, replicationTask)
		}
	}

	taskGeneratedTimer.Stop()

	replicationScope.RecordTimer(
		metrics.ReplicationTasksLag,
		time.Duration(t.shard.GetTransferMaxReadLevel()-readLevel),
	)

	replicationScope.RecordTimer(
		metrics.ReplicationTasksFetched,
		time.Duration(len(taskInfoList)),
	)

	replicationScope.RecordTimer(
		metrics.ReplicationTasksReturned,
		time.Duration(len(replicationTasks)),
	)

	if err := t.shard.UpdateClusterReplicationLevel(
		pollingCluster,
		lastReadTaskID,
	); err != nil {
		t.logger.Error("error updating replication level for shard", tag.Error(err), tag.OperationFailed)
	}

	return &replicator.ReplicationMessages{
		ReplicationTasks:       replicationTasks,
		HasMore:                common.BoolPtr(hasMore),
		LastRetrievedMessageId: common.Int64Ptr(readLevel),
	}, nil
}

func (t *taskAckManagerImpl) toReplicationTask(
	ctx ctx.Context,
	taskInfo task.Info,
) (*replicator.ReplicationTask, error) {

	task, ok := taskInfo.(*persistence.ReplicationTaskInfo)
	if !ok {
		return nil, errUnknownQueueTask
	}

	switch task.TaskType {
	case persistence.ReplicationTaskTypeSyncActivity:
		task, err := t.generateSyncActivityTask(ctx, task)
		if task != nil {
			task.SourceTaskId = common.Int64Ptr(taskInfo.GetTaskID())
		}
		return task, err
	case persistence.ReplicationTaskTypeHistory:
		task, err := t.generateHistoryReplicationTask(ctx, task)
		if task != nil {
			task.SourceTaskId = common.Int64Ptr(taskInfo.GetTaskID())
		}
		return task, err
	case persistence.ReplicationTaskTypeFailoverMarker:
		task := t.generateFailoverMarkerTask(task)
		if task != nil {
			task.SourceTaskId = common.Int64Ptr(taskInfo.GetTaskID())
		}
		return task, nil
	default:
		return nil, errUnknownReplicationTask
	}
}

func (t *taskAckManagerImpl) processReplication(
	ctx ctx.Context,
	processTaskIfClosed bool,
	taskInfo *persistence.ReplicationTaskInfo,
	action func(
		activityInfo *persistence.ActivityInfo,
		versionHistories *persistence.VersionHistories,
	) (*replicator.ReplicationTask, error),
) (retReplicationTask *replicator.ReplicationTask, retError error) {

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr(taskInfo.GetWorkflowID()),
		RunId:      common.StringPtr(taskInfo.GetRunID()),
	}

	context, release, err := t.executionCache.GetOrCreateWorkflowExecution(ctx, taskInfo.GetDomainID(), execution)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	msBuilder, err := context.LoadWorkflowExecution()
	switch err.(type) {
	case nil:
		if !processTaskIfClosed && !msBuilder.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the replication task
			return nil, nil
		}

		var targetVersionHistory *persistence.VersionHistories
		versionHistories := msBuilder.GetVersionHistories()
		if versionHistories != nil {
			targetVersionHistory = msBuilder.GetVersionHistories().Duplicate()
		}

		var targetActivityInfo *persistence.ActivityInfo
		if activityInfo, ok := msBuilder.GetActivityInfo(
			taskInfo.ScheduledID,
		); ok {
			targetActivityInfo = exec.CopyActivityInfo(activityInfo)
		}
		release(nil)

		return action(targetActivityInfo, targetVersionHistory)
	case *shared.EntityNotExistsError:
		return nil, nil
	default:
		return nil, err
	}
}

func (t *taskAckManagerImpl) getEventsBlob(
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
		PageSize:      t.shard.GetConfig().ReplicationTaskProcessorReadHistoryBatchSize(),
		NextPageToken: pageToken,
		ShardID:       common.IntPtr(t.shard.GetShardID()),
	}

	for {
		resp, err := t.historyManager.ReadRawHistoryBranch(req)
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

func (t *taskAckManagerImpl) isNewRunNDCEnabled(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (isNDCWorkflow bool, retError error) {

	context, release, err := t.executionCache.GetOrCreateWorkflowExecution(
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

	mutableState, err := context.LoadWorkflowExecution()
	if err != nil {
		return false, err
	}
	return mutableState.GetVersionHistories() != nil, nil
}

func (t *taskAckManagerImpl) readTasksWithBatchSize(
	readLevel int64,
	batchSize int,
) ([]task.Info, bool, error) {

	response, err := t.executionManager.GetReplicationTasks(&persistence.GetReplicationTasksRequest{
		ReadLevel:    readLevel,
		MaxReadLevel: t.shard.GetTransferMaxReadLevel(),
		BatchSize:    batchSize,
	})

	if err != nil {
		return nil, false, err
	}

	tasks := make([]task.Info, len(response.Tasks))
	for i := range response.Tasks {
		tasks[i] = response.Tasks[i]
	}

	return tasks, len(response.NextPageToken) != 0, nil
}

func (t *taskAckManagerImpl) getAllHistory(
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
) (*shared.History, error) {

	// overall result
	shardID := t.shard.GetShardID()
	var historyEvents []*shared.HistoryEvent
	historySize := 0
	iterator := collection.NewPagingIterator(
		t.getPaginationFunc(
			firstEventID,
			nextEventID,
			branchToken,
			shardID,
			&historySize,
		),
	)
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		historyEvents = append(historyEvents, event.(*shared.HistoryEvent))
	}
	t.metricsClient.RecordTimer(metrics.ReplicatorQueueProcessorScope, metrics.HistorySize, time.Duration(historySize))
	history := &shared.History{
		Events: historyEvents,
	}
	return history, nil
}

func (t *taskAckManagerImpl) getPaginationFunc(
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
	shardID int,
	historySize *int,
) collection.PaginationFn {

	return func(paginationToken []byte) ([]interface{}, []byte, error) {
		events, _, pageToken, pageHistorySize, err := persistence.PaginateHistory(
			t.historyManager,
			false,
			branchToken,
			firstEventID,
			nextEventID,
			paginationToken,
			defaultHistoryPageSize,
			common.IntPtr(shardID),
		)
		if err != nil {
			return nil, nil, err
		}
		*historySize += pageHistorySize
		var paginateItems []interface{}
		for _, event := range events {
			paginateItems = append(paginateItems, event)
		}
		return paginateItems, pageToken, nil
	}
}

func (t *taskAckManagerImpl) generateReplicationTask(
	targetClusters []string,
	task *persistence.ReplicationTaskInfo,
) (*replicator.ReplicationTask, string, error) {

	history, err := t.getAllHistory(
		task.FirstEventID,
		task.NextEventID,
		task.BranchToken,
	)
	if err != nil {
		return nil, "", err
	}
	for _, event := range history.Events {
		if task.Version != event.GetVersion() {
			return nil, "", nil
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
			newRunHistory, err = t.getAllHistory(
				common.FirstEventID,
				common.FirstEventID+1, // [common.FirstEventID to common.FirstEventID+1) will get the first batch
				task.NewRunBranchToken,
			)
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

func (t *taskAckManagerImpl) generateFailoverMarkerTask(
	taskInfo *persistence.ReplicationTaskInfo,
) *replicator.ReplicationTask {

	return &replicator.ReplicationTask{
		TaskType:     replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeFailoverMarker),
		SourceTaskId: common.Int64Ptr(taskInfo.GetTaskID()),
		FailoverMarkerAttributes: &replicator.FailoverMarkerAttributes{
			DomainID:        common.StringPtr(taskInfo.GetDomainID()),
			FailoverVersion: common.Int64Ptr(taskInfo.GetVersion()),
			CreationTime:    common.Int64Ptr(taskInfo.CreationTime),
		},
	}
}

func (t *taskAckManagerImpl) generateSyncActivityTask(
	ctx ctx.Context,
	taskInfo *persistence.ReplicationTaskInfo,
) (*replicator.ReplicationTask, error) {

	return t.processReplication(
		ctx,
		false, // not necessary to send out sync activity task if workflow closed
		taskInfo,
		func(
			activityInfo *persistence.ActivityInfo,
			versionHistories *persistence.VersionHistories,
		) (*replicator.ReplicationTask, error) {
			if activityInfo == nil {
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

func (t *taskAckManagerImpl) generateHistoryReplicationTask(
	ctx ctx.Context,
	task *persistence.ReplicationTaskInfo,
) (*replicator.ReplicationTask, error) {

	return t.processReplication(
		ctx,
		true, // still necessary to send out history replication message if workflow closed
		task,
		func(
			activityInfo *persistence.ActivityInfo,
			versionHistories *persistence.VersionHistories,
		) (*replicator.ReplicationTask, error) {
			// TODO when 3+DC migration is done, remove this block of code
			if versionHistories == nil {
				domainEntry, err := t.shard.GetDomainCache().GetDomainByID(task.DomainID)
				if err != nil {
					return nil, err
				}

				var targetClusters []string
				for _, cluster := range domainEntry.GetReplicationConfig().Clusters {
					targetClusters = append(targetClusters, cluster.ClusterName)
				}

				replicationTask, newRunID, err := t.generateReplicationTask(
					targetClusters,
					task,
				)
				if err != nil {
					return nil, err
				}
				if newRunID != "" {
					isNDCWorkflow, err := t.isNewRunNDCEnabled(ctx, task.DomainID, task.WorkflowID, newRunID)
					if err != nil {
						return nil, err
					}
					replicationTask.HistoryTaskAttributes.NewRunNDC = common.BoolPtr(isNDCWorkflow)
				}

				return replicationTask, err
			}

			// NDC workflow
			versionHistoryItems, branchToken, err := getVersionHistoryItems(
				versionHistories,
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

			eventsBlob, err := t.getEventsBlob(
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
				newRunEventsBlob, err = t.getEventsBlob(
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

func getVersionHistoryItems(
	versionHistories *persistence.VersionHistories,
	eventID int64,
	version int64,
) ([]*shared.VersionHistoryItem, []byte, error) {

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
