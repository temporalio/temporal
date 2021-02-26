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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
)

type (
	replicatorQueueProcessorImpl struct {
		currentClusterName string
		shard              shard.Context
		historyCache       *historyCache
		executionMgr       persistence.ExecutionManager
		historyMgr         persistence.HistoryManager
		replicator         messaging.Producer
		metricsClient      metrics.Client
		logger             log.Logger
		retryPolicy        backoff.RetryPolicy
		// This is the batch size used by pull based RPC replicator.
		fetchTasksBatchSize int
	}
)

var (
	errUnknownReplicationTask = errors.New("unknown replication task")
)

func newReplicatorQueueProcessor(
	shard shard.Context,
	historyCache *historyCache,
	executionMgr persistence.ExecutionManager,
	historyMgr persistence.HistoryManager,
	logger log.Logger,
) *replicatorQueueProcessorImpl {

	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	config := shard.GetConfig()
	logger = logger.WithTags(tag.ComponentReplicatorQueue)

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetMaximumAttempts(10)
	retryPolicy.SetBackoffCoefficient(1)

	return &replicatorQueueProcessorImpl{
		currentClusterName:  currentClusterName,
		shard:               shard,
		historyCache:        historyCache,
		executionMgr:        executionMgr,
		historyMgr:          historyMgr,
		metricsClient:       shard.GetMetricsClient(),
		logger:              logger,
		retryPolicy:         retryPolicy,
		fetchTasksBatchSize: config.ReplicatorProcessorFetchTasksBatchSize(),
	}
}

func (p *replicatorQueueProcessorImpl) getTasks(
	ctx context.Context,
	pollingCluster string,
	lastReadTaskID int64,
) (*replicationspb.ReplicationMessages, error) {

	if lastReadTaskID == persistence.EmptyQueueMessageID {
		lastReadTaskID = p.shard.GetClusterReplicationLevel(pollingCluster)
	} else {
		if err := p.shard.UpdateClusterReplicationLevel(
			pollingCluster,
			lastReadTaskID,
		); err != nil {
			p.logger.Error("error updating replication level for shard", tag.Error(err), tag.OperationFailed)
		}
	}

	taskInfoList, hasMore, err := p.readTasksWithBatchSize(lastReadTaskID, p.fetchTasksBatchSize)
	if err != nil {
		return nil, err
	}

	var replicationTasks []*replicationspb.ReplicationTask
	readLevel := lastReadTaskID
	for _, taskInfo := range taskInfoList {
		var replicationTask *replicationspb.ReplicationTask
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
	p.metricsClient.Scope(
		metrics.ReplicatorQueueProcessorScope,
		metrics.TargetClusterTag(pollingCluster),
	).RecordDistribution(
		metrics.ReplicationTasksLag,
		int(p.shard.GetTransferMaxReadLevel()-readLevel),
	)

	p.metricsClient.RecordDistribution(
		metrics.ReplicatorQueueProcessorScope,
		metrics.ReplicationTasksFetched,
		len(taskInfoList),
	)

	p.metricsClient.RecordDistribution(
		metrics.ReplicatorQueueProcessorScope,
		metrics.ReplicationTasksReturned,
		len(replicationTasks),
	)

	return &replicationspb.ReplicationMessages{
		ReplicationTasks:       replicationTasks,
		HasMore:                hasMore,
		LastRetrievedMessageId: readLevel,
	}, nil
}

func (p *replicatorQueueProcessorImpl) getTask(
	ctx context.Context,
	taskInfo *replicationspb.ReplicationTaskInfo,
) (*replicationspb.ReplicationTask, error) {

	task := &persistencespb.ReplicationTaskInfo{
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

func (p *replicatorQueueProcessorImpl) readTasksWithBatchSize(
	readLevel int64,
	batchSize int,
) ([]queueTaskInfo, bool, error) {
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
) (*replicationspb.ReplicationTask, error) {

	t, ok := qTask.(*persistence.ReplicationTaskInfoWrapper)
	if !ok {
		return nil, errUnexpectedQueueTask
	}

	task := t.ReplicationTaskInfo
	switch task.TaskType {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return p.generateSyncActivityTask(ctx, task)

	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return p.generateHistoryReplicationTask(ctx, task)

	default:
		return nil, errUnknownReplicationTask
	}
}

func (p *replicatorQueueProcessorImpl) generateSyncActivityTask(
	ctx context.Context,
	taskInfo *persistencespb.ReplicationTaskInfo,
) (*replicationspb.ReplicationTask, error) {
	namespaceID := taskInfo.GetNamespaceId()
	workflowID := taskInfo.GetWorkflowId()
	runID := taskInfo.GetRunId()
	taskID := taskInfo.GetTaskId()
	return p.processReplication(
		ctx,
		false, // not necessary to send out sync activity task if workflow closed
		namespaceID,
		workflowID,
		runID,
		func(mutableState mutableState) (*replicationspb.ReplicationTask, error) {
			activityInfo, ok := mutableState.GetActivityInfo(taskInfo.GetScheduledId())
			if !ok {
				return nil, nil
			}

			var startedTime *time.Time
			var heartbeatTime *time.Time
			scheduledTime := activityInfo.ScheduledTime

			// Todo: Comment why this exists? Why not set?
			if activityInfo.StartedId != common.EmptyEventID {
				startedTime = activityInfo.StartedTime
			}

			// LastHeartbeatUpdateTime must be valid when getting the sync activity replication task
			heartbeatTime = activityInfo.LastHeartbeatUpdateTime

			// Version history uses when replicate the sync activity task
			versionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
			versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
			if err != nil {
				return nil, err
			}

			return &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
				SourceTaskId: taskID,
				Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
					SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
						NamespaceId:        namespaceID,
						WorkflowId:         workflowID,
						RunId:              runID,
						Version:            activityInfo.Version,
						ScheduledId:        activityInfo.ScheduleId,
						ScheduledTime:      scheduledTime,
						StartedId:          activityInfo.StartedId,
						StartedTime:        startedTime,
						LastHeartbeatTime:  heartbeatTime,
						Details:            activityInfo.LastHeartbeatDetails,
						Attempt:            activityInfo.Attempt,
						LastFailure:        activityInfo.RetryLastFailure,
						LastWorkerIdentity: activityInfo.RetryLastWorkerIdentity,
						VersionHistory:     versionHistory,
					},
				},
			}, nil
		},
	)
}

func (p *replicatorQueueProcessorImpl) generateHistoryReplicationTask(
	ctx context.Context,
	taskInfo *persistencespb.ReplicationTaskInfo,
) (*replicationspb.ReplicationTask, error) {
	namespaceID := taskInfo.GetNamespaceId()
	workflowID := taskInfo.GetWorkflowId()
	runID := taskInfo.GetRunId()
	taskID := taskInfo.GetTaskId()
	return p.processReplication(
		ctx,
		true, // still necessary to send out history replication message if workflow closed
		namespaceID,
		workflowID,
		runID,
		func(mutableState mutableState) (*replicationspb.ReplicationTask, error) {
			versionHistoryItems, branchToken, err := p.getVersionHistoryItems(
				mutableState,
				taskInfo.GetFirstEventId(),
				taskInfo.Version,
			)
			if err != nil {
				return nil, err
			}

			// BranchToken will not set in get dlq replication message request
			if len(taskInfo.BranchToken) == 0 {
				taskInfo.BranchToken = branchToken
			}

			eventsBlob, err := p.getEventsBlob(
				taskInfo.BranchToken,
				taskInfo.GetFirstEventId(),
				taskInfo.GetNextEventId(),
			)
			if err != nil {
				return nil, err
			}

			var newRunEventsBlob *commonpb.DataBlob
			if len(taskInfo.NewRunBranchToken) != 0 {
				// only get the first batch
				newRunEventsBlob, err = p.getEventsBlob(
					taskInfo.NewRunBranchToken,
					common.FirstEventID,
					common.FirstEventID+1,
				)
				if err != nil {
					return nil, err
				}
			}

			replicationTask := &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
				SourceTaskId: taskID,
				Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
					HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
						TaskId:              taskInfo.GetFirstEventId(),
						NamespaceId:         namespaceID,
						WorkflowId:          workflowID,
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

	var eventBatchBlobs []*commonpb.DataBlob
	var pageToken []byte
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: pageToken,
		ShardID:       p.shard.GetShardID(),
	}

	for {
		resp, err := p.historyMgr.ReadRawHistoryBranch(req)
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

	return eventBatchBlobs[0], nil
}

func (p *replicatorQueueProcessorImpl) getVersionHistoryItems(
	mutableState mutableState,
	eventID int64,
	version int64,
) ([]*historyspb.VersionHistoryItem, []byte, error) {

	versionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
	if versionHistories == nil {
		return nil, nil, serviceerror.NewInternal("replicatorQueueProcessor encounter workflow without version histories")
	}

	versionHistoryIndex, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(
		versionHistories,
		versionhistory.NewVersionHistoryItem(
			eventID,
			version,
		),
	)
	if err != nil {
		return nil, nil, err
	}

	versionHistory, err := versionhistory.GetVersionHistory(versionHistories, versionHistoryIndex)
	if err != nil {
		return nil, nil, err
	}
	return versionHistory.GetItems(), versionHistory.GetBranchToken(), nil
}

func (p *replicatorQueueProcessorImpl) processReplication(
	ctx context.Context,
	processTaskIfClosed bool,
	namespaceID string,
	workflowID string,
	runID string,
	action func(mutableState) (*replicationspb.ReplicationTask, error),
) (retReplicationTask *replicationspb.ReplicationTask, retError error) {

	execution := commonpb.WorkflowExecution{
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
