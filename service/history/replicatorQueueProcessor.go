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
	"fmt"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	replicatorQueueProcessorImpl struct {
		currentClusterName string
		shard              shard.Context
		config             *configs.Config
		historyCache       workflow.Cache
		executionMgr       persistence.ExecutionManager
		metricsClient      metrics.Client
		logger             log.Logger
		retryPolicy        backoff.RetryPolicy
		pageSize           int

		sync.Mutex
		// largest replication task ID generated
		maxTaskID       *int64
		sanityCheckTime time.Time
	}
)

var (
	errUnknownReplicationTask = serviceerror.NewInternal("unknown replication task")
)

func newReplicatorQueueProcessor(
	shard shard.Context,
	historyCache workflow.Cache,
	executionMgr persistence.ExecutionManager,
	logger log.Logger,
) *replicatorQueueProcessorImpl {

	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	config := shard.GetConfig()

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetMaximumAttempts(10)
	retryPolicy.SetBackoffCoefficient(1)

	return &replicatorQueueProcessorImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		config:             shard.GetConfig(),
		historyCache:       historyCache,
		executionMgr:       executionMgr,
		metricsClient:      shard.GetMetricsClient(),
		logger:             log.With(logger, tag.ComponentReplicatorQueue),
		retryPolicy:        retryPolicy,
		pageSize:           config.ReplicatorProcessorFetchTasksBatchSize(),

		maxTaskID:       nil,
		sanityCheckTime: time.Time{},
	}
}

func (p *replicatorQueueProcessorImpl) NotifyNewTasks(
	tasks []tasks.Task,
) {

	if len(tasks) == 0 {
		return
	}
	maxTaskID := tasks[0].GetTaskID()
	for _, task := range tasks {
		if maxTaskID < task.GetTaskID() {
			maxTaskID = task.GetTaskID()
		}
	}

	p.Lock()
	defer p.Unlock()
	if p.maxTaskID == nil || *p.maxTaskID < maxTaskID {
		p.maxTaskID = &maxTaskID
	}
}

func (p *replicatorQueueProcessorImpl) GetMaxReplicationTaskID() int64 {
	p.Lock()
	defer p.Unlock()

	if p.maxTaskID == nil {
		// maxTaskID is nil before any replication task is written which happens right after shard reload. In that case,
		// use TransferMaxReadLevel which is the max task id of any transfer/visibility/replication queues.
		// TransferMaxReadLevel will be the lower bound of new range_id if shard reload. Remote cluster will quickly (in
		// a few seconds) ack to the latest TransferMaxReadLevel if there is no replication tasks at all.
		return p.shard.GetTransferMaxReadLevel()
	}

	return *p.maxTaskID
}

func (p *replicatorQueueProcessorImpl) paginateTasks(
	ctx context.Context,
	pollingCluster string,
	queryMessageID int64,
) (*replicationspb.ReplicationMessages, error) {

	minTaskID, maxTaskID := p.taskIDsRange(queryMessageID)
	replicationTasks, lastTaskID, err := p.getTasks(
		ctx,
		minTaskID,
		maxTaskID,
		p.pageSize,
	)
	if err != nil {
		return nil, err
	}

	// Note this is a very rough indicator of how much the remote DC is behind on this shard.
	p.metricsClient.Scope(
		metrics.ReplicatorQueueProcessorScope,
		metrics.TargetClusterTag(pollingCluster),
	).RecordDistribution(
		metrics.ReplicationTasksLag,
		int(maxTaskID-lastTaskID),
	)

	p.metricsClient.RecordDistribution(
		metrics.ReplicatorQueueProcessorScope,
		metrics.ReplicationTasksFetched,
		len(replicationTasks),
	)

	p.metricsClient.RecordDistribution(
		metrics.ReplicatorQueueProcessorScope,
		metrics.ReplicationTasksReturned,
		len(replicationTasks),
	)

	return &replicationspb.ReplicationMessages{
		ReplicationTasks:       replicationTasks,
		HasMore:                lastTaskID < maxTaskID,
		LastRetrievedMessageId: lastTaskID,
		SyncShardStatus: &replicationspb.SyncShardStatus{
			StatusTime: timestamp.TimePtr(p.shard.GetTimeSource().Now()),
		},
	}, nil
}

func (p *replicatorQueueProcessorImpl) getTasks(
	ctx context.Context,
	minTaskID int64,
	maxTaskID int64,
	batchSize int,
) ([]*replicationspb.ReplicationTask, int64, error) {

	if minTaskID == maxTaskID {
		return []*replicationspb.ReplicationTask{}, maxTaskID, nil
	}

	var token []byte
	tasks := make([]*replicationspb.ReplicationTask, 0, batchSize)
	for {
		response, err := p.executionMgr.GetReplicationTasks(&persistence.GetReplicationTasksRequest{
			ShardID:       p.shard.GetShardID(),
			MinTaskID:     minTaskID,
			MaxTaskID:     maxTaskID,
			BatchSize:     batchSize,
			NextPageToken: token,
		})
		if err != nil {
			return nil, 0, err
		}

		token = response.NextPageToken
		for _, task := range response.Tasks {
			if replicationTask, err := p.taskInfoToTask(
				ctx,
				task,
			); err != nil {
				return nil, 0, err
			} else if replicationTask != nil {
				tasks = append(tasks, replicationTask)
			}
		}

		// break if seen at least one task or no more task
		if len(token) == 0 || len(tasks) > 0 {
			break
		}
	}

	// sanity check we will finish pagination or return some tasks
	if len(token) != 0 && len(tasks) == 0 {
		p.logger.Fatal("replication task reader should finish pagination or return some tasks")
	}

	if len(tasks) == 0 {
		// len(token) == 0, no more items from DB
		return nil, maxTaskID, nil
	}
	return tasks, tasks[len(tasks)-1].GetSourceTaskId(), nil
}

func (p *replicatorQueueProcessorImpl) getTask(
	ctx context.Context,
	taskInfo *replicationspb.ReplicationTaskInfo,
) (*replicationspb.ReplicationTask, error) {

	switch taskInfo.TaskType {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return p.taskInfoToTask(ctx, &tasks.SyncActivityTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0), // TODO add the missing attribute to proto definition
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			ScheduledID:         taskInfo.ScheduledId,
		})
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return p.taskInfoToTask(ctx, &tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0), // TODO add the missing attribute to proto definition
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			FirstEventID:        taskInfo.FirstEventId,
			NextEventID:         taskInfo.NextEventId,
		})
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown replication task type: %v", taskInfo.TaskType))
	}
}

func (p *replicatorQueueProcessorImpl) taskInfoToTask(
	ctx context.Context,
	task tasks.Task,
) (*replicationspb.ReplicationTask, error) {
	var replicationTask *replicationspb.ReplicationTask
	op := func() error {
		var err error
		replicationTask, err = p.toReplicationTask(ctx, task)
		return err
	}

	if err := backoff.Retry(op, p.retryPolicy, common.IsPersistenceTransientError); err != nil {
		return nil, err
	}
	return replicationTask, nil
}

func (p *replicatorQueueProcessorImpl) taskIDsRange(
	lastReadMessageID int64,
) (minTaskID int64, maxTaskID int64) {
	minTaskID = lastReadMessageID
	maxTaskID = p.shard.GetTransferMaxReadLevel()

	p.Lock()
	defer p.Unlock()
	defer func() { p.maxTaskID = convert.Int64Ptr(maxTaskID) }()

	now := p.shard.GetTimeSource().Now()
	if p.sanityCheckTime.IsZero() || p.sanityCheckTime.Before(now) {
		p.sanityCheckTime = now.Add(backoff.JitDuration(
			p.config.ReplicatorProcessorMaxPollInterval(),
			p.config.ReplicatorProcessorMaxPollIntervalJitterCoefficient(),
		))
		return minTaskID, maxTaskID
	}

	if p.maxTaskID != nil && *p.maxTaskID < maxTaskID {
		maxTaskID = *p.maxTaskID
	}

	return minTaskID, maxTaskID
}

func (p *replicatorQueueProcessorImpl) toReplicationTask(
	ctx context.Context,
	task tasks.Task,
) (*replicationspb.ReplicationTask, error) {

	switch task := task.(type) {
	case *tasks.SyncActivityTask:
		return p.generateSyncActivityTask(ctx, task)

	case *tasks.HistoryReplicationTask:
		return p.generateHistoryReplicationTask(ctx, task)

	default:
		return nil, errUnknownReplicationTask
	}
}

func (p *replicatorQueueProcessorImpl) generateSyncActivityTask(
	ctx context.Context,
	taskInfo *tasks.SyncActivityTask,
) (*replicationspb.ReplicationTask, error) {
	namespaceID := namespace.ID(taskInfo.NamespaceID)
	workflowID := taskInfo.WorkflowID
	runID := taskInfo.RunID
	taskID := taskInfo.TaskID
	return p.processReplication(
		ctx,
		false, // not necessary to send out sync activity task if workflow closed
		namespaceID,
		workflowID,
		runID,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			activityInfo, ok := mutableState.GetActivityInfo(taskInfo.ScheduledID)
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
						NamespaceId:        namespaceID.String(),
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
				VisibilityTime: &taskInfo.VisibilityTimestamp,
			}, nil
		},
	)
}

func (p *replicatorQueueProcessorImpl) generateHistoryReplicationTask(
	ctx context.Context,
	taskInfo *tasks.HistoryReplicationTask,
) (*replicationspb.ReplicationTask, error) {
	namespaceID := namespace.ID(taskInfo.NamespaceID)
	workflowID := taskInfo.WorkflowID
	runID := taskInfo.RunID
	taskID := taskInfo.TaskID
	return p.processReplication(
		ctx,
		true, // still necessary to send out history replication message if workflow closed
		namespaceID,
		workflowID,
		runID,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			versionHistoryItems, branchToken, err := p.getVersionHistoryItems(
				mutableState,
				taskInfo.FirstEventID,
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
				taskInfo.FirstEventID,
				taskInfo.NextEventID,
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
						NamespaceId:         namespaceID.String(),
						WorkflowId:          workflowID,
						RunId:               runID,
						VersionHistoryItems: versionHistoryItems,
						Events:              eventsBlob,
						NewRunEvents:        newRunEventsBlob,
					},
				},
				VisibilityTime: &taskInfo.VisibilityTimestamp,
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
		resp, err := p.executionMgr.ReadRawHistoryBranch(req)
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
		return nil, serviceerror.NewInternal("replicatorQueueProcessor encountered more than 1 NDC raw event batch")
	}

	return eventBatchBlobs[0], nil
}

func (p *replicatorQueueProcessorImpl) getVersionHistoryItems(
	mutableState workflow.MutableState,
	eventID int64,
	version int64,
) ([]*historyspb.VersionHistoryItem, []byte, error) {

	versionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
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
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	action func(workflow.MutableState) (*replicationspb.ReplicationTask, error),
) (retReplicationTask *replicationspb.ReplicationTask, retError error) {

	execution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}

	context, release, err := p.historyCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeAPI,
	)
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
		return action(msBuilder)
	case *serviceerror.NotFound:
		return nil, nil
	default:
		return nil, err
	}
}
