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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination ack_manager_mock.go

package replication

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
	AckManager interface {
		NotifyNewTasks(tasks []tasks.Task)
		GetMaxTaskInfo() (int64, time.Time)
		GetTasks(ctx context.Context, pollingCluster string, queryMessageID int64) (*replicationspb.ReplicationMessages, error)
		GetTask(ctx context.Context, taskInfo *replicationspb.ReplicationTaskInfo) (*replicationspb.ReplicationTask, error)
	}

	ackMgrImpl struct {
		currentClusterName string
		shard              shard.Context
		config             *configs.Config
		workflowCache      workflow.Cache
		executionMgr       persistence.ExecutionManager
		metricsClient      metrics.Client
		logger             log.Logger
		retryPolicy        backoff.RetryPolicy
		pageSize           int

		sync.Mutex
		// largest replication task ID generated
		maxTaskID                  *int64
		maxTaskVisibilityTimestamp *time.Time
		sanityCheckTime            time.Time
	}
)

var (
	errUnknownReplicationTask = serviceerror.NewInternal("unknown replication task")
)

func NewAckManager(
	shard shard.Context,
	workflowCache workflow.Cache,
	executionMgr persistence.ExecutionManager,
	logger log.Logger,
) AckManager {

	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	config := shard.GetConfig()

	retryPolicy := backoff.NewExponentialRetryPolicy(200 * time.Millisecond).
		WithMaximumAttempts(5).
		WithBackoffCoefficient(1)

	return &ackMgrImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		config:             shard.GetConfig(),
		workflowCache:      workflowCache,
		executionMgr:       executionMgr,
		metricsClient:      shard.GetMetricsClient(),
		logger:             log.With(logger, tag.ComponentReplicatorQueue),
		retryPolicy:        retryPolicy,
		pageSize:           config.ReplicatorProcessorFetchTasksBatchSize(),

		maxTaskID:       nil,
		sanityCheckTime: time.Time{},
	}
}

func (p *ackMgrImpl) NotifyNewTasks(
	tasks []tasks.Task,
) {

	if len(tasks) == 0 {
		return
	}
	maxTaskID := tasks[0].GetTaskID()
	maxVisibilityTimestamp := tasks[0].GetVisibilityTime()
	for _, task := range tasks {
		if maxTaskID < task.GetTaskID() {
			maxTaskID = task.GetTaskID()
		}
		if maxVisibilityTimestamp.Before(task.GetVisibilityTime()) {
			maxVisibilityTimestamp = task.GetVisibilityTime()
		}
	}

	p.Lock()
	defer p.Unlock()
	if p.maxTaskID == nil || *p.maxTaskID < maxTaskID {
		p.maxTaskID = &maxTaskID
	}
	if p.maxTaskVisibilityTimestamp == nil || p.maxTaskVisibilityTimestamp.Before(maxVisibilityTimestamp) {
		p.maxTaskVisibilityTimestamp = timestamp.TimePtr(maxVisibilityTimestamp)
	}
}

func (p *ackMgrImpl) GetMaxTaskInfo() (int64, time.Time) {
	p.Lock()
	defer p.Unlock()

	maxTaskID := p.maxTaskID
	if maxTaskID == nil {
		// maxTaskID is nil before any replication task is written which happens right after shard reload. In that case,
		// use ImmediateTaskMaxReadLevel which is the max task id of any immediate task queues.
		// ImmediateTaskMaxReadLevel will be the lower bound of new range_id if shard reload. Remote cluster will quickly (in
		// a few seconds) ack to the latest ImmediateTaskMaxReadLevel if there is no replication tasks at all.
		taskID := p.shard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, p.currentClusterName, false).Prev().TaskID
		maxTaskID = &taskID
	}
	maxVisibilityTimestamp := p.maxTaskVisibilityTimestamp
	if maxVisibilityTimestamp == nil {
		maxVisibilityTimestamp = timestamp.TimePtr(p.shard.GetTimeSource().Now())
	}

	return *maxTaskID, timestamp.TimeValue(maxVisibilityTimestamp)
}

func (p *ackMgrImpl) GetTask(
	ctx context.Context,
	taskInfo *replicationspb.ReplicationTaskInfo,
) (*replicationspb.ReplicationTask, error) {

	switch taskInfo.TaskType {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return p.toReplicationTask(ctx, &tasks.SyncActivityTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0), // TODO add the missing attribute to proto definition
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			ScheduledEventID:    taskInfo.ScheduledEventId,
		})
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return p.toReplicationTask(ctx, &tasks.HistoryReplicationTask{
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
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE:
		return p.toReplicationTask(ctx, &tasks.SyncWorkflowStateTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0),
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
		})
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown replication task type: %v", taskInfo.TaskType))
	}
}

func (p *ackMgrImpl) GetTasks(
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

func (p *ackMgrImpl) getTasks(
	ctx context.Context,
	minTaskID int64,
	maxTaskID int64,
	batchSize int,
) ([]*replicationspb.ReplicationTask, int64, error) {

	if minTaskID > maxTaskID {
		return nil, 0, serviceerror.NewUnavailable("min task ID < max task ID, probably due to shard re-balancing")
	} else if minTaskID == maxTaskID {
		return []*replicationspb.ReplicationTask{}, maxTaskID, nil
	}

	var token []byte
	replicationTasks := make([]*replicationspb.ReplicationTask, 0, batchSize)
	for {
		response, err := p.executionMgr.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
			ShardID:             p.shard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
			BatchSize:           batchSize,
			NextPageToken:       token,
		})
		if err != nil {
			return nil, 0, err
		}

		token = response.NextPageToken
		for _, task := range response.Tasks {
			if replicationTask, err := p.toReplicationTask(ctx, task); err != nil {
				return nil, 0, err
			} else if replicationTask != nil {
				replicationTasks = append(replicationTasks, replicationTask)
			}
		}

		// break if seen at least one task or no more task
		if len(token) == 0 || len(replicationTasks) > 0 {
			break
		}
	}

	// sanity check we will finish pagination or return some tasks
	if len(token) != 0 && len(replicationTasks) == 0 {
		p.logger.Fatal("replication task reader should finish pagination or return some tasks")
	}

	if len(replicationTasks) == 0 {
		// len(token) == 0, no more items from DB
		return nil, maxTaskID, nil
	}
	return replicationTasks, replicationTasks[len(replicationTasks)-1].GetSourceTaskId(), nil
}

func (p *ackMgrImpl) taskIDsRange(
	lastReadMessageID int64,
) (minTaskID int64, maxTaskID int64) {
	minTaskID = lastReadMessageID
	maxTaskID = p.shard.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication, p.currentClusterName, false).Prev().TaskID

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

func (p *ackMgrImpl) toReplicationTask(
	ctx context.Context,
	task tasks.Task,
) (*replicationspb.ReplicationTask, error) {

	switch task := task.(type) {
	case *tasks.SyncActivityTask:
		return p.generateSyncActivityTask(ctx, task)
	case *tasks.HistoryReplicationTask:
		return p.generateHistoryReplicationTask(ctx, task)
	case *tasks.SyncWorkflowStateTask:
		return p.generateSyncWorkflowStateTask(ctx, task)
	default:
		return nil, errUnknownReplicationTask
	}
}

func (p *ackMgrImpl) generateSyncActivityTask(
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
			activityInfo, ok := mutableState.GetActivityInfo(taskInfo.ScheduledEventID)
			if !ok {
				return nil, nil
			}

			// The activity may be in a scheduled state
			var startedTime *time.Time
			if activityInfo.StartedEventId != common.EmptyEventID {
				startedTime = activityInfo.StartedTime
			}

			// Version history uses when replicate the sync activity task
			versionHistories := mutableState.GetExecutionInfo().GetVersionHistories()
			currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
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
						ScheduledEventId:   activityInfo.ScheduledEventId,
						ScheduledTime:      activityInfo.ScheduledTime,
						StartedEventId:     activityInfo.StartedEventId,
						StartedTime:        startedTime,
						LastHeartbeatTime:  activityInfo.LastHeartbeatUpdateTime,
						Details:            activityInfo.LastHeartbeatDetails,
						Attempt:            activityInfo.Attempt,
						LastFailure:        activityInfo.RetryLastFailure,
						LastWorkerIdentity: activityInfo.RetryLastWorkerIdentity,
						VersionHistory:     versionhistory.CopyVersionHistory(currentVersionHistory),
					},
				},
				VisibilityTime: &taskInfo.VisibilityTimestamp,
			}, nil
		},
	)
}

func (p *ackMgrImpl) generateHistoryReplicationTask(
	ctx context.Context,
	taskInfo *tasks.HistoryReplicationTask,
) (*replicationspb.ReplicationTask, error) {
	namespaceID := namespace.ID(taskInfo.NamespaceID)
	workflowID := taskInfo.WorkflowID
	runID := taskInfo.RunID
	taskID := taskInfo.TaskID
	replicationTask, err := p.processReplication(
		ctx,
		true, // still necessary to send out history replication message if workflow closed
		namespaceID,
		workflowID,
		runID,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			versionHistoryItems, branchToken, err := getVersionHistoryItems(
				mutableState,
				taskInfo.FirstEventID,
				taskInfo.Version,
			)
			if err != nil {
				return nil, err
			}

			eventsBlob, err := p.getEventsBlob(
				ctx,
				branchToken,
				taskInfo.FirstEventID,
				taskInfo.NextEventID,
			)
			if err != nil {
				return nil, err
			}

			replicationTask := &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
				SourceTaskId: taskID,
				Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
					HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
						NamespaceId:         namespaceID.String(),
						WorkflowId:          workflowID,
						RunId:               runID,
						VersionHistoryItems: versionHistoryItems,
						Events:              eventsBlob,
						// NewRunEvents will be set in processNewRunReplication
					},
				},
				VisibilityTime: &taskInfo.VisibilityTimestamp,
			}
			return replicationTask, nil
		},
	)
	if err != nil {
		return replicationTask, err
	}
	return p.processNewRunReplication(
		ctx,
		namespaceID,
		workflowID,
		taskInfo.NewRunID,
		taskInfo.NewRunBranchToken,
		taskInfo.Version,
		replicationTask,
	)
}

func (p *ackMgrImpl) generateSyncWorkflowStateTask(
	ctx context.Context,
	taskInfo *tasks.SyncWorkflowStateTask,
) (*replicationspb.ReplicationTask, error) {
	namespaceID := namespace.ID(taskInfo.NamespaceID)
	workflowID := taskInfo.WorkflowID
	runID := taskInfo.RunID
	taskID := taskInfo.TaskID
	return p.processReplication(
		ctx,
		true,
		namespaceID,
		workflowID,
		runID,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			return &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
				SourceTaskId: taskID,
				Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
					SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
						WorkflowState: mutableState.CloneToProto(),
					},
				},
				VisibilityTime: &taskInfo.VisibilityTimestamp,
			}, nil
		},
	)
}

func (p *ackMgrImpl) getEventsBlob(
	ctx context.Context,
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
		resp, err := p.executionMgr.ReadRawHistoryBranch(ctx, req)
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

func (p *ackMgrImpl) processReplication(
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

	context, release, err := p.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	ms, err := context.LoadMutableState(ctx)
	switch err.(type) {
	case nil:
		if !processTaskIfClosed && !ms.IsWorkflowExecutionRunning() {
			// workflow already finished, no need to process the replication task
			return nil, nil
		}
		return action(ms)
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

func (p *ackMgrImpl) processNewRunReplication(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	newRunID string,
	branchToken []byte,
	taskVersion int64,
	task *replicationspb.ReplicationTask,
) (retReplicationTask *replicationspb.ReplicationTask, retError error) {

	if task == nil {
		return nil, nil
	}
	attr, ok := task.Attributes.(*replicationspb.ReplicationTask_HistoryTaskAttributes)
	if !ok {
		return nil, serviceerror.NewInternal("Wrong replication task to process new run replication.")
	}

	var newRunBranchToken []byte
	if len(newRunID) > 0 {
		newRunContext, releaseFn, err := p.workflowCache.GetOrCreateWorkflowExecution(
			ctx,
			namespaceID,
			commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      newRunID,
			},
			workflow.CallerTypeTask,
		)
		if err != nil {
			return nil, err
		}
		defer func() { releaseFn(retError) }()

		newRunMutableState, err := newRunContext.LoadMutableState(ctx)
		if err != nil {
			return nil, err
		}
		_, newRunBranchToken, err = getVersionHistoryItems(
			newRunMutableState,
			common.FirstEventID,
			taskVersion,
		)
		if err != nil {
			return nil, err
		}
	} else if len(branchToken) != 0 {
		newRunBranchToken = branchToken
	}

	var newRunEventsBlob *commonpb.DataBlob
	if len(newRunBranchToken) > 0 {
		// only get the first batch
		var err error
		newRunEventsBlob, err = p.getEventsBlob(
			ctx,
			newRunBranchToken,
			common.FirstEventID,
			common.FirstEventID+1,
		)
		if err != nil {
			return nil, err
		}
	}
	attr.HistoryTaskAttributes.NewRunEvents = newRunEventsBlob
	return task, nil
}

func getVersionHistoryItems(
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

	versionHistoryBranch, err := versionhistory.GetVersionHistory(versionHistories, versionHistoryIndex)
	if err != nil {
		return nil, nil, err
	}
	return versionhistory.CopyVersionHistory(versionHistoryBranch).GetItems(), versionHistoryBranch.GetBranchToken(), nil
}
