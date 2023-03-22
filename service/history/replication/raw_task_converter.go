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

package replication

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

func convertActivityStateReplicationTask(
	ctx context.Context,
	taskInfo *tasks.SyncActivityTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		false, // not necessary to send out sync activity task if workflow closed
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		workflowCache,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			activityInfo, ok := mutableState.GetActivityInfo(taskInfo.ScheduledEventID)
			if !ok {
				return nil, nil
			}
			if activityInfo.Version != taskInfo.Version {
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
				SourceTaskId: taskInfo.TaskID,
				Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
					SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
						NamespaceId:        taskInfo.NamespaceID,
						WorkflowId:         taskInfo.WorkflowID,
						RunId:              taskInfo.RunID,
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

func convertWorkflowStateReplicationTask(
	ctx context.Context,
	taskInfo *tasks.SyncWorkflowStateTask,
	workflowCache wcache.Cache,
) (*replicationspb.ReplicationTask, error) {
	return generateStateReplicationTask(
		ctx,
		true,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		workflowCache,
		func(mutableState workflow.MutableState) (*replicationspb.ReplicationTask, error) {
			state, _ := mutableState.GetWorkflowStateStatus()
			if state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
				return nil, nil
			}
			return &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
				SourceTaskId: taskInfo.TaskID,
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

func convertHistoryReplicationTask(
	ctx context.Context,
	taskInfo *tasks.HistoryReplicationTask,
	shardID int32,
	workflowCache wcache.Cache,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
) (*replicationspb.ReplicationTask, error) {
	currentVersionHistory, currentEvents, err := getVersionHistoryAndEvents(
		ctx,
		shardID,
		definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.RunID),
		taskInfo.Version,
		taskInfo.FirstEventID,
		taskInfo.NextEventID,
		workflowCache,
		executionManager,
		logger,
	)
	if err != nil {
		return nil, err
	}
	if currentVersionHistory == nil {
		return nil, nil
	}
	var newEvents *commonpb.DataBlob
	if len(taskInfo.NewRunID) != 0 {
		newVersionHistory, newEventBlob, err := getVersionHistoryAndEvents(
			ctx,
			shardID,
			definition.NewWorkflowKey(taskInfo.NamespaceID, taskInfo.WorkflowID, taskInfo.NewRunID),
			taskInfo.Version,
			common.FirstEventID,
			common.FirstEventID+1,
			workflowCache,
			executionManager,
			logger,
		)
		if err != nil {
			return nil, err
		}
		if newVersionHistory != nil {
			newEvents = newEventBlob
		}
	}

	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		SourceTaskId: taskInfo.TaskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         taskInfo.NamespaceID,
				WorkflowId:          taskInfo.WorkflowID,
				RunId:               taskInfo.RunID,
				VersionHistoryItems: currentVersionHistory,
				Events:              currentEvents,
				NewRunEvents:        newEvents,
			},
		},
		VisibilityTime: &taskInfo.VisibilityTimestamp,
	}, nil
}

func generateStateReplicationTask(
	ctx context.Context,
	processTaskIfClosed bool,
	workflowKey definition.WorkflowKey,
	workflowCache wcache.Cache,
	action func(workflow.MutableState) (*replicationspb.ReplicationTask, error),
) (retReplicationTask *replicationspb.ReplicationTask, retError error) {
	wfContext, release, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		workflow.CallerTypeTask,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	ms, err := wfContext.LoadMutableState(ctx)
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

func getVersionHistoryAndEvents(
	ctx context.Context,
	shardID int32,
	workflowKey definition.WorkflowKey,
	eventVersion int64,
	firstEventID int64,
	nextEventID int64,
	workflowCache wcache.Cache,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
) ([]*historyspb.VersionHistoryItem, *commonpb.DataBlob, error) {
	versionHistory, branchToken, err := getBranchToken(
		ctx,
		workflowKey,
		workflowCache,
		firstEventID,
		eventVersion,
	)
	if err != nil {
		return nil, nil, err
	}
	if versionHistory == nil {
		return nil, nil, nil
	}
	events, err := getEventsBlob(ctx, shardID, branchToken, firstEventID, nextEventID, executionManager)
	if err != nil {
		return nil, nil, convertGetHistoryError(workflowKey, logger, err)
	}
	return versionHistory, events, nil
}

func getBranchToken(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	workflowCache wcache.Cache,
	eventID int64,
	eventVersion int64,
) (_ []*historyspb.VersionHistoryItem, _ []byte, retError error) {
	wfContext, release, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		workflow.CallerTypeTask,
	)
	if err != nil {
		return nil, nil, err
	}
	defer func() { release(retError) }()

	ms, err := wfContext.LoadMutableState(ctx)
	switch err.(type) {
	case nil:
		return getVersionHistoryItems(ms, eventID, eventVersion)
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		return nil, nil, nil
	default:
		return nil, nil, err
	}
}

func getEventsBlob(
	ctx context.Context,
	shardID int32,
	branchToken []byte,
	firstEventID int64,
	nextEventID int64,
	executionManager persistence.ExecutionManager,
) (*commonpb.DataBlob, error) {

	var eventBatchBlobs []*commonpb.DataBlob
	var pageToken []byte
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}

	for {
		resp, err := executionManager.ReadRawHistoryBranch(ctx, req)
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

func convertGetHistoryError(
	workflowKey definition.WorkflowKey,
	logger log.Logger,
	err error,
) error {
	switch err.(type) {
	case *serviceerror.NotFound:
		// bypass this corrupted workflow to unblock the replication queue.
		logger.Error("Cannot get history from missing workflow",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return nil
	case *serviceerror.DataLoss:
		// bypass this corrupted workflow to unblock the replication queue.
		logger.Error("Cannot get history from corrupted workflow",
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
			tag.Error(err))
		return nil
	default:
		return err
	}
}
