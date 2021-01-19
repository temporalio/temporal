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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	standbyActionFn     func(workflowExecutionContext, mutableState) (interface{}, error)
	standbyPostActionFn func(queueTaskInfo, interface{}, log.Logger) error

	standbyCurrentTimeFn func() time.Time
)

func standbyTaskPostActionNoOp(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	// return error so task processing logic will retry
	return ErrTaskRetry
}

func standbyTransferTaskPostActionTaskDiscarded(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	transferTask := taskInfo.(*persistencespb.TransferTaskInfo)
	logger.Error("Discarding standby transfer task due to task being pending for too long.",
		tag.WorkflowID(transferTask.GetWorkflowId()),
		tag.WorkflowRunID(transferTask.GetRunId()),
		tag.WorkflowNamespaceID(transferTask.GetNamespaceId()),
		tag.TaskID(transferTask.GetTaskId()),
		tag.TaskType(transferTask.TaskType),
		tag.FailoverVersion(transferTask.GetVersion()),
		tag.TimestampPtr(transferTask.VisibilityTime),
		tag.WorkflowEventID(transferTask.GetScheduleId()))
	return ErrTaskDiscarded
}

func standbyTimerTaskPostActionTaskDiscarded(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	timerTask := taskInfo.(*persistencespb.TimerTaskInfo)
	logger.Error("Discarding standby timer task due to task being pending for too long.",
		tag.WorkflowID(timerTask.GetWorkflowId()),
		tag.WorkflowRunID(timerTask.GetRunId()),
		tag.WorkflowNamespaceID(timerTask.GetNamespaceId()),
		tag.TaskID(timerTask.GetTaskId()),
		tag.TaskType(timerTask.TaskType),
		tag.WorkflowTimeoutType(timerTask.TimeoutType),
		tag.FailoverVersion(timerTask.GetVersion()),
		tag.TimestampPtr(timerTask.VisibilityTime),
		tag.WorkflowEventID(timerTask.GetEventId()))
	return ErrTaskDiscarded
}

type (
	historyResendInfo struct {

		// used by NDC
		lastEventID      int64
		lastEventVersion int64
	}

	pushActivityTaskToMatchingInfo struct {
		activityTaskScheduleToStartTimeout time.Duration
	}

	pushWorkflowTaskToMatchingInfo struct {
		workflowTaskScheduleToStartTimeout int64
		taskqueue                          taskqueuepb.TaskQueue
	}
)

func newHistoryResendInfo(
	lastEventID int64,
	lastEventVersion int64,
) *historyResendInfo {
	return &historyResendInfo{
		lastEventID:      lastEventID,
		lastEventVersion: lastEventVersion,
	}
}

func newPushActivityToMatchingInfo(
	activityScheduleToStartTimeout time.Duration,
) *pushActivityTaskToMatchingInfo {

	return &pushActivityTaskToMatchingInfo{
		activityTaskScheduleToStartTimeout: activityScheduleToStartTimeout,
	}
}

func newPushWorkflowTaskToMatchingInfo(
	workflowTaskScheduleToStartTimeout int64,
	taskqueue taskqueuepb.TaskQueue,
) *pushWorkflowTaskToMatchingInfo {

	return &pushWorkflowTaskToMatchingInfo{
		workflowTaskScheduleToStartTimeout: workflowTaskScheduleToStartTimeout,
		taskqueue:                          taskqueue,
	}
}

func getHistoryResendInfo(
	mutableState mutableState,
) (*historyResendInfo, error) {

	currentBranch, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().GetVersionHistories())
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(currentBranch)
	if err != nil {
		return nil, err
	}
	return newHistoryResendInfo(lastItem.GetEventId(), lastItem.GetVersion()), nil
}

func getStandbyPostActionFn(
	taskInfo queueTaskInfo,
	standbyNow standbyCurrentTimeFn,
	standbyTaskMissingEventsResendDelay time.Duration,
	standbyTaskMissingEventsDiscardDelay time.Duration,
	fetchHistoryStandbyPostActionFn standbyPostActionFn,
	discardTaskStandbyPostActionFn standbyPostActionFn,
) standbyPostActionFn {

	// this is for task retry, use machine time
	now := standbyNow()
	taskTime := timestamp.TimeValue(taskInfo.GetVisibilityTime())
	resendTime := taskTime.Add(standbyTaskMissingEventsResendDelay)
	discardTime := taskTime.Add(standbyTaskMissingEventsDiscardDelay)

	// now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(resendTime) {
		return standbyTaskPostActionNoOp
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(discardTime) {
		return fetchHistoryStandbyPostActionFn
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now
	return discardTaskStandbyPostActionFn
}

func refreshTasks(
	adminClient admin.Client,
	namespaceCache cache.NamespaceCache,
	namespaceID string,
	workflowID string,
	runID string,
) error {
	namespaceEntry, err := namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), refreshTaskTimeout)
	defer cancel()

	_, err = adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		Namespace: namespaceEntry.GetInfo().Name,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	return err
}
