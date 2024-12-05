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
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	standbyActionFn     func(context.Context, workflow.Context, workflow.MutableState) (interface{}, error)
	standbyPostActionFn func(context.Context, tasks.Task, interface{}, log.Logger) error

	standbyCurrentTimeFn func() time.Time
)

func standbyTaskPostActionNoOp(
	_ context.Context,
	_ tasks.Task,
	postActionInfo interface{},
	_ log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	if err, ok := postActionInfo.(error); ok {
		return err
	}

	// return error so task processing logic will retry
	return consts.ErrTaskRetry
}

func standbyTransferTaskPostActionTaskDiscarded(
	_ context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	logger.Warn("Discarding standby transfer task due to task being pending for too long.", tag.Task(taskInfo))
	return consts.ErrTaskDiscarded
}

func standbyTimerTaskPostActionTaskDiscarded(
	_ context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	logger.Warn("Discarding standby timer task due to task being pending for too long.", tag.Task(taskInfo))
	return consts.ErrTaskDiscarded
}

func isWorkflowExistOnSource(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	logger log.Logger,
	currentCluster string,
	clientBean client.Bean,
	registry namespace.Registry,
) bool {
	remoteClusterName, err := getSourceClusterName(
		currentCluster,
		registry,
		workflowKey.GetNamespaceID(),
	)
	if err != nil {
		return true
	}
	_, remoteAdminClient, err := clientBean.GetRemoteFrontendClient(remoteClusterName)
	if err != nil {
		return true
	}
	_, err = remoteAdminClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: workflowKey.GetNamespaceID(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowKey.GetWorkflowID(),
			RunId:      workflowKey.GetRunID(),
		},
	})
	if err != nil {
		if common.IsNotFoundError(err) {
			return false
		}
		logger.Error("Error describe mutable state from remote.",
			tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
			tag.WorkflowID(workflowKey.GetWorkflowID()),
			tag.WorkflowRunID(workflowKey.GetRunID()),
			tag.ClusterName(remoteClusterName),
			tag.Error(err))
	}
	return true
}

type (
	executionTimerPostActionInfo struct {
		currentRunID string
	}

	activityTaskPostActionInfo struct {
		taskQueue                          string
		activityTaskScheduleToStartTimeout time.Duration
		versionDirective                   *taskqueuespb.TaskVersionDirective
	}

	verifyCompletionRecordedPostActionInfo struct {
		parentWorkflowKey *definition.WorkflowKey
	}

	workflowTaskPostActionInfo struct {
		workflowTaskScheduleToStartTimeout time.Duration
		taskqueue                          *taskqueuepb.TaskQueue
		versionDirective                   *taskqueuespb.TaskVersionDirective
	}
)

func newExecutionTimerPostActionInfo(
	mutableState workflow.MutableState,
) (*executionTimerPostActionInfo, error) {
	return &executionTimerPostActionInfo{
		currentRunID: mutableState.GetExecutionState().RunId,
	}, nil
}

func newActivityTaskPostActionInfo(
	mutableState workflow.MutableState,
	activityInfo *persistencespb.ActivityInfo,
) (*activityTaskPostActionInfo, error) {
	directive := MakeDirectiveForActivityTask(mutableState, activityInfo)

	return &activityTaskPostActionInfo{
		activityTaskScheduleToStartTimeout: activityInfo.ScheduleToStartTimeout.AsDuration(),
		versionDirective:                   directive,
	}, nil
}

func newActivityRetryTimePostActionInfo(
	mutableState workflow.MutableState,
	taskQueue string,
	activityScheduleToStartTimeout time.Duration,
	activityInfo *persistencespb.ActivityInfo,
) (*activityTaskPostActionInfo, error) {
	directive := MakeDirectiveForActivityTask(mutableState, activityInfo)

	return &activityTaskPostActionInfo{
		taskQueue:                          taskQueue,
		activityTaskScheduleToStartTimeout: activityScheduleToStartTimeout,
		versionDirective:                   directive,
	}, nil
}

func newWorkflowTaskPostActionInfo(
	mutableState workflow.MutableState,
	workflowTaskScheduleToStartTimeout time.Duration,
	taskqueue *taskqueuepb.TaskQueue,
) (*workflowTaskPostActionInfo, error) {
	directive := MakeDirectiveForWorkflowTask(mutableState)

	return &workflowTaskPostActionInfo{
		workflowTaskScheduleToStartTimeout: workflowTaskScheduleToStartTimeout,
		taskqueue:                          taskqueue,
		versionDirective:                   directive,
	}, nil
}

func getStandbyPostActionFn(
	taskInfo tasks.Task,
	standbyNow standbyCurrentTimeFn,
	standbyTaskMissingEventsDiscardDelay time.Duration,
	discardTaskStandbyPostActionFn standbyPostActionFn,
) standbyPostActionFn {

	// this is for task retry, use machine time
	now := standbyNow()
	taskTime := taskInfo.GetVisibilityTime()
	discardTime := taskTime.Add(standbyTaskMissingEventsDiscardDelay)

	// now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(discardTime) {
		return standbyTaskPostActionNoOp
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now
	return discardTaskStandbyPostActionFn
}

func getSourceClusterName(
	currentCluster string,
	registry namespace.Registry,
	namespaceID string,
) (string, error) {
	namespaceEntry, err := registry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return "", err
	}

	remoteClusterName := namespaceEntry.ActiveClusterName()
	if remoteClusterName == currentCluster {
		// namespace has turned active, retry the task
		return "", errors.New("namespace becomes active when processing task as standby")
	}
	return remoteClusterName, nil
}
