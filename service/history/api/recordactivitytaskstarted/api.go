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

package recordactivitytaskstarted

import (
	"context"
	"errors"
	"fmt"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
) (resp *historyservice.RecordActivityTaskStartedResponse, retError error) {

	var err error
	response := &historyservice.RecordActivityTaskStartedResponse{}
	var startedTransition bool

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		request.Clock,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.WorkflowExecution.WorkflowId,
			request.WorkflowExecution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			response, startedTransition, err = recordActivityTaskStarted(ctx, shardContext, mutableState, request, matchingClient)
			if err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{
				Noop: false,
				// Create new wft if a transition started with this activity.
				// StartDeploymentTransition rescheduled pending wft, but this creates new
				// one if there is no pending wft.
				CreateWorkflowTask: startedTransition,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	if startedTransition {
		// Rejecting the activity start because the workflow is now in transition. Matching can drop
		// the task, new activity task will be scheduled after transition completion.
		return nil, serviceerrors.NewActivityStartDuringTransition()
	}
	return response, err
}

func recordActivityTaskStarted(
	ctx context.Context,
	shardContext shard.Context,
	mutableState workflow.MutableState,
	request *historyservice.RecordActivityTaskStartedRequest,
	matchingClient matchingservice.MatchingServiceClient,
) (*historyservice.RecordActivityTaskStartedResponse, bool, error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, false, err
	}
	namespaceName := namespaceEntry.Name().String()

	scheduledEventID := request.GetScheduledEventId()
	requestID := request.GetRequestId()
	ai, isRunning := mutableState.GetActivityInfo(scheduledEventID)

	taggedMetrics := shardContext.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.HistoryRecordActivityTaskStartedScope))

	// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
	// some extreme cassandra failure cases.
	if !isRunning && scheduledEventID >= mutableState.GetNextEventID() {
		metrics.StaleMutableStateCounter.With(taggedMetrics).Record(1)
		return nil, false, consts.ErrStaleState
	}

	// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
	// task is not outstanding than it is most probably a duplicate and complete the task.
	if !isRunning {
		// Looks like ActivityTask already completed as a result of another call.
		// It is OK to drop the task at this point.
		return nil, false, consts.ErrActivityTaskNotFound
	}

	scheduledEvent, err := mutableState.GetActivityScheduledEvent(ctx, scheduledEventID)
	if err != nil {
		return nil, false, err
	}

	response := &historyservice.RecordActivityTaskStartedResponse{
		ScheduledEvent:              scheduledEvent,
		CurrentAttemptScheduledTime: ai.ScheduledTime,
	}

	if ai.StartedEventId != common.EmptyEventID {
		// If activity is started as part of the current request scope then return a positive response
		if ai.RequestId == requestID {
			response.StartedTime = ai.StartedTime
			response.Attempt = ai.Attempt
			return response, false, nil
		}

		// Looks like ActivityTask already started as a result of another call.
		// It is OK to drop the task at this point.
		return nil, false, serviceerrors.NewTaskAlreadyStarted("Activity")
	}

	if ai.Stamp != request.Stamp {
		// activity has changes before task is started.
		// ErrActivityStampMismatch is the error to indicate that requested activity has mismatched stamp
		errorMessage := fmt.Sprintf(
			"Activity task with this stamp not found. Id: %s,: type: %s, current stamp: %d",
			ai.ActivityId, ai.ActivityType.Name, ai.Stamp)
		return nil, false, serviceerror.NewNotFound(errorMessage)
	}

	// TODO (shahab): support independent activities. Independent activities do not need all
	// the deployment validations and do not start workflow transition.

	wfDeployment := mutableState.GetEffectiveDeployment()
	pollerDeployment := worker_versioning.DeploymentFromCapabilities(request.PollRequest.WorkerVersionCapabilities)
	// Effective deployment of the workflow when History scheduled the WFT.
	scheduledDeployment := request.GetScheduledDeployment()
	if scheduledDeployment == nil {
		// Matching does not send the directive deployment when it's the same as poller's.
		scheduledDeployment = pollerDeployment
	}
	if !scheduledDeployment.Equal(wfDeployment) {
		// This must be an AT scheduled before the workflow transitions to the current
		// deployment. Matching can drop it.
		return nil, false, serviceerrors.NewObsoleteMatchingTask("wrong directive deployment")
	}

	if mutableState.GetDeploymentTransition() != nil {
		// Can't start activity during a redirect. We reject this request so Matching drops
		// the task. The activity will be rescheduled when the redirect completes/fails.
		return nil, false, serviceerrors.NewActivityStartDuringTransition()
	}

	if !pollerDeployment.Equal(wfDeployment) {
		// Task is redirected, see if this activity should start a transition on the workflow. The
		// workflow transition happens only if the workflow TQ's current deployment is the same as
		// the poller deployment. Otherwise, it means the activity is independently versioned, we
		// allow it to start without affecting the workflow.
		wfTqCurrentDeployment, err := getTaskQueueCurrentDeployment(ctx,
			request.NamespaceId,
			mutableState.GetExecutionInfo().GetTaskQueue(),
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			matchingClient)
		if err != nil {
			// Let matching retry
			return nil, false, err
		}
		if pollerDeployment.Equal(wfTqCurrentDeployment) {
			if err := mutableState.StartDeploymentTransition(pollerDeployment); err != nil {
				if errors.Is(err, workflow.ErrPinnedWorkflowCannotTransition) {
					// This must be a task from a time that the workflow was unpinned, but it's
					// now pinned so can't transition. Matching can drop the task safely.
					return nil, false, serviceerrors.NewObsoleteMatchingTask(err.Error())
				}
				return nil, false, err
			}
			// This activity started a transition, make sure the MS changes are written but
			// reject the activity task.
			return nil, true, nil
		}
	}

	versioningStamp := worker_versioning.StampFromCapabilities(request.PollRequest.WorkerVersionCapabilities)
	if _, err := mutableState.AddActivityTaskStartedEvent(
		ai, scheduledEventID, requestID, request.PollRequest.GetIdentity(),
		versioningStamp, pollerDeployment, request.GetBuildIdRedirectInfo(),
	); err != nil {
		return nil, false, err
	}

	scheduleToStartLatency := ai.GetStartedTime().AsTime().Sub(ai.GetScheduledTime().AsTime())
	metrics.TaskScheduleToStartLatency.With(
		metrics.GetPerTaskQueuePartitionTypeScope(
			taggedMetrics,
			namespaceName,
			// passing the root partition all the time as we don't care about partition ID in this metric
			tqid.UnsafeTaskQueueFamily(namespaceEntry.ID().String(), ai.GetTaskQueue()).TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition(),
			shardContext.GetConfig().BreakdownMetricsByTaskQueue(namespaceName, ai.GetTaskQueue(), enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		),
	).Record(scheduleToStartLatency)

	response.StartedTime = ai.StartedTime
	response.Attempt = ai.Attempt
	response.HeartbeatDetails = ai.LastHeartbeatDetails
	response.Version = ai.Version

	response.WorkflowType = mutableState.GetWorkflowType()
	response.WorkflowNamespace = namespaceName

	return response, false, nil
}

// TODO: move this method to a better place
// TODO: cache this result (especially if the answer is true)
func getTaskQueueCurrentDeployment(
	ctx context.Context,
	namespaceID string,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	matchingClient matchingservice.MatchingServiceClient,
) (*deploymentpb.Deployment, error) {
	resp, err := matchingClient.GetTaskQueueUserData(ctx,
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   namespaceID,
			TaskQueue:     taskQueueName,
			TaskQueueType: taskQueueType,
		})
	if err != nil {
		return nil, err
	}
	tqData, ok := resp.GetUserData().GetData().GetPerType()[int32(taskQueueType)]
	if !ok {
		// The TQ is unversioned
		return nil, nil
	}
	return worker_versioning.FindCurrentDeployment(tqData.GetDeploymentData()), nil
}
