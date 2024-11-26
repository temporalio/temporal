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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
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
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RecordActivityTaskStartedResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespace := namespaceEntry.Name()

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

			scheduledEventID := request.GetScheduledEventId()
			requestID := request.GetRequestId()
			ai, isRunning := mutableState.GetActivityInfo(scheduledEventID)

			taggedMetrics := shard.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.HistoryRecordActivityTaskStartedScope))

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduledEventID >= mutableState.GetNextEventID() {
				metrics.StaleMutableStateCounter.With(taggedMetrics).Record(1)
				return nil, consts.ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if !isRunning {
				// Looks like ActivityTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				return nil, consts.ErrActivityTaskNotFound
			}

			scheduledEvent, err := mutableState.GetActivityScheduledEvent(ctx, scheduledEventID)
			if err != nil {
				return nil, err
			}
			response.ScheduledEvent = scheduledEvent
			response.CurrentAttemptScheduledTime = ai.ScheduledTime

			if ai.StartedEventId != common.EmptyEventID {
				// If activity is started as part of the current request scope then return a positive response
				if ai.RequestId == requestID {
					response.StartedTime = ai.StartedTime
					response.Attempt = ai.Attempt
					return &api.UpdateWorkflowAction{
						Noop:               false,
						CreateWorkflowTask: false,
					}, nil
				}

				// Looks like ActivityTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerrors.NewTaskAlreadyStarted("Activity")
			}

			// TODO (shahab): support independent activities. Independent activities do not need all
			// the deployment validations and do not start workflow transition.

			if mutableState.GetDeploymentTransition() != nil {
				// Can't start activity during a redirect. We reject this request so Matching drops
				// the task. The activity will be rescheduled when the redirect completes/fails.
				return nil, serviceerrors.NewActivityStartDuringTransition()
			}

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
				return nil, serviceerrors.NewObsoleteMatchingTask("wrong directive deployment")
			}

			if mutableState.GetDeploymentTransition() != nil {
				// Can't start activity during a redirect. We reject this request so Matching drops
				// the task. The activity will be rescheduled when the redirect completes/fails.
				return nil, serviceerrors.NewActivityStartDuringTransition()
			}

			if !pollerDeployment.Equal(wfDeployment) {
				// Task is redirected, see if a transition can start.
				if err := mutableState.StartDeploymentTransition(pollerDeployment); err != nil {
					if errors.Is(err, workflow.ErrPinnedWorkflowCannotTransition) {
						// This must be a task from a time that the workflow was unpinned, but it's
						// now pinned so can't transition. Matching can drop the task safely.
						return nil, serviceerrors.NewObsoleteMatchingTask(err.Error())
					}
					return nil, err
				}
				// This activity started a transition, make sure the MS changes are written but
				// reject the activity task.
				// Not returning error so the mutable state is updated. Just setting this flag to
				// return error at a higher level.
				startedTransition = true
				return &api.UpdateWorkflowAction{
					Noop: false,
					// StartDeploymentTransition rescheduled pending wft, but this creates new
					// one if there is no pending wft.
					CreateWorkflowTask: true,
				}, nil

			}

			versioningStamp := worker_versioning.StampFromCapabilities(request.PollRequest.WorkerVersionCapabilities)
			if _, err := mutableState.AddActivityTaskStartedEvent(
				ai, scheduledEventID, requestID, request.PollRequest.GetIdentity(),
				versioningStamp, pollerDeployment, request.GetBuildIdRedirectInfo(),
			); err != nil {
				return nil, err
			}

			scheduleToStartLatency := ai.GetStartedTime().AsTime().Sub(ai.GetScheduledTime().AsTime())
			namespaceName := namespaceEntry.Name().String()
			metrics.TaskScheduleToStartLatency.With(
				metrics.GetPerTaskQueuePartitionTypeScope(
					taggedMetrics,
					namespaceName,
					// passing the root partition all the time as we don't care about partition ID in this metric
					tqid.UnsafeTaskQueueFamily(namespaceEntry.ID().String(), ai.GetTaskQueue()).TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition(),
					shard.GetConfig().BreakdownMetricsByTaskQueue(namespaceName, ai.GetTaskQueue(), enumspb.TASK_QUEUE_TYPE_ACTIVITY),
				),
			).Record(scheduleToStartLatency)

			response.StartedTime = ai.StartedTime
			response.Attempt = ai.Attempt
			response.HeartbeatDetails = ai.LastHeartbeatDetails
			response.Version = ai.Version

			response.WorkflowType = mutableState.GetWorkflowType()
			response.WorkflowNamespace = namespace.String()

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shard,
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
