package respondworkflowtaskfailed

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskFailedRequest,
	shardContext historyi.ShardContext,
	tokenSerializer *tasktoken.Serializer,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (retError error) {
	_, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return err
	}

	request := req.FailedRequest
	token, err := tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return consts.ErrDeserializingToken
	}

	return api.GetAndUpdateWorkflowWithNew(
		ctx,
		token.Clock,
		definition.NewWorkflowKey(
			token.NamespaceId,
			token.WorkflowId,
			token.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()))
			if err != nil {
				return nil, err
			}

			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			scheduledEventID := token.GetScheduledEventId()
			workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)

			// LOG: Check result before validation
			shardContext.GetLogger().Info("DEBUG-STAMP: RespondWorkflowTaskFailed called",
				tag.NewInt64("requested-scheduled-event-id", scheduledEventID),
				tag.NewBoolTag("task-found", workflowTask != nil),
				tag.NewInt32("current-stamp", mutableState.GetExecutionInfo().GetWorkflowTaskStamp()))

			if workflowTask == nil ||
				workflowTask.StartedEventID == common.EmptyEventID ||
				(token.StartedEventId != common.EmptyEventID && token.StartedEventId != workflowTask.StartedEventID) ||
				(token.StartedTime != nil && !workflowTask.StartedTime.IsZero() && !token.StartedTime.AsTime().Equal(workflowTask.StartedTime)) ||
				workflowTask.Attempt != token.Attempt ||
				(workflowTask.Version != common.EmptyVersion && token.Version != workflowTask.Version) {
				// LOG: Rejection reason
				shardContext.GetLogger().Info("DEBUG-STAMP: RespondWorkflowTaskFailed REJECTED",
					tag.NewStringTag("reason", func() string {
						if workflowTask == nil {
							return "workflowTask is nil"
						}
						if workflowTask.StartedEventID == common.EmptyEventID {
							return "StartedEventID is empty"
						}
						if token.StartedEventId != common.EmptyEventID && token.StartedEventId != workflowTask.StartedEventID {
							return fmt.Sprintf("StartedEventID mismatch: token=%d, task=%d", token.StartedEventId, workflowTask.StartedEventID)
						}
						if workflowTask.Attempt != token.Attempt {
							return fmt.Sprintf("Attempt mismatch: token=%d, task=%d", token.Attempt, workflowTask.Attempt)
						}
						return "other mismatch"
					}()))
				// we have not alter mutable state yet, so release with it with nil to avoid clear MS.
				workflowLease.GetReleaseFn()(nil)
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			if workflowTask.Attempt > 1 && shardContext.GetConfig().EnableDropRepeatedWorkflowTaskFailures(namespaceEntry.Name().String()) {
				// drop repeated workflow task failed calls, as workaround to prevent busy loop
				return &api.UpdateWorkflowAction{
					Noop: true,
				}, nil
			}

			if _, err := mutableState.AddWorkflowTaskFailedEvent(
				workflowTask,
				request.GetCause(),
				request.GetFailure(),
				request.GetIdentity(),
				request.GetWorkerVersion(),
				request.GetBinaryChecksum(),
				"",
				"",
				0); err != nil {
				return nil, err
			}

			// STEP 2 LOG: Task failed
			shardContext.GetLogger().Info("DEBUG-FLOW [STEP 2]: Task failed, about to reschedule",
				tag.WorkflowScheduledEventID(workflowTask.ScheduledEventID),
				tag.NewStringTag("assigned-build-id", mutableState.GetAssignedBuildId()),
				tag.NewStringTag("task-build-id", mutableState.GetExecutionInfo().GetWorkflowTaskBuildId()),
				tag.NewInt32("current-stamp", mutableState.GetExecutionInfo().GetWorkflowTaskStamp()),
				tag.NewStringTag("failure-cause", request.GetCause().String()))

			metrics.FailedWorkflowTasksCounter.With(shardContext.GetMetricsHandler()).Record(
				1,
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskFailedScope),
				metrics.NamespaceTag(namespaceEntry.Name().String()),
				metrics.VersioningBehaviorTag(mutableState.GetEffectiveVersioningBehavior()),
				metrics.FailureTag(request.GetCause().String()),
				metrics.FirstAttemptTag(workflowTask.Attempt),
			)

			if request.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_GRPC_MESSAGE_TOO_LARGE {
				if err := workflow.TerminateWorkflow(
					mutableState,
					request.GetCause().String(),
					nil,
					consts.IdentityHistoryService,
					false,
					nil,
				); err != nil {
					return nil, err
				}

				return api.UpdateWorkflowTerminate, nil
			}

			// TODO (alex-update): if it was speculative WT that failed, and there is nothing but pending updates,
			//  new WT also should be create as speculative (or not?). Currently, it will be recreated as normal WT.
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
}
