package respondworkflowtaskfailed

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
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
	request := req.FailedRequest
	token, err := tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return consts.ErrDeserializingToken
	}

	_, err = api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()), token.WorkflowId)
	if err != nil {
		return err
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
			namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()), token.WorkflowId)
			if err != nil {
				return nil, err
			}

			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			scheduledEventID := token.GetScheduledEventId()
			workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)

			if workflowTask == nil ||
				workflowTask.StartedEventID == common.EmptyEventID ||
				(token.StartedEventId != common.EmptyEventID && token.StartedEventId != workflowTask.StartedEventID) ||
				(token.StartedTime != nil && !workflowTask.StartedTime.IsZero() && !token.StartedTime.AsTime().Equal(workflowTask.StartedTime)) ||
				workflowTask.Attempt != token.Attempt ||
				(workflowTask.Version != common.EmptyVersion && token.Version != workflowTask.Version) {
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

			if _, err := mutableState.AddWorkflowTaskFailedEvent(
				workflowTask,
				request.GetCause(),
				request.GetFailure(),
				request.GetIdentity(),
				//nolint:staticcheck
				request.GetWorkerVersion(),
				//nolint:staticcheck
				request.GetBinaryChecksum(),
				"",
				"",
				0); err != nil {
				return nil, err
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
