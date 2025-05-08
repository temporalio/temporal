package respondworkflowtaskfailed

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
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
