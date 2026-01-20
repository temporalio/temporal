package forceworkflowtasktimeout

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	request *historyservice.ForceWorkflowTaskTimeoutRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.ForceWorkflowTaskTimeoutResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	if err := api.ValidateNamespaceUUID(namespaceID); err != nil {
		return nil, err
	}

	workflowKey := definition.NewWorkflowKey(
		request.GetNamespaceId(),
		request.GetWorkflowExecution().GetWorkflowId(),
		request.GetWorkflowExecution().GetRunId(),
	)

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			workflowTask := mutableState.GetPendingWorkflowTask()
			if workflowTask == nil {
				return nil, consts.ErrWorkflowTaskNotFound
			}

			timeoutType := request.GetTimeoutType()

			switch timeoutType {
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				if workflowTask.StartedEventID == common.EmptyEventID {
					return nil, serviceerror.NewInvalidArgument(
						"workflow task not started, use TIMEOUT_TYPE_SCHEDULE_TO_START")
				}
				// AddWorkflowTaskTimedOutEvent handles:
				// - Removing speculative timeout task (for speculative WTs)
				// - Recording persisted timeout tasks for deletion
				// - Adding the timeout event to history
				if _, err := mutableState.AddWorkflowTaskTimedOutEvent(workflowTask); err != nil {
					return nil, err
				}

			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
				if workflowTask.StartedEventID != common.EmptyEventID {
					return nil, serviceerror.NewInvalidArgument(
						"workflow task already started, use TIMEOUT_TYPE_START_TO_CLOSE")
				}
				// AddWorkflowTaskScheduleToStartTimeoutEvent handles:
				// - Removing speculative timeout task (for speculative WTs)
				// - Recording persisted timeout tasks for deletion
				// - Adding the timeout event to history
				if _, err := mutableState.AddWorkflowTaskScheduleToStartTimeoutEvent(workflowTask); err != nil {
					return nil, err
				}

			default:
				return nil, serviceerror.NewInvalidArgument(
					"invalid timeout type: must be TIMEOUT_TYPE_START_TO_CLOSE or TIMEOUT_TYPE_SCHEDULE_TO_START")
			}

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	return &historyservice.ForceWorkflowTaskTimeoutResponse{}, nil
}
