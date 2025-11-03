package isactivitytaskvalid

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	req *historyservice.IsActivityTaskValidRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.IsActivityTaskValidResponse, retError error) {
	isValid := false
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		req.Clock,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.Execution.WorkflowId,
			req.Execution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			isTaskValid, err := isActivityTaskValid(workflowLease, req.ScheduledEventId, req.GetStamp())
			if err != nil {
				return nil, err
			}
			isValid = isTaskValid
			return &api.UpdateWorkflowAction{
				Noop:               true,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
	return &historyservice.IsActivityTaskValidResponse{
		IsValid: isValid,
	}, err
}

func isActivityTaskValid(
	workflowLease api.WorkflowLease,
	scheduledEventID int64,
	stamp int32,
) (bool, error) {
	mutableState := workflowLease.GetMutableState()
	if !mutableState.IsWorkflowExecutionRunning() {
		return false, consts.ErrWorkflowCompleted
	}

	ai, ok := mutableState.GetActivityInfo(scheduledEventID)
	if ok && ai.StartedEventId == common.EmptyEventID && ai.GetStamp() == stamp {
		return true, nil
	}
	return false, nil
}
