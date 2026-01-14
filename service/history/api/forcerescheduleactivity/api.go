package forcerescheduleactivity

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

// Invoke reschedules an activity when a worker dies.
// This is called by the CHASM worker lease expiry executor to reschedule
// activities that were running on the dead worker.
func Invoke(
	ctx context.Context,
	req *historyservice.ForceRescheduleActivityRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.ForceRescheduleActivityResponse, retError error) {
	workflowKey := definition.NewWorkflowKey(
		req.GetNamespaceId(),
		req.GetWorkflowId(),
		req.GetRunId(),
	)

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()

			// Find the activity by ID
			activityID := req.GetActivityId()
			found := false
			for _, ai := range mutableState.GetPendingActivityInfos() {
				if ai.ActivityId == activityID {
					found = true
					break
				}
			}

			if !found {
				// Activity not found - might have already completed
				return nil, consts.ErrActivityNotFound
			}

			// Reset the activity to reschedule it
			// ResetHeartbeat: true - clear heartbeat state since worker is dead
			// KeepPaused: false - unpause if paused
			// ResetOptions: false - keep original options
			// Jitter: 0 - reschedule immediately
			if err := workflow.ResetActivity(
				ctx,
				shardContext, mutableState, activityID,
				true,  // resetHeartbeat
				false, // keepPaused
				false, // resetOptions
				0,     // jitter
			); err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	return &historyservice.ForceRescheduleActivityResponse{}, nil
}

