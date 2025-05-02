package api

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/locks"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func SetActivityTaskRunID(
	ctx context.Context,
	token *tokenspb.Task,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) error {
	// TODO when the following APIs are deprecated
	//  remove this function since run ID will always be set
	//  * RecordActivityTaskHeartbeatById
	//  * RespondActivityTaskCanceledById
	//  * RespondActivityTaskFailedById
	//  * RespondActivityTaskCompletedById

	if len(token.RunId) != 0 {
		return nil
	}

	runID, err := workflowConsistencyChecker.GetCurrentRunID(
		ctx,
		token.NamespaceId,
		token.WorkflowId,
		locks.PriorityHigh,
	)
	if err != nil {
		return err
	}
	token.RunId = runID
	return nil
}

func GetActivityScheduledEventID(
	activityID string,
	mutableState historyi.MutableState,
) (int64, error) {

	if activityID == "" {
		return 0, serviceerror.NewInvalidArgument("activityID cannot be empty")
	}
	activityInfo, ok := mutableState.GetActivityByActivityID(activityID)
	if !ok {
		return 0, serviceerror.NewNotFound(fmt.Sprintf("cannot find pending activity with ActivityID %s, check workflow execution history for more details", activityID))
	}
	return activityInfo.ScheduledEventId, nil
}
