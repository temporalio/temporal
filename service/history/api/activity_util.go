package api

import (
	"context"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
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

	runID, err := workflowConsistencyChecker.GetCurrentWorkflowRunID(
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
		return 0, serviceerror.NewNotFoundf("cannot find pending activity with ActivityID %s, check workflow execution history for more details", activityID)
	}
	return activityInfo.ScheduledEventId, nil
}

func IsActivityTaskNotFoundForToken(
	token *tokenspb.Task,
	ai *persistencespb.ActivityInfo,
	isCompletedByID *bool,
) bool {
	if isCompletedByID == nil || !*isCompletedByID {
		if ai.StartedEventId == common.EmptyEventID {
			return true
		}
	}
	if token.GetScheduledEventId() != common.EmptyEventID && token.Attempt != ai.Attempt {
		return true
	}
	if token.GetStartVersion() != common.EmptyVersion && ai.GetStartVersion() != common.EmptyVersion {
		return token.GetStartVersion() != ai.GetStartVersion()
	}
	if token.GetVersion() != common.EmptyVersion && token.GetVersion() != ai.GetVersion() {
		// For backward compatibility. We should not check version here because ai.Version is last write version,
		// but token.Version is generated when task is created. We should use start version instead.
		return true
	}
	return false
}
