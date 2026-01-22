package forcerescheduleactivity

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
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
	logger := shardContext.GetLogger()
	workflowKey := definition.NewWorkflowKey(
		req.GetNamespaceId(),
		req.GetWorkflowId(),
		req.GetRunId(),
	)

	logger.Info("worker_chasm: ForceRescheduleActivity called",
		tag.WorkflowID(req.GetWorkflowId()),
		tag.WorkflowRunID(req.GetRunId()),
		tag.ActivityID(req.GetActivityId()))

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()

			// Find the activity by ID
			activityID := req.GetActivityId()
			var activityInfo *persistencespb.ActivityInfo
			for _, ai := range mutableState.GetPendingActivityInfos() {
				if ai.ActivityId == activityID {
					activityInfo = ai
					break
				}
			}

			if activityInfo == nil {
				logger.Info("worker_chasm: Activity not found, may have completed",
					tag.WorkflowID(req.GetWorkflowId()),
					tag.ActivityID(activityID))
				return nil, consts.ErrActivityNotFound
			}

			activityState := workflow.GetActivityState(activityInfo)
			logger.Info("worker_chasm: Found activity, checking state",
				tag.WorkflowID(req.GetWorkflowId()),
				tag.ActivityID(activityID),
				tag.NewStringTag("activity_state", activityState.String()),
				tag.NewInt64("started_event_id", activityInfo.StartedEventId),
				tag.NewStringTag("started_identity", activityInfo.StartedIdentity))

			// Handle STARTED (running) activities - need to reset to SCHEDULED
			if activityState == enumspb.PENDING_ACTIVITY_STATE_STARTED {
				logger.Info("worker_chasm: Activity is STARTED, resetting to SCHEDULED for reschedule",
					tag.WorkflowID(req.GetWorkflowId()),
					tag.ActivityID(activityID),
					tag.NewStringTag("old_worker", activityInfo.StartedIdentity))

				// Reset the activity to SCHEDULED state so it can be picked up again
				err := mutableState.UpdateActivity(activityInfo.ScheduledEventId, func(ai *persistencespb.ActivityInfo, ms historyi.MutableState) error {
					// Store old identity for logging
					oldIdentity := ai.StartedIdentity

					// Reset started state - this moves activity from STARTED to SCHEDULED
					ai.StartedEventId = common.EmptyEventID
					ai.StartVersion = common.EmptyVersion
					ai.RequestId = ""
					ai.StartedTime = nil
					ai.RetryLastWorkerIdentity = ai.StartedIdentity
					ai.StartedIdentity = ""

					// Increment stamp to invalidate any in-flight responses from dead worker
					ai.Stamp++

					// Clear timer status so new timers will be created
					ai.TimerTaskStatus = workflow.TimerTaskStatusNone

					// Clear heartbeat since worker is dead
					ai.LastHeartbeatDetails = nil
					ai.LastHeartbeatUpdateTime = nil

					logger.Info("worker_chasm: Activity state reset, regenerating task",
						tag.WorkflowID(req.GetWorkflowId()),
						tag.ActivityID(activityID),
						tag.NewStringTag("old_worker", oldIdentity),
						tag.NewInt64("new_stamp", int64(ai.Stamp)))

					// Regenerate the activity task so it can be dispatched to a new worker
					scheduleTime := shardContext.GetTimeSource().Now().UTC()
					if err := ms.RegenerateActivityRetryTask(ai, scheduleTime); err != nil {
						logger.Error("worker_chasm: Failed to regenerate activity task",
							tag.WorkflowID(req.GetWorkflowId()),
							tag.ActivityID(activityID),
							tag.Error(err))
						return err
					}

					logger.Info("worker_chasm: Activity rescheduled successfully",
						tag.WorkflowID(req.GetWorkflowId()),
						tag.ActivityID(activityID))

					return nil
				})
				if err != nil {
					return nil, err
				}

				return &api.UpdateWorkflowAction{
					Noop:               false,
					CreateWorkflowTask: false,
				}, nil
			}

			// For non-STARTED activities, use the existing ResetActivity logic
			logger.Info("worker_chasm: Activity not STARTED, using ResetActivity",
				tag.WorkflowID(req.GetWorkflowId()),
				tag.ActivityID(activityID),
				tag.NewStringTag("activity_state", activityState.String()))

			if err := workflow.ResetActivity(
				ctx,
				shardContext, mutableState, activityID,
				true,  // resetHeartbeat
				false, // keepPaused
				false, // resetOptions
				0,     // jitter
			); err != nil {
				logger.Error("worker_chasm: ResetActivity failed",
					tag.WorkflowID(req.GetWorkflowId()),
					tag.ActivityID(activityID),
					tag.Error(err))
				return nil, err
			}

			logger.Info("worker_chasm: ResetActivity completed",
				tag.WorkflowID(req.GetWorkflowId()),
				tag.ActivityID(activityID))

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
		logger.Error("worker_chasm: ForceRescheduleActivity failed",
			tag.WorkflowID(req.GetWorkflowId()),
			tag.ActivityID(req.GetActivityId()),
			tag.Error(err))
		return nil, err
	}

	return &historyservice.ForceRescheduleActivityResponse{}, nil
}
