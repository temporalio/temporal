package workflow

import (
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
)

type activityCommandHandler struct{}

func (h *activityCommandHandler) handleScheduleCommand(
	ctx chasm.MutableContext,
	wf *Workflow,
	validator Validator,
	cmd *commandpb.Command,
	opts CommandHandlerOptions,
) error {
	attrs := cmd.GetScheduleActivityTaskCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
			Message: "empty ScheduleActivityTaskCommandAttributes",
		}
	}

	if _, exists := wf.Activities[attrs.GetActivityId()]; exists {
		return FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_SCHEDULE_ACTIVITY_DUPLICATE_ID,
			Message: fmt.Sprintf("activity with id %q already exists", attrs.GetActivityId()),
		}
	}

	if !validator.IsValidPayloadSize(attrs.GetInput().Size()) {
		return FailWorkflowTaskError{
			Cause:             enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
			Message:           "ScheduleActivityTaskCommandAttributes.Input exceeds size limit",
			TerminateWorkflow: true,
		}
	}

	_, err := addAndApplyHistoryEvent[ActivityTaskScheduledEventDefinition](wf, ctx, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:                   attrs.GetActivityId(),
				ActivityType:                 attrs.GetActivityType(),
				TaskQueue:                    attrs.GetTaskQueue(),
				Input:                        attrs.GetInput(),
				Header:                       attrs.GetHeader(),
				ScheduleToCloseTimeout:       attrs.GetScheduleToCloseTimeout(),
				ScheduleToStartTimeout:       attrs.GetScheduleToStartTimeout(),
				StartToCloseTimeout:          attrs.GetStartToCloseTimeout(),
				HeartbeatTimeout:             attrs.GetHeartbeatTimeout(),
				RetryPolicy:                  attrs.GetRetryPolicy(),
				Priority:                     attrs.GetPriority(),
				WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
			},
		}
		he.UserMetadata = cmd.UserMetadata
	})
	return err
}

func (h *activityCommandHandler) handleRequestCancelCommand(
	ctx chasm.MutableContext,
	wf *Workflow,
	_ Validator,
	cmd *commandpb.Command,
	opts CommandHandlerOptions,
) error {
	attrs := cmd.GetRequestCancelActivityTaskCommandAttributes()
	if attrs == nil {
		return FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES,
			Message: "empty RequestCancelActivityTaskCommandAttributes",
		}
	}
	scheduledEventID := attrs.GetScheduledEventId()

	act := wf.FindActivityByScheduledEventID(ctx, scheduledEventID)
	if act == nil {
		return FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES,
			Message: fmt.Sprintf("activity with scheduled event ID %d not found in CHASM tree", scheduledEventID),
		}
	}

	wasNotStarted := act.Status == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED

	cancelReqEvent, err := addAndApplyHistoryEvent[ActivityTaskCancelRequestedEventDefinition](wf, ctx, func(he *historypb.HistoryEvent) {
		he.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
			ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
				ScheduledEventId:             scheduledEventID,
				WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
			},
		}
		he.UserMetadata = cmd.UserMetadata
	})
	if err != nil {
		return err
	}
	if opts.CancelRequestedEventID != nil {
		*opts.CancelRequestedEventID = cancelReqEvent.GetEventId()
	}

	if wasNotStarted {
		_, err = addAndApplyHistoryEvent[ActivityTaskCanceledEventDefinition](wf, ctx, func(he *historypb.HistoryEvent) {
			he.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
				ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
					ScheduledEventId:              scheduledEventID,
					StartedEventId:                common.EmptyEventID,
					LatestCancelRequestedEventId:  cancelReqEvent.GetEventId(),
					Details:                       payloads.EncodeString("ACTIVITY_ID_NOT_STARTED"),
					Identity:                      opts.Identity,
				},
			}
		})
	}
	return err
}

// FindActivityIDByScheduledEventID returns the Activities map key for the activity with the
// given scheduled event ID, or ("", false) if no such activity exists in the CHASM tree.
func FindActivityIDByScheduledEventID(ctx chasm.Context, wf *Workflow, scheduledEventID int64) (string, bool) {
	for id, field := range wf.Activities {
		if field.Get(ctx).GetScheduledEventId() == scheduledEventID {
			return id, true
		}
	}
	return "", false
}

