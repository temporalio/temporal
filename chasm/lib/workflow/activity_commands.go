package workflow

import (
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
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
