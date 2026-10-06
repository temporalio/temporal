package workflow

import (
	activitypb "go.temporal.io/api/activity/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"google.golang.org/protobuf/types/known/durationpb"
)

type activityCommandHandler struct {
	config Config
}

// handleScheduleCommand validates and normalizes a ScheduleActivityTask command as the server's
// command handler does, and adds the ActivityTaskScheduled event, whose definition creates the
// activity.
func (h *activityCommandHandler) handleScheduleCommand(
	ctx chasm.MutableContext,
	wf *Workflow,
	_ Validator,
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
	if err := h.normalizeAttributes(ctx, wf, attrs); err != nil {
		if _, ok := err.(*serviceerror.InvalidArgument); ok {
			return FailWorkflowTaskError{
				Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES,
				Message: err.Error(),
			}
		}
		return err
	}
	_, err := addAndApplyHistoryEvent[ActivityTaskScheduledEventDefinition](wf, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:                   attrs.GetActivityId(),
				ActivityType:                 attrs.GetActivityType(),
				TaskQueue:                    attrs.GetTaskQueue(),
				Header:                       attrs.GetHeader(),
				Input:                        attrs.GetInput(),
				ScheduleToCloseTimeout:       attrs.GetScheduleToCloseTimeout(),
				ScheduleToStartTimeout:       attrs.GetScheduleToStartTimeout(),
				StartToCloseTimeout:          attrs.GetStartToCloseTimeout(),
				HeartbeatTimeout:             attrs.GetHeartbeatTimeout(),
				WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
				RetryPolicy:                  attrs.GetRetryPolicy(),
				UseWorkflowBuildId:           attrs.GetUseWorkflowBuildId(), //nolint:staticcheck // deprecated, but the server still records it
				Priority:                     attrs.GetPriority(),
			},
		}
	})
	return err
}

// normalizeAttributes fills in the defaults the server fills in for a workflow activity.
func (h *activityCommandHandler) normalizeAttributes(
	ctx chasm.Context,
	wf *Workflow,
	attrs *commandpb.ScheduleActivityTaskCommandAttributes,
) error {
	if attrs.RetryPolicy == nil {
		attrs.RetryPolicy = &commonpb.RetryPolicy{}
	}
	options := &activitypb.ActivityOptions{
		TaskQueue:              attrs.TaskQueue,
		ScheduleToCloseTimeout: attrs.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout: attrs.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    attrs.GetStartToCloseTimeout(),
		HeartbeatTimeout:       attrs.GetHeartbeatTimeout(),
		RetryPolicy:            attrs.RetryPolicy,
	}
	if err := activity.ValidateAndNormalizeEmbeddedActivity(
		attrs.GetActivityId(),
		attrs.GetActivityType().GetName(),
		h.config.defaultActivityRetrySettings,
		h.config.maxIDLengthLimit(),
		ctx.NamespaceEntry().Name(),
		options,
		attrs.GetPriority(),
		durationpb.New(wf.WorkflowRunTimeout()),
		wf.WorkflowTaskQueue(),
	); err != nil {
		return err
	}
	attrs.TaskQueue = options.TaskQueue
	attrs.ScheduleToCloseTimeout = options.ScheduleToCloseTimeout
	attrs.ScheduleToStartTimeout = options.ScheduleToStartTimeout
	attrs.StartToCloseTimeout = options.StartToCloseTimeout
	attrs.HeartbeatTimeout = options.HeartbeatTimeout
	attrs.RetryPolicy = options.RetryPolicy
	return nil
}
