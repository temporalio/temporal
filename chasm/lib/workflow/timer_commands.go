package workflow

import (
	"fmt"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
)

func handleStartTimerCommand(
	ctx chasm.MutableContext,
	wf *Workflow,
	_ Validator,
	cmd *commandpb.Command,
	opts CommandHandlerOptions,
) error {
	attrs := cmd.GetStartTimerCommandAttributes()
	if attrs.GetTimerId() == "" {
		return FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_TIMER_ATTRIBUTES,
			Message: "TimerId is not set on StartTimerCommandAttributes",
		}
	}
	if _, ok := wf.Timers[attrs.GetTimerId()]; ok {
		return FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_START_TIMER_DUPLICATE_ID,
			Message: fmt.Sprintf("timer %q is already running", attrs.GetTimerId()),
		}
	}
	_, err := addAndApplyHistoryEvent[TimerStartedEventDefinition](wf, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
				TimerId:                      attrs.GetTimerId(),
				StartToFireTimeout:           attrs.GetStartToFireTimeout(),
				WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
			},
		}
	})
	return err
}

func handleCancelTimerCommand(
	ctx chasm.MutableContext,
	wf *Workflow,
	_ Validator,
	cmd *commandpb.Command,
	opts CommandHandlerOptions,
) error {
	attrs := cmd.GetCancelTimerCommandAttributes()
	field, ok := wf.Timers[attrs.GetTimerId()]
	if !ok {
		return FailWorkflowTaskError{
			Cause:   enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CANCEL_TIMER_ATTRIBUTES,
			Message: fmt.Sprintf("timer %q is not running", attrs.GetTimerId()),
		}
	}
	timer := field.Get(ctx)
	_, err := addAndApplyHistoryEvent[TimerCanceledEventDefinition](wf, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{
			TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
				TimerId:                      timer.GetTimerId(),
				StartedEventId:               timer.GetStartedEventId(),
				WorkflowTaskCompletedEventId: opts.WorkflowTaskCompletedEventID,
			},
		}
	})
	return err
}
