package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

// ActivityTaskScheduledEventDefinition handles the ActivityTaskScheduled history event: it adds
// the activity to the workflow and schedules it.
type ActivityTaskScheduledEventDefinition struct{}

func (d ActivityTaskScheduledEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d ActivityTaskScheduledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
}

func (d ActivityTaskScheduledEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskScheduledEventAttributes()
	a := activity.NewEmbeddedActivity(
		ctx,
		&activitypb.ActivityState{
			ActivityType:           attrs.GetActivityType(),
			TaskQueue:              attrs.GetTaskQueue(),
			ScheduleToCloseTimeout: attrs.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout: attrs.GetScheduleToStartTimeout(),
			StartToCloseTimeout:    attrs.GetStartToCloseTimeout(),
			HeartbeatTimeout:       attrs.GetHeartbeatTimeout(),
			RetryPolicy:            attrs.GetRetryPolicy(),
			Priority:               attrs.GetPriority(),
		},
		&activitypb.ActivityRequestData{
			Input:  attrs.GetInput(),
			Header: attrs.GetHeader(),
		},
	)
	if wf.Activities == nil {
		wf.Activities = make(chasm.Map[int64, *activity.Activity])
	}
	wf.Activities[event.GetEventId()] = chasm.NewComponentField(ctx, a)
	return activity.TransitionScheduled.Apply(a, ctx, nil)
}

func (d ActivityTaskScheduledEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return ErrEventNotCherryPickable
}

// ActivityTaskStartedEventDefinition handles the ActivityTaskStarted history event. The event is
// written together with the activity's outcome, which the activity has already recorded.
type ActivityTaskStartedEventDefinition struct{}

func (d ActivityTaskStartedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d ActivityTaskStartedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
}

func (d ActivityTaskStartedEventDefinition) Apply(_ chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	return wf.requireActivity(event.GetActivityTaskStartedEventAttributes().GetScheduledEventId())
}

func (d ActivityTaskStartedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskCompletedEventDefinition handles the ActivityTaskCompleted history event.
type ActivityTaskCompletedEventDefinition struct{}

func (d ActivityTaskCompletedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskCompletedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
}

func (d ActivityTaskCompletedEventDefinition) Apply(_ chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	return wf.removeActivity(event.GetActivityTaskCompletedEventAttributes().GetScheduledEventId())
}

func (d ActivityTaskCompletedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskFailedEventDefinition handles the ActivityTaskFailed history event.
type ActivityTaskFailedEventDefinition struct{}

func (d ActivityTaskFailedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskFailedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
}

func (d ActivityTaskFailedEventDefinition) Apply(_ chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	return wf.removeActivity(event.GetActivityTaskFailedEventAttributes().GetScheduledEventId())
}

func (d ActivityTaskFailedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskTimedOutEventDefinition handles the ActivityTaskTimedOut history event.
type ActivityTaskTimedOutEventDefinition struct{}

func (d ActivityTaskTimedOutEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskTimedOutEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
}

func (d ActivityTaskTimedOutEventDefinition) Apply(_ chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	return wf.removeActivity(event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId())
}

func (d ActivityTaskTimedOutEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskCanceledEventDefinition handles the ActivityTaskCanceled history event.
type ActivityTaskCanceledEventDefinition struct{}

func (d ActivityTaskCanceledEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskCanceledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED
}

func (d ActivityTaskCanceledEventDefinition) Apply(_ chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	return wf.removeActivity(event.GetActivityTaskCanceledEventAttributes().GetScheduledEventId())
}

func (d ActivityTaskCanceledEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

func (w *Workflow) requireActivity(scheduledEventID int64) error {
	if _, ok := w.Activities[scheduledEventID]; !ok {
		return serviceerror.NewNotFoundf("activity not found for scheduled event ID %d", scheduledEventID)
	}
	return nil
}

func (w *Workflow) removeActivity(scheduledEventID int64) error {
	if err := w.requireActivity(scheduledEventID); err != nil {
		return err
	}
	delete(w.Activities, scheduledEventID)
	return nil
}
