package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	chasmactivity "go.temporal.io/server/chasm/lib/activity"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ActivityTaskScheduledEventDefinition handles the ActivityTaskScheduled history event.
type ActivityTaskScheduledEventDefinition struct{}

func (d ActivityTaskScheduledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
}

func (d ActivityTaskScheduledEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d ActivityTaskScheduledEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskScheduledEventAttributes()

	act := &chasmactivity.Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           attrs.GetActivityType(),
			TaskQueue:              attrs.GetTaskQueue(),
			ScheduleToCloseTimeout: attrs.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout: attrs.GetScheduleToStartTimeout(),
			StartToCloseTimeout:    attrs.GetStartToCloseTimeout(),
			HeartbeatTimeout:       attrs.GetHeartbeatTimeout(),
			RetryPolicy:            attrs.GetRetryPolicy(),
			Priority:               attrs.GetPriority(),
			ActivityId:             attrs.GetActivityId(),
			ScheduledEventId:       event.GetEventId(),
			ScheduleTime:           event.GetEventTime(),
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
		Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input:  attrs.GetInput(),
			Header: attrs.GetHeader(),
		}),
	}

	if err := chasmactivity.TransitionScheduled.Apply(act, ctx, nil); err != nil {
		return err
	}

	wf.AddEmbeddedActivity(ctx, attrs.GetActivityId(), act)
	return nil
}

func (d ActivityTaskScheduledEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskStartedEventDefinition handles the ActivityTaskStarted history event.
type ActivityTaskStartedEventDefinition struct{}

func (d ActivityTaskStartedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
}

func (d ActivityTaskStartedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d ActivityTaskStartedEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskStartedEventAttributes()

	act := wf.FindActivityByScheduledEventID(ctx, attrs.GetScheduledEventId())
	if act == nil {
		return nil
	}

	// On the forward path, TransitionStarted was already applied by RecordActivityTaskStarted before
	// OnActivityCompleted/OnActivityFailed/OnActivityTimedOut bundle this event into history.
	// Only apply during replication replay, when the activity is still in the Scheduled state.
	if act.Status != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		return nil
	}

	syntheticReq := &historyservice.RecordActivityTaskStartedRequest{
		RequestId: attrs.GetRequestId(),
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			Identity: attrs.GetIdentity(),
		},
	}
	return chasmactivity.TransitionStarted.Apply(act, ctx, syntheticReq)
}

func (d ActivityTaskStartedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskCompletedEventDefinition handles the ActivityTaskCompleted history event.
type ActivityTaskCompletedEventDefinition struct{}

func (d ActivityTaskCompletedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
}

func (d ActivityTaskCompletedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskCompletedEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskCompletedEventAttributes()

	act := wf.FindActivityByScheduledEventID(ctx, attrs.GetScheduledEventId())
	if act == nil {
		return nil
	}

	delete(wf.Activities, act.GetActivityId())
	return wf.ScheduleWorkflowTask()
}

func (d ActivityTaskCompletedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskFailedEventDefinition handles the ActivityTaskFailed history event.
type ActivityTaskFailedEventDefinition struct{}

func (d ActivityTaskFailedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
}

func (d ActivityTaskFailedEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskFailedEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskFailedEventAttributes()

	act := wf.FindActivityByScheduledEventID(ctx, attrs.GetScheduledEventId())
	if act == nil {
		return nil
	}

	delete(wf.Activities, act.GetActivityId())
	return wf.ScheduleWorkflowTask()
}

func (d ActivityTaskFailedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskTimedOutEventDefinition handles the ActivityTaskTimedOut history event.
type ActivityTaskTimedOutEventDefinition struct{}

func (d ActivityTaskTimedOutEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
}

func (d ActivityTaskTimedOutEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskTimedOutEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskTimedOutEventAttributes()

	act := wf.FindActivityByScheduledEventID(ctx, attrs.GetScheduledEventId())
	if act == nil {
		return nil
	}

	delete(wf.Activities, act.GetActivityId())
	return wf.ScheduleWorkflowTask()
}

func (d ActivityTaskTimedOutEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskCancelRequestedEventDefinition handles the ActivityTaskCancelRequested history event.
type ActivityTaskCancelRequestedEventDefinition struct{}

func (d ActivityTaskCancelRequestedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
}

func (d ActivityTaskCancelRequestedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d ActivityTaskCancelRequestedEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskCancelRequestedEventAttributes()

	act := wf.FindActivityByScheduledEventID(ctx, attrs.GetScheduledEventId())
	if act == nil {
		return nil
	}

	// The cancel-requested event attributes don't carry identity/reason, so mutate state directly
	// rather than constructing a synthetic RequestCancelActivityExecution request.
	act.CancelState = &activitypb.ActivityCancelState{
		RequestTime: timestamppb.New(event.GetEventTime().AsTime()),
	}
	act.Status = activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED
	return nil
}

func (d ActivityTaskCancelRequestedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// ActivityTaskCanceledEventDefinition handles the ActivityTaskCanceled history event.
type ActivityTaskCanceledEventDefinition struct{}

func (d ActivityTaskCanceledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED
}

func (d ActivityTaskCanceledEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d ActivityTaskCanceledEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetActivityTaskCanceledEventAttributes()

	act := wf.FindActivityByScheduledEventID(ctx, attrs.GetScheduledEventId())
	if act == nil {
		return nil
	}

	delete(wf.Activities, act.GetActivityId())
	return wf.ScheduleWorkflowTask()
}

func (d ActivityTaskCanceledEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}
