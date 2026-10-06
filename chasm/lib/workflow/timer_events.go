package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/timer"
)

// TimerStartedEventDefinition handles the TimerStarted history event: it adds a timer to the
// workflow that fires StartToFireTimeout after the event.
type TimerStartedEventDefinition struct{}

func (d TimerStartedEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d TimerStartedEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_TIMER_STARTED
}

func (d TimerStartedEventDefinition) Apply(ctx chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	attrs := event.GetTimerStartedEventAttributes()
	fireTime := event.GetEventTime().AsTime().Add(attrs.GetStartToFireTimeout().AsDuration())
	if wf.Timers == nil {
		wf.Timers = make(chasm.Map[string, *timer.Timer])
	}
	t := timer.New(ctx, attrs.GetTimerId(), event.GetEventId(), fireTime)
	wf.Timers[attrs.GetTimerId()] = chasm.NewComponentField(ctx, t)
	return nil
}

func (d TimerStartedEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	// We never cherry pick command events, and instead allow user logic to reschedule those commands.
	return ErrEventNotCherryPickable
}

// TimerFiredEventDefinition handles the TimerFired history event: it removes the timer.
type TimerFiredEventDefinition struct{}

func (d TimerFiredEventDefinition) IsWorkflowTaskTrigger() bool {
	return true
}

func (d TimerFiredEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_TIMER_FIRED
}

func (d TimerFiredEventDefinition) Apply(_ chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	return wf.removeTimer(event.GetTimerFiredEventAttributes().GetTimerId())
}

func (d TimerFiredEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

// TimerCanceledEventDefinition handles the TimerCanceled history event: it removes the timer.
type TimerCanceledEventDefinition struct{}

func (d TimerCanceledEventDefinition) IsWorkflowTaskTrigger() bool {
	return false
}

func (d TimerCanceledEventDefinition) Type() enumspb.EventType {
	return enumspb.EVENT_TYPE_TIMER_CANCELED
}

func (d TimerCanceledEventDefinition) Apply(_ chasm.MutableContext, wf *Workflow, event *historypb.HistoryEvent) error {
	return wf.removeTimer(event.GetTimerCanceledEventAttributes().GetTimerId())
}

func (d TimerCanceledEventDefinition) CherryPick(_ chasm.MutableContext, _ *Workflow, _ *historypb.HistoryEvent, _ map[enumspb.ResetReapplyExcludeType]struct{}) error {
	return ErrEventNotCherryPickable
}

func (w *Workflow) removeTimer(timerID string) error {
	if _, ok := w.Timers[timerID]; !ok {
		return serviceerror.NewNotFoundf("timer %q not found", timerID)
	}
	delete(w.Timers, timerID)
	return nil
}
