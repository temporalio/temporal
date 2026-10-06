package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/timer"
)

var _ timer.Store = (*Workflow)(nil)

// RecordTimerFired implements timer.Store by adding the TimerFired event.
func (w *Workflow) RecordTimerFired(ctx chasm.MutableContext, t *timer.Timer) error {
	_, err := addAndApplyHistoryEvent[TimerFiredEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{
			TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
				TimerId:        t.GetTimerId(),
				StartedEventId: t.GetStartedEventId(),
			},
		}
	})
	return err
}

// timerLibrary holds the command handlers and event definitions for workflow timers that are
// timer.Timer components. It is not registered by the server, which runs workflow timers in
// mutable state.
type timerLibrary struct{}

// NewTimerLibrary returns the command handlers and event definitions for workflow timers that are
// timer.Timer components.
func NewTimerLibrary() Library {
	return timerLibrary{}
}

func (timerLibrary) CommandHandlers() map[enumspb.CommandType]CommandHandler {
	return map[enumspb.CommandType]CommandHandler{
		enumspb.COMMAND_TYPE_START_TIMER:  handleStartTimerCommand,
		enumspb.COMMAND_TYPE_CANCEL_TIMER: handleCancelTimerCommand,
	}
}

func (timerLibrary) EventDefinitions() []EventDefinition {
	return []EventDefinition{
		TimerStartedEventDefinition{},
		TimerFiredEventDefinition{},
		TimerCanceledEventDefinition{},
	}
}
