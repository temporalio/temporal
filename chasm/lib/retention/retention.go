package retention

import (
	"time"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

// Retention timer component. This component can be used to close out a CHASM
// component after a period of time has elapsed. This is a separate timer from the
// namespace-level workflow/mutable state retention (which takes effect *after* the workflow has
// closed, e.g., by this component).
type Retention struct {
	chasm.UnimplementedComponent

	*persistencespb.RetentionData
}

// NewRetention creates a new Retention component whose LifecycleState will flip
// to the given terminalState after the given duration has elapsed.
func NewRetention(
	ctx chasm.MutableContext,
	duration time.Duration,
	terminalState chasm.LifecycleState,
) (*Retention, error) {
	if !terminalState.IsClosed() {
		return nil,
			serviceerror.NewInternalf("terminalState isn't a 'Closed' state: %v", terminalState)
	}

	r := &Retention{
		RetentionData: &persistencespb.RetentionData{
			TerminalState: int64(terminalState),
		},
	}

	// Adds a timer task to flip the latch to closed after the retention period
	// elapses.
	scheduledTime := ctx.Now(r).Add(duration)
	ctx.AddTask(r, chasm.TaskAttributes{
		ScheduledTime: scheduledTime,
	}, &persistencespb.RetentionTask{})

	return r, nil
}

func (r *Retention) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	if r.Closed {
		return chasm.LifecycleState(r.TerminalState)
	}

	return chasm.LifecycleStateRunning
}
