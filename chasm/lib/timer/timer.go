// Package timer is a CHASM component for a durable timer that fires via a pure task. Its parent
// records the firing.
package timer

import (
	"time"

	"go.temporal.io/server/chasm"
	timerpb "go.temporal.io/server/chasm/lib/timer/gen/timerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store is the parent of a timer, which records that it fired.
type Store interface {
	RecordTimerFired(ctx chasm.MutableContext, t *Timer) error
}

type Timer struct {
	chasm.UnimplementedComponent

	*timerpb.TimerState

	Store chasm.ParentPtr[Store]
}

// New returns a timer that fires at fireTime. The parent removes it when it fires or is canceled.
func New(ctx chasm.MutableContext, timerID string, startedEventID int64, fireTime time.Time) *Timer {
	t := &Timer{TimerState: &timerpb.TimerState{
		TimerId:        timerID,
		StartedEventId: startedEventID,
		FireTime:       timestamppb.New(fireTime),
	}}
	ctx.AddTask(t, chasm.TaskAttributes{ScheduledTime: fireTime}, &timerpb.TimerFireTask{})
	return t
}

// LifecycleState is always running: a timer is removed from its parent when it fires or is
// canceled.
func (t *Timer) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

type fireTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func (h *fireTaskHandler) Validate(chasm.Context, *Timer, chasm.TaskInvocation, *timerpb.TimerFireTask) (bool, error) {
	return true, nil
}

func (h *fireTaskHandler) Execute(ctx chasm.MutableContext, t *Timer, _ chasm.TaskAttributes, _ *timerpb.TimerFireTask) error {
	return t.Store.Get(ctx).RecordTimerFired(ctx, t)
}
