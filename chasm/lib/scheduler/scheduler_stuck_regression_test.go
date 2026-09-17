package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// a regression test to ensure idle-tasks are handled correctly
func TestIdleDeadlineRearmedAfterRecordingStart(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()

	// Exhaust a limited schedule so the Generator arms the idle task rather
	// than another tick: with no actions left useScheduledAction is false, so
	// getIdleExpiration reports isIdle.
	ctx := env.MutableContext()
	sched := env.Scheduler
	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 0
	sched.ConflictToken++

	// The final action, readied for execution. Attempt == 1 is what
	// recordProcessBufferResult sets before the ExecuteTask runs; it also keeps
	// the start out of the Attempt == 0 filter that would send it back through
	// ProcessBuffer.
	sched.Invoker.Get(ctx).BufferedStarts = append(
		sched.Invoker.Get(ctx).BufferedStarts,
		&schedulespb.BufferedStart{
			RequestId:  "req-final-action",
			WorkflowId: "wf-final-action",
			Attempt:    1,
			ActualTime: timestamppb.New(now),
		},
	)

	sched.Generator.Get(ctx).Generate(ctx)
	require.NoError(t, env.CloseTransaction())

	armedDeadline := sched.GetIdleCloseTime()
	require.NotNil(t, armedDeadline, "precondition: exhausted schedule should arm an idle deadline")
	require.NotZero(t, idleTaskCount(env.Registry, env.Node), "precondition: an idle task should be armed")

	// Record the start through the real invoker path.
	ctx2 := env.MutableContext()
	newlyStarted, _, _ := sched.Invoker.Get(ctx2).RecordExecuteResult(ctx2,
		[]*schedulespb.BufferedStart{{
			RequestId:  "req-final-action",
			WorkflowId: "wf-final-action",
			RunId:      "run-final-action",
			Attempt:    1,
			ActualTime: timestamppb.New(now),
			StartTime:  timestamppb.New(now.Add(30 * time.Minute)),
		}},
		nil,
	)
	require.Equal(t, 1, newlyStarted)
	require.NoError(t, env.CloseTransaction())

	// Confirm the deadline really did shift, so a failure below is about the
	// missing re-arm and not about a test that set up nothing.
	isValid, err := newIdleHandler(scheduler.DefaultTweakables.IdleTime).Validate(
		env.ReadContext(),
		sched,
		chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: armedDeadline.AsTime()}},
		&schedulerpb.SchedulerIdleTask{
			IdleTimeTotal: durationpb.New(scheduler.DefaultTweakables.IdleTime),
		},
	)
	require.NoError(t, err)
	require.False(t, isValid,
		"precondition: recording the start should shift the deadline past the armed task's time")

	// The schedule must still have a path to closing: a replacement idle task,
	// armed from the new deadline.
	require.NotZero(t, idleTaskCount(env.Registry, env.Node),
		"recording a start invalidated the idle task with no replacement armed, while "+
			"IdleCloseTime still advertises %v: the schedule will never close and the "+
			"stuck-schedule scanner is told otherwise", sched.GetIdleCloseTime().AsTime())
}
