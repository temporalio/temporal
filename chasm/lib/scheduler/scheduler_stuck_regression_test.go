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

// TestIdleDeadlineRearmedAfterRecordingStart pins the P1 raised in review on
// "Align CHASM ALLOW_ALL lifecycle with V1" (#11631).
//
// Recording a start destroys the schedule's idle timer and does not replace it:
//
//   - The Generator arms a SchedulerIdleTask at D1 = getLastEventTime() +
//     IdleTime, and records D1 in IdleCloseTime.
//   - recordExecuteResult stamps RunId and StartTime onto the BufferedStart.
//     recentActions skips empty-RunId starts, so that stamp is what first
//     exposes the start to getLastEventTime, which maxes over ActualTime
//     (populated from StartTime). The deadline moves to D2 > D1.
//   - SchedulerIdleTaskHandler.Validate recomputes D2, finds it after the D1 the
//     task was armed for, and drops the task as expiration_shift.
//   - recordExecuteResult ends at i.addTasks(ctx), which arms only invoker
//     tasks. Only Generator.Generate arms an idle task, and there is no Generate
//     call anywhere in invoker.go or invoker_tasks.go.
//
// The schedule is then open with nothing that can ever close it, while
// IdleCloseTime - the ScheduleIdleCloseTime search attribute the stuck-schedule
// scanner reads - still advertises D1.
//
// The completion path already guards this hazard: Scheduler.recordCompletedAction
// calls Generate immediately after recording, commenting that "additional events
// invalidate in-flight idle tasks" (scheduler.go). The start path does not, so on
// main the schedule stays stuck until a completion callback happens to arrive.
// #11631 attaches no callback to ALLOW_ALL actions, which makes the gap permanent.
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
