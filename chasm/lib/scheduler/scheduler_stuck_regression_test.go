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

func TestIdleDeadlineRearmedAfterRecordingStart(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()

	// An exhausted limited schedule makes the Generator arm the idle task rather than another tick.
	ctx := env.MutableContext()
	sched := env.Scheduler
	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 0
	sched.ConflictToken++

	// Attempt == 1 is what recordProcessBufferResult sets before ExecuteTask, and keeps it out of ProcessBuffer.
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

	// Confirm the deadline shifted, so a failure below is about the missing re-arm.
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

	// A replacement idle task armed from the new deadline must keep a path to closing.
	require.NotZero(t, idleTaskCount(env.Registry, env.Node),
		"recording a start invalidated the idle task with no replacement armed, while "+
			"IdleCloseTime still advertises %v: the schedule will never close and the "+
			"stuck-schedule scanner is told otherwise", sched.GetIdleCloseTime().AsTime())
}
