package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGeneratorJitterGapPreservesPendingAction(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			schedule := defaultSchedule()
			schedule.Spec = &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(15 * time.Minute)}},
				Jitter:   durationpb.New(10 * time.Minute),
			}
			schedule.Policies.OverlapPolicy = policy
			env := newTestEnv(t, withSchedule(schedule))
			ctx := env.MutableContext()
			generator := env.Scheduler.Generator.Get(ctx)
			invoker := env.Scheduler.Invoker.Get(ctx)
			handler := newGeneratorHandler(env)
			start := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second)
			generator.LastProcessedTime = timestamppb.New(start)
			env.Scheduler.Info.UpdateTime = timestamppb.New(start)
			next, err := env.SpecProcessor.NextTime(env.Scheduler, start)
			require.NoError(t, err)
			require.True(t, next.Next.After(next.Nominal))

			// A completion or other event can wake the generator before the
			// pending action's jittered fire time, when nothing is due yet.
			env.TimeSource.Update(next.Nominal.Add(next.Next.Sub(next.Nominal) / 2))
			require.NoError(t, handler.Execute(env.MutableContext(), generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
			require.Empty(t, invoker.BufferedStarts)
			require.Equal(t, start, generator.LastProcessedTime.AsTime())

			// The regular timer must still generate that action exactly once.
			env.TimeSource.Update(next.Next)
			require.NoError(t, handler.Execute(env.MutableContext(), generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
			require.Len(t, invoker.BufferedStarts, 1)
			require.Equal(t, next.Nominal, invoker.BufferedStarts[0].NominalTime.AsTime())
			require.Equal(t, next.Next, invoker.BufferedStarts[0].ActualTime.AsTime())
			require.Equal(t, next.Next, generator.LastProcessedTime.AsTime())
			require.NoError(t, handler.Execute(env.MutableContext(), generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
			require.Len(t, invoker.BufferedStarts, 1)
		})
	}
}

func TestGeneratorRetiresObsoleteTimerWithoutSkippingPendingAction(t *testing.T) {
	t.Parallel()
	schedule := defaultSchedule()
	schedule.Spec.Interval[0].Phase = durationpb.New(30 * time.Second)
	env := newTestEnv(t, withSchedule(schedule))
	generator := env.Scheduler.Generator.Get(env.MutableContext())
	invoker := env.Scheduler.Invoker.Get(env.MutableContext())
	handler := newGeneratorHandler(env)
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	updateTime := base.Add(45 * time.Second)
	generator.LastProcessedTime = timestamppb.New(updateTime)
	env.Scheduler.Info.UpdateTime = timestamppb.New(updateTime)

	// An update changes the next fire from 00:01:00 to 00:01:30, but the
	// timer from the old spec is still queued. Its empty execution must retire it.
	oldTimer := base.Add(time.Minute)
	env.TimeSource.Update(oldTimer)
	attrs := chasm.TaskAttributes{ScheduledTime: oldTimer}
	valid, err := handler.Validate(env.ReadContext(), generator, chasm.TaskInvocation{TaskAttributes: attrs}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)
	require.True(t, valid)
	require.NoError(t, handler.Execute(env.MutableContext(), generator, attrs, &schedulerpb.GeneratorTask{}))
	require.Empty(t, invoker.BufferedStarts)
	require.Equal(t, updateTime, generator.LastProcessedTime.AsTime())
	valid, err = handler.Validate(env.ReadContext(), generator, chasm.TaskInvocation{TaskAttributes: attrs}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)
	require.False(t, valid, "a successful execution must invalidate its scheduled pure task")

	nextFire := base.Add(90 * time.Second)
	env.TimeSource.Update(nextFire)
	attrs.ScheduledTime = nextFire
	valid, err = handler.Validate(env.ReadContext(), generator, chasm.TaskInvocation{TaskAttributes: attrs}, &schedulerpb.GeneratorTask{})
	require.NoError(t, err)
	require.True(t, valid)
	require.NoError(t, handler.Execute(env.MutableContext(), generator, attrs, &schedulerpb.GeneratorTask{}))
	require.Len(t, invoker.BufferedStarts, 1)
	require.Equal(t, nextFire, invoker.BufferedStarts[0].ActualTime.AsTime())
}

func TestGeneratorClockRollbackAndUpdatePreservePendingAction(t *testing.T) {
	t.Parallel()
	schedule := defaultSchedule()
	schedule.Spec.Interval[0].Interval = durationpb.New(time.Hour)
	schedule.Spec.Interval[0].Phase = durationpb.New(5 * time.Minute)
	env := newTestEnv(t, withSchedule(schedule))
	generator := env.Scheduler.Generator.Get(env.MutableContext())
	invoker := env.Scheduler.Invoker.Get(env.MutableContext())
	handler := newGeneratorHandler(env)
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	start := base.Add(-time.Second)
	generator.LastProcessedTime = timestamppb.New(start)
	env.Scheduler.Info.UpdateTime = timestamppb.New(start)

	// An empty wakeup records task progress just after 00:01:00. The old
	// spec's next timer is at 00:05:00. Use sub-millisecond precision to check
	// that the deferred timer aligns with the queue's millisecond precision.
	previousNow := base.Add(time.Minute + 500*time.Microsecond)
	env.TimeSource.Update(previousNow)
	require.NoError(t, handler.Execute(env.MutableContext(), generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{}))
	require.NoError(t, env.CloseTransaction())
	require.Empty(t, invoker.BufferedStarts)
	require.Equal(t, start, generator.LastProcessedTime.AsTime())

	// After the clock rolls back, an update makes a new action due at
	// 00:00:30, before the most recently observed task time.
	env.TimeSource.Update(base.Add(15 * time.Second))
	updated := defaultSchedule()
	updated.Spec.Interval[0].Phase = durationpb.New(30 * time.Second)
	_, err := env.Scheduler.Update(env.MutableContext(), &schedulerpb.UpdateScheduleRequest{
		FrontendRequest: &workflowservice.UpdateScheduleRequest{Schedule: updated},
	})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())
	require.Empty(t, invoker.BufferedStarts, "the pending action must not run before its fire time")

	// Inspect persisted tasks after validation and transaction cleanup. A
	// usable wakeup must remain near the rolled-back period, rather than
	// relying on the old spec's much later timer to revive the schedule.
	type pendingTask struct {
		handler chasm.NodePureTask
		attrs   chasm.TaskAttributes
		task    any
	}
	var pending []pendingTask
	require.NoError(t, env.Node.EachPureTask(previousNow.Add(time.Second), func(
		nodeHandler chasm.NodePureTask, attrs chasm.TaskAttributes, task any,
	) (bool, error) {
		if _, ok := task.(*schedulerpb.GeneratorTask); ok {
			pending = append(pending, pendingTask{nodeHandler, attrs, task})
		}
		return false, nil
	}))
	require.NotEmpty(t, pending, "clock rollback and an update must leave a usable generator timer")
	require.Equal(t, base.Add(time.Minute+time.Millisecond), pending[0].attrs.ScheduledTime)

	// Let the clock reach the retained timer and run it through CHASM's
	// validation and execution path. The new slot must still be generated.
	env.TimeSource.Update(pending[0].attrs.ScheduledTime)
	executed, err := pending[0].handler.ExecutePureTask(t.Context(), pending[0].attrs, pending[0].task)
	require.NoError(t, err)
	require.True(t, executed)
	require.Len(t, invoker.BufferedStarts, 1)
	expectedFire := base.Add(30 * time.Second)
	require.Equal(t, expectedFire, invoker.BufferedStarts[0].NominalTime.AsTime())
	require.Equal(t, expectedFire, invoker.BufferedStarts[0].ActualTime.AsTime())
}
