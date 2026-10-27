package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
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
