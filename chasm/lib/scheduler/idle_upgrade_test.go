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

func TestIdle_UpgradeStaleIdleTask_FinalStartFailsStillCloses(t *testing.T) {
	fire := pendingUnpauseTime.Add(idleWeek + time.Hour).Truncate(time.Second)
	e, ts := newPausedThenUnpaused(t, oneShotSpec(fire), 0)
	ts.Update(fire.Add(10 * time.Millisecond))
	// Reconstruct the old generator's final tick without running the fixed
	// generator, which would already arm a replacement idle task. The watermark
	// invalidates the initial generator timer; only the old idle timer remains.
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		g := s.Generator.Get(ctx)
		g.LastProcessedTime = timestamppb.New(fire)
		g.LastProcessedTaskTime = timestamppb.New(ts.Now())
		s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
			ActualTime: timestamppb.New(fire),
			RequestId:  "pending",
			WorkflowId: "pending-workflow",
			Attempt:    1,
		}}
		s.LastEventTime = timestamppb.New(pendingUnpauseTime)
		stale := pendingUnpauseTime.Add(idleWeek)
		s.IdleCloseTime = timestamppb.New(stale)
		ctx.AddTask(s, chasm.TaskAttributes{ScheduledTime: stale}, &schedulerpb.SchedulerIdleTask{
			IdleTimeTotal: durationpb.New(idleWeek),
		})
		return nil
	}))
	firePureTasksAt(t, e, ts, ts.Now())
	// A non-retryable failure removes the start without requesting Generate.
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).BufferedStarts = nil
		return nil
	}))
	firePureTasksAt(t, e, ts, fire.Add(2*idleWeek))
	requireSchedulerState(t, e, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.True(t, s.Closed, "old idle task was invalidated without a replacement")
	})
}
