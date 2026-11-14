package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDefaultCapacityAllowsProgressWithTenBackfillers(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(env.TimeSource.Now()),
		EndTime:   timestamppb.New(env.TimeSource.Now().Add(time.Hour)),
	}
	for range 10 {
		env.Scheduler.NewRangeBackfiller(ctx, request)
	}

	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	limit, err := handler.AllowedBufferedStarts(
		ctx,
		env.Scheduler,
		env.Scheduler.Invoker.Get(ctx),
		scheduler.DefaultTweakables,
	)
	require.NoError(t, err)
	require.Positive(t, limit, "global free capacity must allow at least one backfiller to make progress")
}

// Each backfiller covers a single instant, so it enqueues exactly one start.
// With the default tweakables the shared backfill budget is 450 slots; once
// backfillers outnumber it, an even split rounds every share to zero unless
// admission guarantees one start per backfiller.
func TestBackfillersDrainWhenOutnumberingCapacity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		backfillers int
	}{
		{"backfillers equal to capacity", 450},
		{"one more backfiller than capacity", 451},
		{"backfillers well past capacity", 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
			ts := clock.NewEventTimeSource().Update(now)
			spec := defaultSchedule()
			spec.State.Paused = true
			e := newSchedulerTestEngine(t, spec, withEngineTimeSource(ts))
			require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				for range tc.backfillers {
					s.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
						StartTime:     timestamppb.New(now),
						EndTime:       timestamppb.New(now),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					})
				}
				return nil
			}))

			// Each round drains the buffer, then fires the backfillers' pure tasks.
			enqueued := make(map[string]struct{})
			remaining := tc.backfillers
			for round := 0; round < 50 && remaining > 0; round++ {
				buffered := 0
				require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
					invoker := s.Invoker.Get(ctx)
					buffered = len(invoker.BufferedStarts)
					require.LessOrEqual(t, buffered, 460, "backfills must stay within the shared budget plus retained history")
					for _, start := range invoker.BufferedStarts {
						require.NotContains(t, enqueued, start.RequestId)
						enqueued[start.RequestId] = struct{}{}
					}
					invoker.BufferedStarts = nil
					remaining = len(s.Backfillers)
					return nil
				}))
				if remaining == 0 {
					break
				}
				require.Positive(t, buffered, "round %d: empty buffer with %d backfillers left must make progress", round, remaining)
				ts.Update(ts.Now().Add(time.Hour))
				_, err := e.engine.FirePureTasks(e.rootRef, ts.Now())
				require.NoError(t, err)
			}
			require.Zero(t, remaining)
			require.Len(t, enqueued, tc.backfillers)
		})
	}
}
