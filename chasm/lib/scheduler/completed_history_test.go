package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCompletedHistoryDoesNotConsumeBackfillCapacity(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	env.Scheduler.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(env.TimeSource.Now()),
		EndTime:   timestamppb.New(env.TimeSource.Now().Add(time.Hour)),
	})
	invoker := env.Scheduler.Invoker.Get(ctx)
	for i := range scheduler.RecentActionCount {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId: fmt.Sprintf("completed-%d", i),
			Completed: &schedulespb.CompletedResult{
				CloseTime: timestamppb.New(env.TimeSource.Now()),
			},
		})
	}
	tweakables := scheduler.DefaultTweakables
	tweakables.MaxBufferSize = 20
	tweakables.GeneratorBufferReserveSize = 0

	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	limit, err := handler.AllowedBufferedStarts(ctx, env.Scheduler, invoker, tweakables)
	require.NoError(t, err)
	require.Positive(t, limit, "retained completed actions must not consume actionable buffer capacity")
}

// TestAllowedBufferedStartsCountsOnlyActionableWork pins the corrected
// allowedBufferedStarts arithmetic: actionable (non-completed) buffered starts
// always consume backfill capacity 1:1, with no free discount by count, while
// retained completed history -- regardless of how many entries are actually
// present -- never consumes any capacity. This guards against reintroducing a
// flat recentActionCount-sized discount: subtracting the retention cap
// unconditionally (instead of the live completed count) let MaxBufferSize be
// exceeded whenever the buffer held fewer completions than the cap allowed,
// e.g. a fresh buffer with zero completions and recentActionCount actionable
// starts was wrongly treated as having recentActionCount of free capacity.
func TestAllowedBufferedStartsCountsOnlyActionableWork(t *testing.T) {
	// With one backfiller, MaxBufferSize=20 and no generator reserve, base
	// capacity is (20/2)/1 = 10.
	const (
		maxBufferSize = 20
		baseCapacity  = (maxBufferSize / 2) / 1
	)
	cases := []struct {
		name       string
		completed  int
		actionable int
		expected   int
	}{
		{"empty buffer keeps full capacity", 0, 0, baseCapacity},
		{"actionable starts cost capacity 1:1, no free discount", 0, 5, baseCapacity - 5},
		{"actionable starts at recentActionCount still cost 1:1", 0, scheduler.RecentActionCount, baseCapacity - scheduler.RecentActionCount},
		{"completed history never costs capacity, however many entries", scheduler.RecentActionCount, 0, baseCapacity},
		{"completed history and actionable work are counted independently", scheduler.RecentActionCount, 5, baseCapacity - 5},
		{"buffer full of actionable starts clamps to zero", 0, maxBufferSize, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			ctx := env.MutableContext()
			env.Scheduler.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
				StartTime: timestamppb.New(env.TimeSource.Now()),
				EndTime:   timestamppb.New(env.TimeSource.Now().Add(time.Hour)),
			})
			invoker := env.Scheduler.Invoker.Get(ctx)
			for i := range tc.completed {
				invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
					RequestId: fmt.Sprintf("completed-%d", i),
					Completed: &schedulespb.CompletedResult{
						CloseTime: timestamppb.New(env.TimeSource.Now()),
					},
				})
			}
			for i := range tc.actionable {
				invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
					RequestId: fmt.Sprintf("pending-%d", i),
				})
			}
			tweakables := scheduler.DefaultTweakables
			tweakables.MaxBufferSize = maxBufferSize
			tweakables.GeneratorBufferReserveSize = 0

			handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
				Config:         defaultConfig(),
				MetricsHandler: metrics.NoopMetricsHandler,
				BaseLogger:     env.Logger,
				SpecProcessor:  env.SpecProcessor,
			})
			limit, err := handler.AllowedBufferedStarts(ctx, env.Scheduler, invoker, tweakables)
			require.NoError(t, err)
			require.Equal(t, tc.expected, limit)
		})
	}
}
