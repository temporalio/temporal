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
	for i := range 10 {
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

// Non-completed starts consume backfill capacity, the first recentActionCount are discounted, and the result clamps at zero.
func TestAllowedBufferedStartsDiscountsRetainedHistory(t *testing.T) {
	// recentActionCount is 10; one backfiller with MaxBufferSize=20 gives base capacity (20/2)/1 = 10.
	const (
		maxBufferSize     = 20
		recentActionCount = 10
		baseCapacity      = (maxBufferSize / 2) / 1
	)
	cases := []struct {
		name       string
		actionable int
		expected   int
	}{
		{"empty buffer keeps full capacity", 0, baseCapacity},
		{"up to recentActionCount is fully discounted", recentActionCount, baseCapacity},
		{"beyond recentActionCount reduces capacity 1:1", 15, baseCapacity - (15 - recentActionCount)},
		{"buffer full of actionable starts clamps to zero", maxBufferSize, 0},
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
