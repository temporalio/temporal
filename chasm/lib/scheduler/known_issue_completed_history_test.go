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

func TestKnownIssue_CompletedHistoryDoesNotConsumeBackfillCapacity(t *testing.T) {
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
