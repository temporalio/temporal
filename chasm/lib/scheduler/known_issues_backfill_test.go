package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestKnownIssue_BackfillCapacityStallDoesNotSkipRange(t *testing.T) {
	env := newTestEnv(t)
	fixedNow := env.TimeSource.Now().Truncate(defaultInterval)
	env.TimeSource.Update(fixedNow)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(fixedNow.Add(-5 * defaultInterval)),
		EndTime:   timestamppb.New(fixedNow),
	}
	result, err := env.SpecProcessor.ProcessTimeRange(
		env.Scheduler,
		request.StartTime.AsTime().Add(-time.Millisecond),
		request.EndTime.AsTime(),
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		env.Scheduler.WorkflowID(),
		"expected",
		true,
		nil,
	)
	require.NoError(t, err)
	want := len(result.BufferedStarts)
	require.Positive(t, want)

	ctx := env.MutableContext()
	backfiller := env.Scheduler.NewRangeBackfiller(ctx, request)
	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	backfiller.Attempt = 1
	backfiller.LastProcessedTime = timestamppb.New(fixedNow)
	retryResult, err := handler.ProcessBackfill(env.Scheduler, backfiller, scheduler.DefaultTweakables.MaxBufferSize)
	require.NoError(t, err)

	require.Len(t, retryResult.BufferedStarts, want,
		"a capacity-only retry must process the same complete range as an unstalled first attempt")
}

func TestKnownIssue_DefaultCapacityAllowsProgressWithTenBackfillers(t *testing.T) {
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
