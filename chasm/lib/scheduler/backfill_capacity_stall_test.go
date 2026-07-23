package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestBackfillCapacityStallDoesNotSkipRange pins that a backfill whose
// first attempt is stalled by a full buffer still reprocesses its complete range
// on retry, rather than skipping the earlier part.
//
// A stalled attempt takes Execute's buffer-full back-off path, which increments
// Attempt (its documented back-off counter) but returns before any watermark is
// written. So the real post-stall state is Attempt > 0 with the high watermark
// left unset. processBackfill must therefore key its resume decision on the
// watermark's presence, not on Attempt: keying on Attempt (the original bug)
// treated the stall as durable progress and resumed from the backfiller's
// creation-time default, skipping the earlier part of the requested range.
func TestBackfillCapacityStallDoesNotSkipRange(t *testing.T) {
	env := newTestEnv(t)
	fixedNow := env.TimeSource.Now().Truncate(defaultInterval)
	env.TimeSource.Update(fixedNow)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(fixedNow.Add(-5 * defaultInterval)),
		EndTime:   timestamppb.New(fixedNow),
	}

	// Number of starts an unstalled first attempt would buffer for the full range.
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

	// A fresh range backfiller must carry no recorded progress: the high watermark
	// doubles as the "progress recorded" signal and stays unset until a batch is
	// actually processed.
	require.Nil(t, backfiller.GetLastProcessedTime(),
		"a fresh range backfiller must not report progress before processing anything")

	// Reproduce the state left by a capacity-only stall: Execute's buffer-full path
	// bumps Attempt (the back-off counter) but records no progress, so the watermark
	// stays unset.
	backfiller.Attempt = 1

	retryResult, err := handler.ProcessBackfill(env.Scheduler, backfiller, scheduler.DefaultTweakables.MaxBufferSize)
	require.NoError(t, err)

	require.Len(t, retryResult.BufferedStarts, want,
		"a capacity-only retry must process the same complete range as an unstalled first attempt")
}
