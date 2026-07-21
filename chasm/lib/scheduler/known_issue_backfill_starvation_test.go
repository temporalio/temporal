package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
