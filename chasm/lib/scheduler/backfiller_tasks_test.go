package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type backfillTestCase struct {
	InitialTriggerRequest     *schedulepb.TriggerImmediatelyRequest
	InitialBackfillRequest    *schedulepb.BackfillRequest
	ExpectedBufferedStarts    int
	ExpectedComplete          bool // asserts the Backfiller is deleted
	ExpectedLastProcessedTime time.Time
	ExpectedAttempt           int

	ValidateInvoker    func(t *testing.T, invoker *scheduler.Invoker)
	ValidateBackfiller func(t *testing.T, backfiller *scheduler.Backfiller)
}

func TestBackfillTask_Validate_MigrationPending(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	backfiller := env.Scheduler.NewImmediateBackfiller(ctx, &schedulepb.TriggerImmediatelyRequest{})
	env.Scheduler.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}
	require.NoError(t, env.CloseTransaction())

	ctx = env.MutableContext()
	schedulerComponent, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	persistedScheduler := schedulerComponent.(*scheduler.Scheduler)
	_, exists := persistedScheduler.Backfillers[backfiller.BackfillId].TryGet(ctx)
	require.True(t, exists)
	require.Empty(t, persistedScheduler.Invoker.Get(ctx).BufferedStarts)
}

func runBackfillTestCase(t *testing.T, env *testEnv, c *backfillTestCase) {
	ctx := env.MutableContext()
	schedComponent, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched := schedComponent.(*scheduler.Scheduler)
	invoker := sched.Invoker.Get(ctx)

	require.False(t, c.InitialBackfillRequest != nil && c.InitialTriggerRequest != nil)
	require.False(t, c.InitialBackfillRequest == nil && c.InitialTriggerRequest == nil)

	var backfiller *scheduler.Backfiller
	if c.InitialTriggerRequest != nil {
		backfiller = sched.NewImmediateBackfiller(ctx, c.InitialTriggerRequest)
	} else {
		backfiller = sched.NewRangeBackfiller(ctx, c.InitialBackfillRequest)
	}

	// The immediate pure task executes automatically during CloseTransaction().
	require.NoError(t, env.CloseTransaction())

	if c.ExpectedComplete {
		_, ok := sched.Backfillers[backfiller.BackfillId].TryGet(ctx)
		require.False(t, ok)
	} else {
		// TODO - check the continuation pure task exists once CHASM offers task generation test hooks.

		require.Equal(t, int64(c.ExpectedAttempt), backfiller.GetAttempt())
		require.Equal(t, c.ExpectedLastProcessedTime.UTC(), backfiller.GetLastProcessedTime().AsTime())
	}

	require.Len(t, invoker.GetBufferedStarts(), c.ExpectedBufferedStarts)

	for _, start := range invoker.GetBufferedStarts() {
		require.Equal(t, start.WorkflowId, invoker.RunningWorkflowID(start.RequestId))
	}

	if c.ValidateInvoker != nil {
		c.ValidateInvoker(t, invoker)
	}
	if c.ValidateBackfiller != nil {
		c.ValidateBackfiller(t, backfiller)
	}
}

func TestBackfillTask_TriggerImmediate(t *testing.T) {
	env := newTestEnv(t)
	request := &schedulepb.TriggerImmediatelyRequest{
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	runBackfillTestCase(t, env, &backfillTestCase{
		InitialTriggerRequest:  request,
		ExpectedBufferedStarts: 1,
		ExpectedComplete:       true,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			start := invoker.GetBufferedStarts()[0]
			require.Equal(t, request.OverlapPolicy, start.OverlapPolicy)
			require.True(t, start.Manual)
		},
	})
}

func TestBackfillTask_TriggerImmediateFullBuffer(t *testing.T) {
	env := newTestEnv(t)

	// Backfillers get half of the max buffer size.
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	for range scheduler.DefaultTweakables.MaxBufferSize {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	now := env.TimeSource.Now()
	runBackfillTestCase(t, env, &backfillTestCase{
		InitialTriggerRequest:     &schedulepb.TriggerImmediatelyRequest{},
		ExpectedBufferedStarts:    1000,
		ExpectedComplete:          false,
		ExpectedLastProcessedTime: now,
		ExpectedAttempt:           1,
	})
}

func TestBackfillTask_CompleteFill(t *testing.T) {
	env := newTestEnv(t)
	startTime := env.TimeSource.Now()
	endTime := startTime.Add(5 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(startTime),
		EndTime:       timestamppb.New(endTime),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	runBackfillTestCase(t, env, &backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 5,
		ExpectedComplete:       true,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			for _, start := range invoker.GetBufferedStarts() {
				require.Equal(t, request.OverlapPolicy, start.OverlapPolicy)
				startAt := start.GetActualTime().AsTime()
				require.True(t, startAt.After(startTime))
				require.True(t, startAt.Before(endTime))
				require.True(t, start.Manual)
			}
		},
	})
}

func TestBackfillTask_InclusiveStartEnd(t *testing.T) {
	env := newTestEnv(t)

	backfillTime := env.TimeSource.Now().Truncate(defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(backfillTime),
		EndTime:   timestamppb.New(backfillTime),
	}
	runBackfillTestCase(t, env, &backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 1,
		ExpectedComplete:       true,
	})

	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = nil

	backfillTime = backfillTime.Add(1 * time.Millisecond)
	request = &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(backfillTime),
		EndTime:   timestamppb.New(backfillTime),
	}
	runBackfillTestCase(t, env, &backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 0,
		ExpectedComplete:       true,
	})
}

func TestBackfillTask_BufferCompletelyFull(t *testing.T) {
	env := newTestEnv(t)

	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	for range scheduler.DefaultTweakables.MaxBufferSize {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}

	startTime := env.TimeSource.Now()
	endTime := startTime.Add(5 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}
	runBackfillTestCase(t, env, &backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 1000,
		ExpectedComplete:       false,
		ExpectedAttempt:        1,
		// No progress is recorded, so the unset watermark surfaces as the Unix epoch through AsTime().
		ExpectedLastProcessedTime: time.Unix(0, 0),
	})
}

func TestBackfillTask_PartialFill(t *testing.T) {
	env := newTestEnv(t)

	// 1000 intervals exceeds the backfiller's buffer limit (MaxBufferSize/2 = 500).
	startTime := env.TimeSource.Now()
	endTime := startTime.Add(1000 * defaultInterval)
	request := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(startTime),
		EndTime:       timestamppb.New(endTime),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}

	ctx := env.MutableContext()
	schedComponent, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched := schedComponent.(*scheduler.Scheduler)
	backfiller := sched.NewRangeBackfiller(ctx, request)
	require.NoError(t, env.CloseTransaction())

	require.False(t, backfiller.GetLastProcessedTime().AsTime().IsZero())
	require.Equal(t, int64(1), backfiller.GetAttempt())

	ctx = env.MutableContext()
	schedComponent, err = env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched = schedComponent.(*scheduler.Scheduler)
	_, ok := sched.Backfillers[backfiller.BackfillId].TryGet(ctx)
	require.True(t, ok)

	// The continuation task is scheduled after a backoff delay, so execute it manually.
	invoker := sched.Invoker.Get(ctx)
	invoker.BufferedStarts = nil
	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	err = handler.Execute(ctx, backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{Stamp: backfiller.GetTaskStamp()})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	require.Equal(t, int64(2), backfiller.GetAttempt())
}

// A stall bumps Attempt without writing a watermark, so resume must key on the watermark, not Attempt.
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

	require.Nil(t, backfiller.GetLastProcessedTime(),
		"a fresh range backfiller must not report progress before processing anything")

	// State left by a capacity-only stall: Attempt bumped, watermark unset.
	backfiller.Attempt = 1

	retryResult, err := handler.ProcessBackfill(env.Scheduler, backfiller, scheduler.DefaultTweakables.MaxBufferSize)
	require.NoError(t, err)

	require.Len(t, retryResult.BufferedStarts, want,
		"a capacity-only retry must process the same complete range as an unstalled first attempt")
}
