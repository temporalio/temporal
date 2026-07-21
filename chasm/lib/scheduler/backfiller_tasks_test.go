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
	"go.temporal.io/server/common/backoff"
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

type retryPolicyRecorder struct {
	attempts []int
}

func (p *retryPolicyRecorder) ComputeNextDelay(_ time.Duration, attempts int, _ error) time.Duration {
	p.attempts = append(p.attempts, attempts)
	return time.Second
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

	// Exactly one type of request can be set per Backfiller.
	require.False(t, c.InitialBackfillRequest != nil && c.InitialTriggerRequest != nil)
	require.False(t, c.InitialBackfillRequest == nil && c.InitialTriggerRequest == nil)

	// Spawn backfiller.
	var backfiller *scheduler.Backfiller
	if c.InitialTriggerRequest != nil {
		backfiller = sched.NewImmediateBackfiller(ctx, c.InitialTriggerRequest)
	} else {
		backfiller = sched.NewRangeBackfiller(ctx, c.InitialBackfillRequest)
	}

	// Either type of request will spawn a Backfiller and schedule an immediate pure task.
	// The immediate task executes automatically during CloseTransaction().
	require.NoError(t, env.CloseTransaction())

	// Validate completion or partial progress.
	if c.ExpectedComplete {
		// Backfiller should no longer be present in the backfiller map.
		_, ok := sched.Backfillers[backfiller.BackfillId].TryGet(ctx)
		require.False(t, ok)
	} else {
		// TODO - check that a pure task to continue driving backfill exists here. Because
		// a pure task in the tree already has the physically-created status, closing the
		// transaction won't call our backend mock for AddTasks twice. Fix this when CHASM
		// offers unit testing hooks for task generation.

		require.Equal(t, int64(c.ExpectedAttempt), backfiller.GetAttempt())
		require.Equal(t, c.ExpectedLastProcessedTime.UTC(), backfiller.GetLastProcessedTime().AsTime())
	}

	// Validate BufferedStarts. More detailed validation must be done in the callbacks.
	require.Len(t, invoker.GetBufferedStarts(), c.ExpectedBufferedStarts)

	// Validate RequestId -> WorkflowId mapping.
	for _, start := range invoker.GetBufferedStarts() {
		require.Equal(t, start.WorkflowId, invoker.RunningWorkflowID(start.RequestId))
	}

	// Callbacks.
	if c.ValidateInvoker != nil {
		c.ValidateInvoker(t, invoker)
	}
	if c.ValidateBackfiller != nil {
		c.ValidateBackfiller(t, backfiller)
	}
}

// An immediately-triggered run should result in the machine being deleted after
// completion.
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

// An immediately-triggered run will back off and retry if the buffer is full.
func TestBackfillTask_TriggerImmediateFullBuffer(t *testing.T) {
	env := newTestEnv(t)

	// Backfillers get half of the max buffer size, so fill (half the buffer -
	// expected starts).
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

// A backfill request completes entirely should result in the machine being
// deleted after completion.
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

// Backfill start and end times are inclusive, so a backfill scheduled for an
// instant that exactly matches a time in the calendar spec's sequence should result
// in a start.
func TestBackfillTask_InclusiveStartEnd(t *testing.T) {
	env := newTestEnv(t)

	// Set an identical start and end time, landing on the calendar spec's interval.
	backfillTime := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	request := &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(backfillTime),
		EndTime:   timestamppb.New(backfillTime),
	}
	runBackfillTestCase(t, env, &backfillTestCase{
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 1,
		ExpectedComplete:       true,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.True(t, backfillTime.Equal(invoker.GetBufferedStarts()[0].GetActualTime().AsTime()))
		},
	})

	// Clear the Invoker's buffered starts.
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = nil

	// A hair off and the action won't fire.
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

func TestBackfillTask_MigratedExclusiveCursor(t *testing.T) {
	env := newTestEnv(t)
	cursor := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	ctx := env.MutableContext()
	schedComponent, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched := schedComponent.(*scheduler.Scheduler)
	backfiller := sched.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(cursor),
		EndTime:   timestamppb.New(cursor.Add(defaultInterval)),
	})
	// This is the persisted state produced by the V1-to-CHASM migration: V1's
	// StartTime has already been buffered and is an exclusive cursor.
	backfiller.LastProcessedTime = timestamppb.New(cursor)
	backfiller.Attempt = 0
	backfiller.Progress = schedulerpb.BACKFILLER_PROGRESS_CURSOR_EXCLUSIVE

	require.NoError(t, env.CloseTransaction())

	invoker := sched.Invoker.Get(ctx)
	require.Len(t, invoker.GetBufferedStarts(), 1)
	require.True(t, cursor.Add(defaultInterval).Equal(invoker.GetBufferedStarts()[0].GetActualTime().AsTime()))
}

func TestBackfillTask_MigratedCursorDoesNotAdvanceRetryBackoff(t *testing.T) {
	env := newTestEnv(t)
	cursor := env.TimeSource.Now().Truncate(defaultInterval)

	ctx := env.MutableContext()
	schedComponent, err := env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched := schedComponent.(*scheduler.Scheduler)
	invoker := sched.Invoker.Get(ctx)
	for range scheduler.DefaultTweakables.MaxBufferSize {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
	}
	backfiller := sched.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
		StartTime: timestamppb.New(cursor),
		EndTime:   timestamppb.New(cursor.Add(defaultInterval)),
	})
	backfiller.LastProcessedTime = timestamppb.New(cursor)
	backfiller.Attempt = 0
	backfiller.Progress = schedulerpb.BACKFILLER_PROGRESS_CURSOR_EXCLUSIVE
	sched.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}
	require.NoError(t, env.CloseTransaction())

	ctx = env.MutableContext()
	schedComponent, err = env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched = schedComponent.(*scheduler.Scheduler)
	backfiller = sched.Backfillers[backfiller.BackfillId].Get(ctx)
	sched.WorkflowMigration = nil

	retryPolicy := &retryPolicyRecorder{}
	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config: &scheduler.Config{
			Tweakables:                      defaultConfig().Tweakables,
			ServiceCallTimeout:              defaultConfig().ServiceCallTimeout,
			EncodeInternalTokenWithEnvelope: defaultConfig().EncodeInternalTokenWithEnvelope,
			RetryPolicy: func() backoff.RetryPolicy {
				return retryPolicy
			},
		},
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	require.NoError(t, handler.Execute(ctx, backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{}))

	require.Equal(t, int64(1), backfiller.GetAttempt())
	require.Equal(t, []int{1}, retryPolicy.attempts)
}

// When the buffer's completely full, the high watermark shouldn't advance and no
// starts should be buffered.
func TestBackfillTask_BufferCompletelyFull(t *testing.T) {
	env := newTestEnv(t)

	// Fill buffer past max.
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
		InitialBackfillRequest:    request,
		ExpectedBufferedStarts:    1000,
		ExpectedComplete:          false,
		ExpectedAttempt:           1,
		ExpectedLastProcessedTime: startTime,
	})
}

// When the backfill range exceeds buffer capacity, partial filling should occur
// with the remainder left for a retry.
func TestBackfillTask_PartialFill(t *testing.T) {
	env := newTestEnv(t)

	// Use a large backfill range (1000 intervals) that exceeds the backfiller's
	// buffer limit (MaxBufferSize/2 = 500).
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

	// Backfiller should have processed up to its limit (500), not the full 1000.
	require.False(t, backfiller.GetLastProcessedTime().AsTime().IsZero())
	require.Equal(t, int64(1), backfiller.GetAttempt())

	// Backfiller should still exist (not complete).
	ctx = env.MutableContext()
	schedComponent, err = env.Node.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	sched = schedComponent.(*scheduler.Scheduler)
	_, ok := sched.Backfillers[backfiller.BackfillId].TryGet(ctx)
	require.True(t, ok)

	// Manually execute the second iteration since the scheduled continuation
	// task is in the future (after backoff delay).
	invoker := sched.Invoker.Get(ctx)
	invoker.BufferedStarts = nil // Clear to make room for next batch
	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
	err = handler.Execute(ctx, backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	// After second iteration, should have processed another batch.
	require.Equal(t, int64(2), backfiller.GetAttempt())
}
