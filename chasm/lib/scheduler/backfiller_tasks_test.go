package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
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
		InitialBackfillRequest: request,
		ExpectedBufferedStarts: 1000,
		ExpectedComplete:       false,
		ExpectedAttempt:        1,
		// A completely-full-buffer attempt backs off (Attempt increments) but records
		// no progress: the range backfiller's high watermark stays unset, which
		// surfaces as the Unix epoch through AsTime(). (Previously the watermark was
		// seeded to creation time, which a retry then mistook for durable progress.)
		ExpectedLastProcessedTime: time.Unix(0, 0),
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

// TestBackfillCapacityStallDoesNotSkipRange pins that a backfill whose first
// attempt is stalled by a full buffer still reprocesses its complete range on
// retry, rather than skipping the earlier part.
//
// A stalled attempt takes Execute's buffer-full back-off path, which increments
// Attempt (its documented back-off counter) but returns before any watermark is
// written. So the real post-stall state is Attempt > 0 with the high watermark
// left unset. processBackfill must therefore key its resume decision on the
// watermark's presence, not on Attempt: keying on Attempt (the original bug)
// treated the stall as durable progress and resumed from the backfiller's
// creation-time default, skipping the earlier part of the requested range.
//
// This drives the stall and the retry through the real Execute/Validate pair via
// the CHASM test engine (rather than calling processBackfill directly), so a
// regression in Validate would fail this test too.
func TestBackfillCapacityStallDoesNotSkipRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	specProcessor := newRealSpecProcessor(ctrl, logger)
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))

	ts := clock.NewEventTimeSource()
	fixedNow := time.Now().Truncate(defaultInterval)
	ts.Update(fixedNow)
	tv := testvars.New(t)
	testEngine := chasmtest.NewEngine(t, registry,
		chasmtest.WithTimeSource(ts),
		chasmtest.WithNodeBackendDecorator(func(b *chasm.MockNodeBackend) {
			b.HandleGetNamespaceEntry = tv.Namespace
		}),
	)
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)

	key := chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID}
	_, err := chasm.StartExecution(engineCtx, key,
		func(mc chasm.MutableContext, _ any) (*scheduler.Scheduler, error) {
			sched, err := scheduler.NewScheduler(mc, namespace, namespaceID, scheduleID, defaultSchedule(), nil)
			if err != nil {
				return nil, err
			}
			// Pin the Generator's high water mark to fixedNow (matching newTestEnv's
			// setup) so the schedule's own regular ticking doesn't independently fire
			// an action inside our backfill window and skew the buffered-start count.
			sched.Generator.Get(mc).LastProcessedTime = timestamppb.New(fixedNow)
			return sched, nil
		}, nil)
	require.NoError(t, err)
	schedRef := chasm.NewComponentRef[*scheduler.Scheduler](key)

	// ALLOW_ALL, matching TestBackfillTask_CompleteFill/PartialFill above: with the
	// default SKIP policy, each buffered (not yet completed) start would block every
	// later nominal time in the range as "overlapping," collapsing a multi-action
	// backfill down to just its first action.
	request := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(fixedNow.Add(-5 * defaultInterval)),
		EndTime:       timestamppb.New(fixedNow),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}

	// Number of starts an unstalled first attempt would buffer for the full range.
	var want int
	_, err = chasm.ReadComponent(engineCtx, schedRef,
		func(sched *scheduler.Scheduler, _ chasm.Context, _ any) (any, error) {
			result, err := specProcessor.ProcessTimeRange(
				sched,
				request.StartTime.AsTime().Add(-time.Millisecond),
				request.EndTime.AsTime(),
				request.GetOverlapPolicy(),
				sched.WorkflowID(),
				"expected",
				true,
				nil,
			)
			want = len(result.BufferedStarts)
			return nil, err
		}, nil)
	require.NoError(t, err)
	require.Positive(t, want)

	// Fill the buffer to capacity and create the backfiller in the same
	// transaction, so the immediate first-attempt task (auto-executed by
	// CloseTransaction, same as production) genuinely stalls on a full buffer
	// rather than a synthetic Attempt bump.
	var backfiller *scheduler.Backfiller
	_, _, err = chasm.UpdateComponent(engineCtx, schedRef,
		func(sched *scheduler.Scheduler, mc chasm.MutableContext, _ any) (any, error) {
			invoker := sched.Invoker.Get(mc)
			for range scheduler.DefaultTweakables.MaxBufferSize {
				invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
			}
			backfiller = sched.NewRangeBackfiller(mc, request)
			return nil, nil
		}, nil)
	require.NoError(t, err)

	require.Equal(t, int64(1), backfiller.GetAttempt(),
		"the stalled first attempt must still count as an attempt")
	require.Nil(t, backfiller.GetLastProcessedTime(),
		"a capacity-only stall must not record progress")

	// Free up the buffer, then drive the retry through the real Execute/Validate
	// pair, exactly as the framework would dispatch it.
	_, _, err = chasm.UpdateComponent(engineCtx, schedRef,
		func(sched *scheduler.Scheduler, mc chasm.MutableContext, _ any) (any, error) {
			sched.Invoker.Get(mc).BufferedStarts = nil
			return nil, nil
		}, nil)
	require.NoError(t, err)

	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
	})
	taskDropped, err := chasmtest.ExecutePureTask(
		engineCtx, testEngine, backfiller, handler, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{},
	)
	require.NoError(t, err)
	require.False(t, taskDropped)

	_, err = chasm.ReadComponent(engineCtx, schedRef,
		func(sched *scheduler.Scheduler, ctx chasm.Context, _ any) (any, error) {
			_, exists := sched.Backfillers[backfiller.BackfillId].TryGet(ctx)
			require.False(t, exists,
				"a capacity-only retry must process the complete range and delete the Backfiller")
			require.Len(t, sched.Invoker.Get(ctx).BufferedStarts, want,
				"a capacity-only retry must process the same complete range as an unstalled first attempt")
			return nil, nil
		}, nil)
	require.NoError(t, err)
}
