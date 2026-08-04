package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type backfillerState struct {
	present           bool
	lastProcessedTime time.Time
	attempt           int64
}

// TestBackfiller_ProcessedContinuationDoesNotRunAgain covers the historical
// range failure from SCH-074. Once a continuation advances the range and
// schedules its replacement, firing tasks at the same time must not execute
// the processed continuation again.
func TestBackfiller_ProcessedContinuationDoesNotRunAgain(t *testing.T) {
	// Use the CHASM test engine and a controllable clock so each task execution
	// crosses the same transaction boundary as a production task.
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	specProcessor := scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})

	// Create a historical range large enough that the immediate task can only
	// process its first batch and must leave a delayed continuation.
	now := timeSource.Now()
	handler := scheduler.NewTestHandler(logger)
	_, err := handler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			InitialPatch: &schedulepb.SchedulePatch{
				BackfillRequest: []*schedulepb.BackfillRequest{{
					StartTime:     timestamppb.New(now.Add(-5000 * defaultInterval)),
					EndTime:       timestamppb.New(now.Add(-1 * defaultInterval)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}},
			},
			RequestId: "req-create",
		},
	})
	require.NoError(t, err)

	// Confirm schedule creation processed one batch and retained the Backfiller.
	var afterCreate backfillerState
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if backfiller, ok := field.TryGet(ctx); ok {
					afterCreate.present = true
					afterCreate.lastProcessedTime = backfiller.GetLastProcessedTime().AsTime()
					afterCreate.attempt = backfiller.GetAttempt()
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.True(t, afterCreate.present, "backfiller should still be processing")
	require.Equal(t, int64(1), afterCreate.attempt)

	// Free the Invoker capacity consumed by the first batch, then run the
	// delayed continuation once.
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.Invoker.Get(ctx).BufferedStarts = nil
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	fireTime := now.Add(1 * time.Hour)
	timeSource.Update(fireTime)
	_, err = engine.FirePureTasks(rootRef, fireTime)
	require.NoError(t, err)

	// The valid continuation must process exactly one more batch and advance
	// the historical range cursor.
	var afterFirst backfillerState
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if backfiller, ok := field.TryGet(ctx); ok {
					afterFirst.present = true
					afterFirst.lastProcessedTime = backfiller.GetLastProcessedTime().AsTime()
					afterFirst.attempt = backfiller.GetAttempt()
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.True(t, afterFirst.present)
	require.Equal(t, int64(2), afterFirst.attempt, "continuation should have executed once")
	require.True(t, afterFirst.lastProcessedTime.After(afterCreate.lastProcessedTime),
		"range cursor should advance on a legitimate continuation")

	// Fire tasks again without advancing time. The replacement is scheduled in
	// the future, so another execution here could only come from the old task.
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.Invoker.Get(ctx).BufferedStarts = nil
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	_, err = engine.FirePureTasks(rootRef, fireTime)
	require.NoError(t, err)

	// Verify the stale task did not consume another attempt or move the cursor.
	var afterSecondFire backfillerState
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if backfiller, ok := field.TryGet(ctx); ok {
					afterSecondFire.present = true
					afterSecondFire.lastProcessedTime = backfiller.GetLastProcessedTime().AsTime()
					afterSecondFire.attempt = backfiller.GetAttempt()
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.Equal(t, afterFirst.attempt, afterSecondFire.attempt,
		"processed continuation must not execute again")
	require.Equal(t, afterFirst.lastProcessedTime, afterSecondFire.lastProcessedTime,
		"processed continuation must not advance the range cursor again")
}

// TestBackfiller_FutureRangeDoesNotStall covers the opposite clock-domain
// failure. A range cursor later than wall-clock time must not invalidate the
// continuations needed to finish the range.
func TestBackfiller_FutureRangeDoesNotStall(t *testing.T) {
	// Use the full task lifecycle so validation runs after each committed batch.
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	specProcessor := scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})

	// Create a forward-dated range that requires several batches. Its range
	// cursor will move ahead of the task's wall-clock scheduled time.
	now := timeSource.Now()
	handler := scheduler.NewTestHandler(logger)
	_, err := handler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			InitialPatch: &schedulepb.SchedulePatch{
				BackfillRequest: []*schedulepb.BackfillRequest{{
					StartTime:     timestamppb.New(now),
					EndTime:       timestamppb.New(now.Add(1000 * defaultInterval)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}},
			},
			RequestId: "req-create",
		},
	})
	require.NoError(t, err)

	// Confirm the immediate task processed only the initial batch.
	var afterCreate backfillerState
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if backfiller, ok := field.TryGet(ctx); ok {
					afterCreate.present = true
					afterCreate.attempt = backfiller.GetAttempt()
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.True(t, afterCreate.present)
	require.Equal(t, int64(1), afterCreate.attempt, "only the first batch should have run at create")

	// Replenish capacity and advance past each backoff. Completion removes the
	// Backfiller child, proving that every continuation remained valid.
	for i := 1; i <= 10; i++ {
		_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				s.Invoker.Get(ctx).BufferedStarts = nil
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)

		fireTime := now.Add(time.Duration(i) * time.Hour)
		timeSource.Update(fireTime)
		_, err = engine.FirePureTasks(rootRef, fireTime)
		require.NoError(t, err)

		present := false
		_, err = chasm.ReadComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
				for _, field := range s.Backfillers {
					if _, ok := field.TryGet(ctx); ok {
						present = true
					}
				}
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
		if !present {
			return
		}
	}

	require.Fail(t, "backfill should complete, but the continuation was invalidated and the range stalled")
}

// TestBackfiller_Validate_AcceptsOnlyCurrentGeneration pins the steady-state
// generation fence. Validation must accept the current task, reject both stale
// and impossible future generations, and invalidate a task when it is replaced.
func TestBackfiller_Validate_AcceptsOnlyCurrentGeneration(t *testing.T) {
	// Create the schedule through the live handler API so the Backfiller and its
	// initial task are produced by the same path used in production.
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	specProcessor := scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})

	now := timeSource.Now()
	createHandler := scheduler.NewTestHandler(logger)
	_, err := createHandler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			InitialPatch: &schedulepb.SchedulePatch{
				BackfillRequest: []*schedulepb.BackfillRequest{{
					StartTime:     timestamppb.New(now.Add(-5000 * defaultInterval)),
					EndTime:       timestamppb.New(now.Add(-1 * defaultInterval)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}},
			},
			RequestId: "req-create",
		},
	})
	require.NoError(t, err)

	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
	})

	// Resolve the committed Backfiller and validate task generations against
	// the state created by the initial immediate execution.
	var backfiller *scheduler.Backfiller
	var gen int64
	var currentValid, staleValid, futureValid bool
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					backfiller = candidate
					gen = candidate.GetTaskGeneration()
					currentValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Generation: gen})
					if err != nil {
						return struct{}{}, err
					}
					staleValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Generation: gen - 1})
					if err != nil {
						return struct{}{}, err
					}
					futureValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Generation: gen + 1})
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotNil(t, backfiller)
	require.Positive(t, gen, "scheduling a task must have advanced the generation")
	require.True(t, currentValid, "the current generation must be accepted")
	require.False(t, staleValid, "an older (superseded/redelivered) generation must be rejected")
	require.False(t, futureValid, "a generation ahead of the component must be rejected")

	// Restore capacity through an engine update, then deliver the current task
	// through the CHASM pure-task lifecycle.
	currentTask := &schedulerpb.BackfillerTask{Generation: gen}
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.Invoker.Get(ctx).BufferedStarts = nil
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	dropped, err := chasmtest.ExecutePureTask(
		context.Background(), engine, backfiller, handler, chasm.TaskAttributes{}, currentTask)
	require.NoError(t, err)
	require.False(t, dropped)

	// The exact payload that executed must now be invalid, while the successor
	// generation remains executable.
	var nextGeneration int64
	var executedValid, successorValid bool
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					nextGeneration = candidate.GetTaskGeneration()
					executedValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{}, currentTask)
					if err != nil {
						return struct{}{}, err
					}
					successorValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Generation: nextGeneration})
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.Greater(t, nextGeneration, gen)
	require.False(t, executedValid, "the executed generation must become invalid")
	require.True(t, successorValid, "the newly-scheduled generation must be valid")
}

// TestBackfiller_Validate_LegacyContinuation covers a rolling-upgrade handoff.
// It models an old binary preserving the generation field, incrementing Attempt,
// and scheduling a generation-zero successor. The new binary must accept that
// successor once and restore the generation fence when it executes.
func TestBackfiller_Validate_LegacyContinuation(t *testing.T) {
	// Create the schedule through the live handler API and let its immediate
	// Backfiller task produce the first persisted continuation.
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	specProcessor := scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})

	now := timeSource.Now()
	createHandler := scheduler.NewTestHandler(logger)
	_, err := createHandler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			InitialPatch: &schedulepb.SchedulePatch{
				BackfillRequest: []*schedulepb.BackfillRequest{{
					StartTime:     timestamppb.New(now.Add(-5000 * defaultInterval)),
					EndTime:       timestamppb.New(now.Add(-1 * defaultInterval)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}},
			},
			RequestId: "req-create",
		},
	})
	require.NoError(t, err)

	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
	})

	// Model the old binary successfully processing the generated task. It knows
	// about Attempt, but preserves the unknown TaskGeneration state field.
	var backfiller *scheduler.Backfiller
	var generation int64
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					backfiller = candidate
					generation = candidate.GetTaskGeneration()
					candidate.Attempt++
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotNil(t, backfiller)

	// The attempt increment makes the already-processed generated task stale.
	currentTask := &schedulerpb.BackfillerTask{Generation: generation}
	legacyTask := &schedulerpb.BackfillerTask{}
	var currentValid, legacyValid bool
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					currentValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{}, currentTask)
					if err != nil {
						return struct{}{}, err
					}
					legacyValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{}, legacyTask)
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.False(t, currentValid, "the task processed by the old binary must be stale")
	require.True(t, legacyValid, "the old binary's continuation must remain executable")

	// Executing the legacy task schedules a generated successor and reestablishes
	// the invariant that generation is one ahead of the completed attempt count.
	dropped, err := chasmtest.ExecutePureTask(
		context.Background(), engine, backfiller, handler, chasm.TaskAttributes{}, legacyTask)
	require.NoError(t, err)
	require.False(t, dropped)

	// The same legacy task cannot execute again after the fence is restored.
	var attempt, taskGeneration int64
	var legacyStillValid bool
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					attempt = candidate.GetAttempt()
					taskGeneration = candidate.GetTaskGeneration()
					legacyStillValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{}, legacyTask)
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.Equal(t, attempt+1, taskGeneration)
	require.False(t, legacyStillValid, "executing the legacy continuation must restore the fence")
}
