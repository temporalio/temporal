package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
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
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine, engineCtx := newTestEngineContext(t, logger, withEngineSpecProcessor(specProcessor), withEngineTimeSource(timeSource))
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
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine, engineCtx := newTestEngineContext(t, logger, withEngineSpecProcessor(specProcessor), withEngineTimeSource(timeSource))
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

// TestBackfiller_Validate_AcceptsOnlyCurrentStamp pins the steady-state stamp
// validation. It must accept the current task, reject stale and impossible
// future stamps, and make a replaced task stale.
func TestBackfiller_Validate_AcceptsOnlyCurrentStamp(t *testing.T) {
	// Create the schedule through the live handler API so the Backfiller and its
	// initial task are produced by the same path used in production.
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	specProcessor := scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine, engineCtx := newTestEngineContext(t, logger, withEngineSpecProcessor(specProcessor), withEngineTimeSource(timeSource))
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

	// Resolve the committed Backfiller and validate task stamps against
	// the state created by the initial immediate execution.
	var backfiller *scheduler.Backfiller
	var stamp int64
	var currentValid, staleValid, futureValid bool
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					backfiller = candidate
					stamp = candidate.GetTaskStamp()
					currentValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Stamp: stamp})
					if err != nil {
						return struct{}{}, err
					}
					staleValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Stamp: stamp - 1})
					if err != nil {
						return struct{}{}, err
					}
					futureValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Stamp: stamp + 1})
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotNil(t, backfiller)
	require.Positive(t, stamp, "scheduling a task must have advanced the stamp")
	require.True(t, currentValid, "the current stamp must be accepted")
	require.False(t, staleValid, "an older/redelivered stamp must be rejected")
	require.False(t, futureValid, "a stamp ahead of the component must be rejected")

	// Restore capacity through an engine update, then deliver the current task
	// through the CHASM pure-task lifecycle.
	currentTask := &schedulerpb.BackfillerTask{Stamp: stamp}
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
	// stamp remains executable.
	var nextStamp int64
	var executedValid, successorValid bool
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					nextStamp = candidate.GetTaskStamp()
					executedValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{}, currentTask)
					if err != nil {
						return struct{}{}, err
					}
					successorValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Stamp: nextStamp})
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.Greater(t, nextStamp, stamp)
	require.False(t, executedValid, "the executed stamp must become invalid")
	require.True(t, successorValid, "the newly-scheduled stamp must be valid")
}

// TestBackfiller_LegacyTaskSchedulesStampedSuccessor covers a rolling-upgrade handoff.
func TestBackfiller_LegacyTaskSchedulesStampedSuccessor(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	specProcessor := scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine, engineCtx := newTestEngineContext(t, logger, withEngineSpecProcessor(specProcessor), withEngineTimeSource(timeSource))
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})

	now := timeSource.Now()
	request := &schedulepb.BackfillRequest{
		StartTime:     timestamppb.New(now.Add(-5000 * defaultInterval)),
		EndTime:       timestamppb.New(now.Add(-1 * defaultInterval)),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}
	createHandler := scheduler.NewTestHandler(logger)
	_, err := createHandler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
			RequestId:  "req-create",
		},
	})
	require.NoError(t, err)

	handler := scheduler.NewBackfillerTaskHandler(scheduler.BackfillerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
	})

	// Make task N hit capacity before recording progress.
	var backfiller *scheduler.Backfiller
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			invoker := s.Invoker.Get(ctx)
			for range scheduler.DefaultTweakables.MaxBufferSize {
				invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{})
			}
			backfiller = s.NewRangeBackfiller(ctx, request)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotNil(t, backfiller)
	require.Nil(t, backfiller.GetLastProcessedTime())

	legacyTaskAttrs := chasm.TaskAttributes{ScheduledTime: now}

	// Arrange the persisted state at the handoff boundary directly: Attempt has
	// advanced to the current stamp and a zero-stamp task is ready for the new
	// code.
	var stamp, attemptBeforeWake int64
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.Invoker.Get(ctx).BufferedStarts = nil
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					backfiller = candidate
					stamp = candidate.GetTaskStamp()
					candidate.Attempt++
					attemptBeforeWake = candidate.GetAttempt()
					ctx.AddTask(candidate, legacyTaskAttrs, &schedulerpb.BackfillerTask{})
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotNil(t, backfiller)

	// The new validator rejects the stale stamped task and accepts the zero-stamp
	// legacy task.
	currentTask := &schedulerpb.BackfillerTask{Stamp: stamp}
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
	require.False(t, currentValid, "the stamped task must be stale")
	require.True(t, legacyValid, "the zero-stamp task must remain executable")

	// Execute the old task with the new handler through the CHASM lifecycle. It
	// only schedules a stamped successor; it must not process the backfill range.
	executed, err := engine.FirePureTasks(rootRef, now)
	require.NoError(t, err)
	require.Equal(t, 1, executed)

	// The migration wake-up keeps the range unprocessed, does not increment
	// Attempt, and invalidates all zero-stamp tasks after scheduling the successor.
	var attempt, taskStamp int64
	var legacyStillValid, successorValid bool
	var actualTimes []time.Time
	var lastProcessedTime *timestamppb.Timestamp
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, start := range s.Invoker.Get(ctx).GetBufferedStarts() {
				actualTimes = append(actualTimes, start.GetActualTime().AsTime())
			}
			for _, field := range s.Backfillers {
				if candidate, ok := field.TryGet(ctx); ok {
					attempt = candidate.GetAttempt()
					taskStamp = candidate.GetTaskStamp()
					lastProcessedTime = candidate.GetLastProcessedTime()
					legacyStillValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{}, legacyTask)
					if err != nil {
						return struct{}{}, err
					}
					successorValid, err = handler.Validate(ctx, candidate, chasm.TaskInvocation{},
						&schedulerpb.BackfillerTask{Stamp: taskStamp})
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.Empty(t, actualTimes, "the zero-stamp task must not process the range")
	require.Nil(t, lastProcessedTime, "the zero-stamp task must not advance the range watermark")
	require.Equal(t, attemptBeforeWake, attempt, "the migration wake-up must not increment Attempt")
	require.Equal(t, attempt+1, taskStamp)
	require.False(t, legacyStillValid, "the zero-stamp task must be invalid after the successor is scheduled")
	require.True(t, successorValid, "the stamped successor must be valid")
}
