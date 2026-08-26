package scheduler_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

// TestScheduler_IdleTask_DoesNotAccumulateAcrossCompletionBurst reproduces the
// idle-task leak seen with a historical, ALLOW_ALL backfill: many buffered
// starts are kicked off at nearly the same wall-clock instant, then complete
// over an extended window. Each completion independently invalidates and
// re-arms the idle task (see the comment on HandleNexusCompletion), but since
// idleDeadline is derived from getLastEventTime - which reads BufferedStart's
// real start time, not its completion time - every one of those completions
// recomputes the *same* deadline. SchedulerIdleTaskHandler.Validate only
// detects drift (a deadline that moved), so identical recomputed deadlines
// pass validation every time and the duplicates are never cleaned up. Only
// singleton task registration (deduplicating at insertion time, independent
// of what Validate would decide) prevents the leak.
func TestScheduler_IdleTask_DoesNotAccumulateAcrossCompletionBurst(t *testing.T) {
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

	// A schedule with no remaining actions goes idle as soon as it's created -
	// there's no spec-driven wakeup to keep it open.
	schedule := defaultSchedule()
	schedule.State.LimitedActions = true
	schedule.State.RemainingActions = 0

	handler := scheduler.NewTestHandler(logger)
	_, err := handler.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   schedule,
			RequestId:  "req-create",
		},
	})
	require.NoError(t, err)

	// Seed several running buffered starts, all with StartTime pinned to the
	// same instant - the realistic shape of a historical ALLOW_ALL backfill,
	// where every occurrence is kicked off back-to-back near "now" regardless
	// of how spread out their eventual completions are.
	const completions = 5
	for i := range completions {
		_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				invoker := s.Invoker.Get(ctx)
				invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
					RequestId:  fmt.Sprintf("req-%d", i),
					WorkflowId: fmt.Sprintf("wf-%d", i),
					RunId:      fmt.Sprintf("run-%d", i),
					Attempt:    1,
					ActualTime: timestamppb.New(now),
					StartTime:  timestamppb.New(now),
				})
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
	}

	// Complete each buffered start through the real Nexus-completion path -
	// exactly what HandleNexusCompletion processes in production - spreading
	// the completions out in time to mirror workflows that ran for different
	// durations. Each call is its own transaction, matching separate
	// completion callbacks arriving independently.
	var idleCloseTime time.Time
	for i := range completions {
		_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
				return struct{}{}, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
					RequestId: fmt.Sprintf("req-%d", i),
					Outcome: &persistencespb.ChasmNexusCompletion_Success{
						Success: &commonpb.Payload{},
					},
					CloseTime: timestamppb.New(now.Add(time.Duration(i) * time.Hour)),
				})
			}, struct{}{})
		require.NoError(t, err)

		_, err = chasm.ReadComponent(engineCtx, rootRef,
			func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
				require.NotNil(t, s.IdleCloseTime, "expected an idle task to be armed after every completion")
				idleCloseTime = s.IdleCloseTime.AsTime()
				return struct{}{}, nil
			}, struct{}{})
		require.NoError(t, err)
	}

	// Every completion independently recomputed the same idle deadline (their
	// CloseTimes differ, but StartTime - what getLastEventTime actually reads
	// - doesn't), so without deduplication each of the `completions` calls
	// above appends its own SchedulerIdleTask. There should only ever be one.
	count, err := engine.PureTaskCount(rootRef, idleCloseTime, reflect.TypeFor[*schedulerpb.SchedulerIdleTask]())
	require.NoError(t, err)
	require.Equal(t, 1, count,
		"expected exactly one idle pure task after a completion burst, found %d", count)
}
