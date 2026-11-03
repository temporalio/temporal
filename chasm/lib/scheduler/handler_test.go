package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// runSentinelHandlerTestCase asserts that the given operation returns
// NotFound when invoked on a sentinel scheduler.
func runSentinelHandlerTestCase(
	t *testing.T,
	callFn func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, specBuilder *legacyscheduler.SpecBuilder) error,
) {
	sentinel, ctx, _ := setupSentinelForTest(t)
	specBuilder := newLegacySpecBuilder(0, 0)

	err := callFn(sentinel, ctx, specBuilder)

	require.Error(t, err)
	var notFoundErr *serviceerror.NotFound
	require.ErrorAs(t, err, &notFoundErr, "expected NotFound error for sentinel")
}

func TestSentinelHandler_DescribeSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, specBuilder *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Describe(ctx, &schedulerpb.DescribeScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		}, specBuilder)
		return err
	})
}

func TestSentinelHandler_ListScheduleMatchingTimes(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, specBuilder *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.ListMatchingTimes(ctx, &schedulerpb.ListScheduleMatchingTimesRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				StartTime:  timestamppb.Now(),
				EndTime:    timestamppb.Now(),
			},
		}, specBuilder)
		return err
	})
}

func TestSentinelHandler_UpdateSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Update(ctx, &schedulerpb.UpdateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		})
		return err
	})
}

func TestSentinelHandler_PatchSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Patch(ctx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		})
		return err
	})
}

func TestSentinelHandler_DeleteSchedule(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.Delete(ctx, &schedulerpb.DeleteScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.DeleteScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		})
		return err
	})
}

func TestSentinelHandler_MigrateToWorkflow(t *testing.T) {
	runSentinelHandlerTestCase(t, func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, _ *legacyscheduler.SpecBuilder) error {
		_, err := sentinel.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
			NamespaceId: namespaceID,
			ScheduleId:  scheduleID,
		})
		return err
	})
}

func TestHandler_CreateSchedule_Sentinel(t *testing.T) {
	logger := log.NewTestLogger()
	h := scheduler.NewTestHandler(logger)
	_, engineCtx := newTestEngineContext(t, logger)
	_, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  scheduleID,
		},
		func(ctx chasm.MutableContext, _ struct{}) (*scheduler.Scheduler, error) {
			return scheduler.NewSentinel(ctx, namespace, namespaceID, scheduleID), nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	_, err = h.CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			ScheduleId: scheduleID,
		},
	})

	require.ErrorIs(t, err, scheduler.ErrSentinel)
}

func TestHandler_CreateFromMigrationState_Sentinel(t *testing.T) {
	logger := log.NewTestLogger()
	h := scheduler.NewTestHandler(logger)
	_, engineCtx := newTestEngineContext(t, logger)
	_, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  scheduleID,
		},
		func(ctx chasm.MutableContext, _ struct{}) (*scheduler.Scheduler, error) {
			return scheduler.NewSentinel(ctx, namespace, namespaceID, scheduleID), nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	_, err = h.TestCreateFromMigrationState(engineCtx, &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: namespaceID,
		State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{
				ScheduleId: scheduleID,
			},
		},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, scheduler.ErrSentinelBlocked)
	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
}

func TestHandler_MigrateToWorkflow_Sentinel(t *testing.T) {
	logger := log.NewTestLogger()
	h := scheduler.NewTestHandler(logger)
	_, engineCtx := newTestEngineContext(t, logger)
	_, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  scheduleID,
		},
		func(ctx chasm.MutableContext, _ struct{}) (*scheduler.Scheduler, error) {
			return scheduler.NewSentinel(ctx, namespace, namespaceID, scheduleID), nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	_, err = h.TestMigrateToWorkflow(engineCtx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, scheduler.ErrSentinelBlocked)
	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
}

// A retried PatchSchedule (same RequestId) must not enqueue another manual action.
func TestHandler_PatchSchedule_RequestIDDedup(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 26, 22, 4, 0, 0, time.UTC)
	trigger := &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		ScheduledTime: timestamppb.New(now),
	}}
	backfill := &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
		StartTime:     timestamppb.New(now.Add(-time.Minute)),
		EndTime:       timestamppb.New(now.Add(-time.Minute)),
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}}}

	for _, tc := range []struct {
		name       string
		patch      *schedulepb.SchedulePatch
		requestIDs []string
		wantManual int
	}{
		{name: "trigger retried", patch: trigger, requestIDs: []string{"req-1", "req-1"}, wantManual: 1},
		{name: "backfill retried", patch: backfill, requestIDs: []string{"req-1", "req-1"}, wantManual: 1},
		{name: "trigger distinct request IDs", patch: trigger, requestIDs: []string{"req-1", "req-2"}, wantManual: 2},
		{name: "backfill distinct request IDs", patch: backfill, requestIDs: []string{"req-1", "req-2"}, wantManual: 2},
		{name: "trigger empty request ID", patch: trigger, requestIDs: []string{"", ""}, wantManual: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := clock.NewEventTimeSource()
			ts.Update(now)
			env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
			h := scheduler.NewTestHandler(env.logger)

			for _, requestID := range tc.requestIDs {
				resp, err := h.PatchSchedule(env.engineCtx, &schedulerpb.PatchScheduleRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.PatchScheduleRequest{
						Namespace:  namespace,
						ScheduleId: scheduleID,
						RequestId:  requestID,
						Patch:      common.CloneProto(tc.patch),
					},
				})
				require.NoError(t, err)
				require.NotNil(t, resp.GetFrontendResponse())
			}

			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
				var manual int
				for _, start := range s.Invoker.Get(ctx).BufferedStarts {
					if start.Manual {
						manual++
					}
				}
				require.Equal(t, tc.wantManual, manual)
				return nil
			}))
		})
	}
}

// A retried UpdateSchedule (same RequestId) must succeed without reapplying, matching V1's
// signal dedup: no conflict-token mismatch, and no clobbering of a later update.
func TestHandler_UpdateSchedule_RequestIDDedup(t *testing.T) {
	t.Parallel()
	type call struct {
		requestID     string
		notes         string
		initialToken  bool
		wantTokenFail bool
	}
	for _, tc := range []struct {
		name      string
		calls     []call
		wantNotes string
	}{
		{
			name: "retry with stale conflict token succeeds",
			calls: []call{
				{requestID: "req-1", notes: "a", initialToken: true},
				{requestID: "req-1", notes: "a", initialToken: true},
			},
			wantNotes: "a",
		},
		{
			name: "late retry does not clobber a newer update",
			calls: []call{
				{requestID: "req-1", notes: "a"},
				{requestID: "req-2", notes: "b"},
				{requestID: "req-1", notes: "a"},
			},
			wantNotes: "b",
		},
		{
			name: "empty request ID is not deduplicated",
			calls: []call{
				{notes: "a", initialToken: true},
				{notes: "b", initialToken: true, wantTokenFail: true},
				{notes: "c"},
			},
			wantNotes: "c",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newSchedulerTestEngine(t, defaultSchedule())
			h := scheduler.NewTestHandler(env.logger)

			desc, err := h.DescribeSchedule(env.engineCtx, &schedulerpb.DescribeScheduleRequest{
				NamespaceId:     namespaceID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: namespace, ScheduleId: scheduleID},
			})
			require.NoError(t, err)
			initialToken := desc.GetFrontendResponse().GetConflictToken()
			require.NotEmpty(t, initialToken)

			for _, c := range tc.calls {
				sched := defaultSchedule()
				sched.State.Notes = c.notes
				req := &workflowservice.UpdateScheduleRequest{
					Namespace:  namespace,
					ScheduleId: scheduleID,
					RequestId:  c.requestID,
					Schedule:   sched,
				}
				if c.initialToken {
					req.ConflictToken = initialToken
				}
				_, err := h.UpdateSchedule(env.engineCtx, &schedulerpb.UpdateScheduleRequest{
					NamespaceId:     namespaceID,
					FrontendRequest: req,
				})
				if c.wantTokenFail {
					require.ErrorIs(t, err, scheduler.ErrConflictTokenMismatch)
				} else {
					require.NoError(t, err)
				}
			}

			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
				require.Equal(t, tc.wantNotes, s.Schedule.GetState().GetNotes())
				return nil
			}))
		})
	}
}
