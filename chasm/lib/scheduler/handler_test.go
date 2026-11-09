package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
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

func TestHandler_PatchSchedule_DedupsRequestID(t *testing.T) {
	e := newSchedulerTestEngine(t, defaultSchedule())
	h := scheduler.NewTestHandler(e.logger)
	patch := func(requestID string) {
		_, err := h.PatchSchedule(e.engineCtx, &schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Patch: &schedulepb.SchedulePatch{
					TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
				},
				RequestId: requestID,
			},
		})
		require.NoError(t, err)
	}
	// The test engine runs tasks inline, so each applied trigger has already become one buffered start.
	requireBufferedStarts := func(expected int) {
		require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
			require.Len(t, s.Invoker.Get(ctx).BufferedStarts, expected)
			return nil
		}))
	}

	patch("patch-1")
	requireBufferedStarts(1)

	// A retry with the same request ID succeeds without triggering again.
	patch("patch-1")
	requireBufferedStarts(1)

	patch("patch-2")
	requireBufferedStarts(2)

	// Requests without a request ID are never deduped.
	patch("")
	patch("")
	requireBufferedStarts(4)
}

func TestHandler_UpdateSchedule_DedupsRequestID(t *testing.T) {
	e := newSchedulerTestEngine(t, defaultSchedule())
	h := scheduler.NewTestHandler(e.logger)
	describeResp, err := h.DescribeSchedule(e.engineCtx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		},
	})
	require.NoError(t, err)
	conflictToken := describeResp.GetFrontendResponse().GetConflictToken()
	update := func(requestID string) error {
		_, err := h.UpdateSchedule(e.engineCtx, &schedulerpb.UpdateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:     namespace,
				ScheduleId:    scheduleID,
				Schedule:      defaultSchedule(),
				ConflictToken: conflictToken,
				RequestId:     requestID,
			},
		})
		return err
	}

	require.NoError(t, update("update-1"))

	// The first update changed the conflict token, so only dedup lets the retry succeed.
	require.NoError(t, update("update-1"))

	require.ErrorIs(t, update("update-2"), scheduler.ErrConflictTokenMismatch)
}
