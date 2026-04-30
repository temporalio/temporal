package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// runSentinelHandlerTestCase asserts that the given operation returns
// NotFound when invoked on a sentinel scheduler.
func runSentinelHandlerTestCase(
	t *testing.T,
	callFn func(sentinel *scheduler.Scheduler, ctx chasm.MutableContext, specBuilder *legacyscheduler.SpecBuilder) error,
) {
	sentinel, ctx, _ := setupSentinelForTest(t)
	specBuilder := legacyscheduler.NewSpecBuilder()

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

func TestHandler_CreateFromMigrationState_Sentinel(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, newRealSpecProcessor(ctrl, logger))))

	h := scheduler.NewTestHandler(logger)
	testEngine := chasmtest.NewEngine(t, registry)
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)
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
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, newRealSpecProcessor(ctrl, logger))))

	h := scheduler.NewTestHandler(logger)
	testEngine := chasmtest.NewEngine(t, registry)
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)
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
