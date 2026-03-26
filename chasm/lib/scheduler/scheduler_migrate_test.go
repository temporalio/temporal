package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

func TestMigrateToWorkflow_PausesSchedule(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	require.False(t, sched.Schedule.State.Paused)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.True(t, sched.Schedule.State.Paused)
	require.Equal(t, "paused for migration to workflow-backed scheduler", sched.Schedule.State.Notes)
	require.NotNil(t, sched.WorkflowMigration)
}

func TestMigrateToWorkflow_SavesPreMigrationState(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	// Pause the schedule before migration with custom notes.
	sched.Schedule.State.Paused = true
	sched.Schedule.State.Notes = "user paused"

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.NotNil(t, sched.WorkflowMigration)
	require.True(t, sched.WorkflowMigration.PreMigrationPaused)
	require.Equal(t, "user paused", sched.WorkflowMigration.PreMigrationNotes)
}

func TestMigrateToWorkflow_SavesPreMigrationState_Unpaused(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	require.False(t, sched.Schedule.State.Paused)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.NotNil(t, sched.WorkflowMigration)
	require.False(t, sched.WorkflowMigration.PreMigrationPaused)
	require.Empty(t, sched.WorkflowMigration.PreMigrationNotes)
}

func TestMigrateToWorkflow_Idempotent(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	// Second call succeeds without error (no-op).
	_, err = sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)
}

func TestMigrateToWorkflow_Sentinel(t *testing.T) {
	sentinel, ctx, _ := setupSentinelForTest(t)

	_, err := sentinel.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})

	var notFoundErr *serviceerror.NotFound
	require.ErrorAs(t, err, &notFoundErr)
}

func TestPatch_UnpauseBlockedDuringMigration(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	// Initiate migration.
	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	// Attempt to unpause should fail.
	_, err = sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				Unpause: "resuming",
			},
		},
	})

	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
	require.ErrorIs(t, err, scheduler.ErrMigrationPending)
}

func TestPatch_PauseAllowedDuringMigration(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	// Pause with different notes should succeed.
	_, err = sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				Pause: "user pause during migration",
			},
		},
	})
	require.NoError(t, err)
	require.True(t, sched.Schedule.State.Paused)
}

func TestDescribe_ClosedReturnsErrClosed(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	sched.Closed = true

	_, err := sched.Describe(ctx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId:     namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: namespace, ScheduleId: scheduleID},
	}, nil)

	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
	require.ErrorIs(t, err, scheduler.ErrClosed)
}

func TestListMatchingTimes_ClosedReturnsErrClosed(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	sched.Closed = true

	_, err := sched.ListMatchingTimes(ctx, &schedulerpb.ListScheduleMatchingTimesRequest{
		NamespaceId:     namespaceID,
		FrontendRequest: &workflowservice.ListScheduleMatchingTimesRequest{Namespace: namespace, ScheduleId: scheduleID},
	}, nil)

	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
	require.ErrorIs(t, err, scheduler.ErrClosed)
}

func TestUpdate_ClosedReturnsErrClosed(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	sched.Closed = true

	_, err := sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   defaultSchedule(),
		},
	})

	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
	require.ErrorIs(t, err, scheduler.ErrClosed)
}

func TestPatch_ClosedReturnsErrClosed(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	sched.Closed = true

	_, err := sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch:      &schedulepb.SchedulePatch{Pause: "test"},
		},
	})

	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
	require.ErrorIs(t, err, scheduler.ErrClosed)
}

func TestMigrateToWorkflow_ClosedReturnsErrClosed(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	sched.Closed = true

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})

	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
	require.ErrorIs(t, err, scheduler.ErrClosed)
}

func TestUpdate_RejectedDuringMigration(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	// Update should be rejected outright when a migration is pending.
	newSchedule := defaultSchedule()
	newSchedule.State.Paused = false
	newSchedule.State.Notes = "user unpaused"

	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   newSchedule,
		},
	})
	var unavailableErr *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailableErr)
	require.ErrorIs(t, err, scheduler.ErrMigrationPending)
}
