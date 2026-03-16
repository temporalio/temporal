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

	require.False(t, sched.SchedulerState.Schedule.State.Paused)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.True(t, sched.SchedulerState.Schedule.State.Paused)
	require.Equal(t, "paused for migration to workflow-backed scheduler", sched.SchedulerState.Schedule.State.Notes)
	require.True(t, sched.SchedulerState.MigrationToWorkflowPending)
}

func TestMigrateToWorkflow_SavesPreMigrationState(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	// Pause the schedule before migration with custom notes.
	sched.SchedulerState.Schedule.State.Paused = true
	sched.SchedulerState.Schedule.State.Notes = "user paused"

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.True(t, sched.SchedulerState.PreMigrationPaused)
	require.Equal(t, "user paused", sched.SchedulerState.PreMigrationNotes)
}

func TestMigrateToWorkflow_SavesPreMigrationState_Unpaused(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	require.False(t, sched.SchedulerState.Schedule.State.Paused)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.False(t, sched.SchedulerState.PreMigrationPaused)
	require.Empty(t, sched.SchedulerState.PreMigrationNotes)
}

func TestMigrateToWorkflow_Idempotent_DoesNotOverwritePreMigrationState(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	// Schedule is not paused before first migration.
	require.False(t, sched.SchedulerState.Schedule.State.Paused)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.False(t, sched.SchedulerState.PreMigrationPaused)

	// Second call should not overwrite the pre-migration state (which is now
	// "paused for migration" -- but the saved state should still be unpaused).
	_, err = sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	require.False(t, sched.SchedulerState.PreMigrationPaused)
	require.Empty(t, sched.SchedulerState.PreMigrationNotes)
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

	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
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
	require.True(t, sched.SchedulerState.Schedule.State.Paused)
}

func TestUpdate_ForcesPauseDuringMigration(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)

	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{
		NamespaceId: namespaceID,
		ScheduleId:  scheduleID,
	})
	require.NoError(t, err)

	// Update with an unpaused schedule should still result in paused.
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
	require.NoError(t, err)

	require.True(t, sched.SchedulerState.Schedule.State.Paused)
	require.Equal(t, "paused for migration to workflow-backed scheduler", sched.SchedulerState.Schedule.State.Notes)
}
