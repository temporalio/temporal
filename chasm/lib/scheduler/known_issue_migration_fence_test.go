package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/payload"
)

func TestKnownIssue_UpdateDuringMigrationIsAtomic(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	visibility := sched.Visibility.Get(ctx)
	visibility.ReplaceCustomSearchAttributes(ctx, map[string]*commonpb.Payload{
		"KeywordField": payload.EncodeString("before"),
	})
	before := visibility.CustomSearchAttributes(ctx)
	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
	require.NoError(t, err)

	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Schedule: defaultSchedule(),
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"KeywordField": payload.EncodeString("after"),
			}},
		},
	})
	require.ErrorIs(t, err, scheduler.ErrMigrationPending)
	require.Equal(t, before, sched.Visibility.Get(ctx).CustomSearchAttributes(ctx),
		"a rejected migration-pending update must not mutate visibility state")
}

func TestKnownIssue_TriggerRejectedDuringMigration(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	_, err := sched.MigrateToWorkflow(ctx, &schedulerpb.MigrateToWorkflowRequest{})
	require.NoError(t, err)

	_, err = sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
			},
		},
	})
	require.ErrorIs(t, err, scheduler.ErrMigrationPending)
	require.Empty(t, sched.Backfillers, "rejected trigger must not create migration-unfenced work")
}
