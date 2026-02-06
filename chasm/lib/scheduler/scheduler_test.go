package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPatchRejectsExcessBackfillers(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)

	now := time.Now()

	// Add backfillers up to the limit in batches via Patch.
	batchSize := 50
	for i := 0; i < 100; i += batchSize {
		ctx = reopenTransaction(node)
		backfills := make([]*schedulepb.BackfillRequest, batchSize)
		for j := range backfills {
			backfills[j] = &schedulepb.BackfillRequest{
				StartTime: timestamppb.New(now),
				EndTime:   timestamppb.New(now.Add(time.Minute)),
			}
		}
		_, err := sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Patch: &schedulepb.SchedulePatch{
					BackfillRequest: backfills,
				},
			},
		})
		require.NoError(t, err)
		_, err = node.CloseTransaction()
		require.NoError(t, err)
	}

	// The scheduler should now have exactly 100 backfillers.
	require.Equal(t, 100, len(sched.Backfillers))

	// Adding one more should fail with ErrTooManyBackfillers.
	ctx = reopenTransaction(node)
	_, err := sched.Patch(ctx, &schedulerpb.PatchScheduleRequest{
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{
				BackfillRequest: []*schedulepb.BackfillRequest{
					{
						StartTime: timestamppb.New(now),
						EndTime:   timestamppb.New(now.Add(time.Minute)),
					},
				},
			},
		},
	})
	require.ErrorIs(t, err, scheduler.ErrTooManyBackfillers)

	// Backfiller count should be unchanged (no partial creation).
	require.Equal(t, 100, len(sched.Backfillers))
}

func reopenTransaction(node *chasm.Node) chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), node)
}

func TestListInfo(t *testing.T) {
	scheduler, ctx, _ := setupSchedulerForTest(t)

	// Generator maintains the FutureActionTimes list, set that up first.
	generator := scheduler.Generator.Get(ctx)
	expectedFutureTimes := []*timestamppb.Timestamp{timestamppb.Now(), timestamppb.Now()}
	generator.FutureActionTimes = expectedFutureTimes

	listInfo := scheduler.ListInfo(ctx)

	// Should return a populated info block.
	require.NotNil(t, listInfo)
	require.NotNil(t, listInfo.Spec)
	require.NotEmpty(t, listInfo.Spec.Interval)
	protorequire.ProtoEqual(t, listInfo.Spec.Interval[0], scheduler.Schedule.Spec.Interval[0])
	require.NotNil(t, listInfo.WorkflowType)
	require.NotEmpty(t, listInfo.FutureActionTimes)
	require.Equal(t, expectedFutureTimes, listInfo.FutureActionTimes)
}
