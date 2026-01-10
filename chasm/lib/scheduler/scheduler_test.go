package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDescribe(t *testing.T) {
	scheduler, ctx, _ := setupSchedulerForTest(t)

	// Generator maintains the FutureActionTimes list, set that up first.
	generator := scheduler.Generator.Get(ctx)
	expectedFutureTimes := []*timestamppb.Timestamp{timestamppb.Now(), timestamppb.Now()}
	generator.FutureActionTimes = expectedFutureTimes

	// Add a buffered start to verify BufferSize is returned.
	invoker := scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
		WorkflowId: "test-workflow-id",
	})

	resp, err := scheduler.Describe(ctx, &schedulerpb.DescribeScheduleRequest{})
	require.NoError(t, err)

	// Should return a populated response with FutureActionTimes from the Generator.
	require.NotNil(t, resp)
	require.NotNil(t, resp.FrontendResponse)
	require.NotNil(t, resp.FrontendResponse.Info)
	require.NotEmpty(t, resp.FrontendResponse.Info.FutureActionTimes)
	require.Equal(t, expectedFutureTimes, resp.FrontendResponse.Info.FutureActionTimes)

	// Should return BufferSize from the Invoker.
	require.Equal(t, int64(1), resp.FrontendResponse.Info.BufferSize)
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
