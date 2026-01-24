package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListInfo(t *testing.T) {
	scheduler, ctx, _ := setupSchedulerForTest(t)

	// Generator maintains the FutureActionTimes list, set that up first.
	generator := scheduler.Generator.Get(ctx)
	expectedFutureTimes := []*timestamppb.Timestamp{timestamppb.Now(), timestamppb.Now()}
	generator.FutureActionTimes = expectedFutureTimes

	listInfo := scheduler.ListInfo(ctx)

	// Should return a populated info block.
	require.NotNil(t, listInfo)
	require.NotNil(t, listInfo.GetSpec())
	require.NotEmpty(t, listInfo.GetSpec().GetInterval())
	protorequire.ProtoEqual(t, listInfo.GetSpec().GetInterval()[0], scheduler.Schedule.GetSpec().GetInterval()[0])
	require.NotNil(t, listInfo.GetWorkflowType())
	require.NotEmpty(t, listInfo.GetFutureActionTimes())
	require.Equal(t, expectedFutureTimes, listInfo.GetFutureActionTimes())
}
