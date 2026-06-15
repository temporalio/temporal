package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/payload"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

// TestScheduler_Describe_ReturnsIsolatedVisibilityMaps proves that DescribeSchedule
// returns isolated copies of the Visibility component's memo and search-attribute maps.
//
// CustomMemo/CustomSearchAttributes return the Visibility component's live maps by
// reference. DescribeSchedule's response is marshalled by gRPC after the read lease is
// released, so if those maps are aliased into the response, a concurrent operation that
// mutates them races the marshal and trips "concurrent map iteration and map write".
//
// Rather than race the panic (which is nondeterministic), we test the positive invariant:
// the response carries independent copies, so mutating the response leaves the live
// component maps untouched. Before the fix this assertion fails because the maps are shared.
func TestScheduler_Describe_ReturnsIsolatedVisibilityMaps(t *testing.T) {
	sched, ctx, _ := setupSchedulerForTest(t)
	specBuilder := legacyscheduler.NewSpecBuilder()

	vis := sched.Visibility.Get(ctx)
	vis.MergeCustomMemo(ctx, map[string]*commonpb.Payload{"memoKey": payload.EncodeString("v")})
	vis.MergeCustomSearchAttributes(ctx, map[string]*commonpb.Payload{"saKey": payload.EncodeString("v")})

	resp, err := sched.Describe(ctx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.DescribeScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
		},
	}, specBuilder)
	require.NoError(t, err)

	fr := resp.GetFrontendResponse()
	require.Contains(t, fr.GetMemo().GetFields(), "memoKey")
	require.Contains(t, fr.GetSearchAttributes().GetIndexedFields(), "saKey")

	// Mutating the response must not reach back into the live component maps.
	fr.GetMemo().GetFields()["injectedMemo"] = payload.EncodeString("x")
	fr.GetSearchAttributes().GetIndexedFields()["injectedSA"] = payload.EncodeString("x")

	require.NotContains(t, vis.CustomMemo(ctx), "injectedMemo",
		"DescribeSchedule response Memo must be a copy, not the live Visibility map")
	require.NotContains(t, vis.CustomSearchAttributes(ctx), "injectedSA",
		"DescribeSchedule response SearchAttributes must be a copy, not the live Visibility map")
}
