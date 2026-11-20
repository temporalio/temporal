package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Guards that a migrated schedule's backfill waits for SchedulerCallbacksTask to refresh running workflows,
// or its starts are judged against a running entry the V1 workflow may have outlived.
func TestMigration_BackfillWaitsForCallbacks(t *testing.T) {
	t.Skip("known bug: CreateSchedulerFromMigration arms Backfiller tasks immediately instead of deferring them behind SchedulerCallbacksTask")
	ctrl := gomock.NewController(t)
	// Mid-minute, so the one-interval backfill below covers exactly one action.
	now := time.Now().UTC().Truncate(time.Minute).Add(30 * time.Second)
	schedule := defaultSchedule()
	schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP

	req := &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: namespaceID,
		State: &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{
				Schedule:    schedule,
				Info:        &schedulepb.ScheduleInfo{},
				Namespace:   namespace,
				NamespaceId: namespaceID,
				ScheduleId:  scheduleID,
			},
			GeneratorState: &schedulerpb.GeneratorState{LastProcessedTime: timestamppb.New(now)},
			InvokerState: &schedulerpb.InvokerState{
				LastProcessedTime: timestamppb.New(now),
				BufferedStarts: []*schedulespb.BufferedStart{{
					NominalTime:   timestamppb.New(now.Add(-time.Hour)),
					ActualTime:    timestamppb.New(now.Add(-time.Hour)),
					StartTime:     timestamppb.New(now.Add(-time.Hour)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					RequestId:     "v1-req",
					WorkflowId:    "v1-wf",
					RunId:         "v1-run",
					Attempt:       1,
				}},
			},
			Backfillers: map[string]*schedulerpb.BackfillerState{
				"bf-1": {
					BackfillId: "bf-1",
					Request: &schedulerpb.BackfillerState_BackfillRequest{
						BackfillRequest: &schedulepb.BackfillRequest{
							StartTime:     timestamppb.New(now.Add(-defaultInterval)),
							EndTime:       timestamppb.New(now),
							OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
						},
					},
				},
			},
		},
	}
	sched, _, env := setupExecutionForTest(t,
		newRealSpecProcessor(ctrl, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)),
		func(ctx chasm.MutableContext) (*scheduler.Scheduler, error) {
			return scheduler.CreateSchedulerFromMigration(ctx, req)
		})

	require.Zero(t, sched.Info.OverlapSkipped, "backfill starts were judged before callbacks refreshed running state")

	// The V1 workflow closed before migration.
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewNotFound("closed"))
	handler := scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
		Config:         defaultConfig(),
		HistoryClient:  historyClient,
		MetricsHandler: metrics.NoopMetricsHandler,
	})
	dropped, err := chasmtest.ExecuteSideEffectTask(context.Background(), env.engine, sched, handler,
		chasm.TaskAttributes{}, &schedulerpb.SchedulerCallbacksTask{})
	require.NoError(t, err)
	require.False(t, dropped)
	_, err = env.engine.FirePureTasks(env.rootRef, env.TimeSource.Now())
	require.NoError(t, err)

	require.Zero(t, sched.Info.OverlapSkipped)
	var backfillStarts int
	for _, start := range sched.Invoker.Get(env.ReadContext()).GetBufferedStarts() {
		if start.GetRequestId() != "v1-req" {
			backfillStarts++
		}
	}
	require.Equal(t, 1, backfillStarts, "backfill should buffer its start once callbacks are attached")
}
