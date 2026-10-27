package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/testsuite"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestJitterGapPreservesPendingActionV1AndV2(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			schedule := &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(15 * time.Minute)}},
					Jitter:   durationpb.New(10 * time.Minute),
				},
				Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE},
				State:    &schedulepb.ScheduleState{},
			}
			builder := legacyscheduler.NewSpecBuilder(dynamicconfig.GetIntPropertyFn(0), dynamicconfig.GetIntPropertyFn(0))
			spec, err := builder.NewCompiledSpec(schedule.Spec)
			require.NoError(t, err)
			start := time.Date(2026, 10, 26, 21, 59, 59, 0, time.UTC)
			next, err := spec.GetNextTime("namespace-id-schedule-id", start)
			require.NoError(t, err)
			require.True(t, next.Next.After(next.Nominal), "the slot must have a nonzero jitter delay")

			// A completion can wake the generator inside this gap. The second
			// processing window ends at the regular jittered timer deadline.
			// Neither backend migrates or updates its specification.
			ends := []time.Time{next.Nominal.Add(next.Next.Sub(next.Nominal) / 2), next.Next}
			var results []*schedulespb.InternalState
			switch backend {
			case "v1":
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestWorkflowEnvironment()
				env.ExecuteWorkflow(legacyscheduler.ProcessTimeRangesForTest, &schedulespb.StartScheduleArgs{
					Schedule: schedule,
					Info:     &schedulepb.ScheduleInfo{},
					State: &schedulespb.InternalState{
						NamespaceId:       "namespace-id",
						ScheduleId:        "schedule-id",
						LastProcessedTime: timestamppb.New(start),
					},
				}, ends)
				require.NoError(t, env.GetWorkflowError())
				require.NoError(t, env.GetWorkflowResult(&results))
			case "v2":
				sched := &chasmscheduler.Scheduler{SchedulerState: &schedulerpb.SchedulerState{
					Schedule:    schedule,
					Info:        &schedulepb.ScheduleInfo{},
					NamespaceId: "namespace-id",
					ScheduleId:  "schedule-id",
				}}
				processor := chasmscheduler.NewSpecProcessor(&chasmscheduler.Config{
					Tweakables: func(string) chasmscheduler.Tweakables { return chasmscheduler.DefaultTweakables },
				}, metrics.NoopMetricsHandler, log.NewTestLogger(), builder)
				watermark := start
				var buffered []*schedulespb.BufferedStart
				for _, end := range ends {
					result, err := processor.ProcessTimeRange(
						sched, watermark, end, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, "workflow-id", "", false, nil,
					)
					require.NoError(t, err)
					watermark = result.LastActionTime
					buffered = append(buffered, result.BufferedStarts...)
					results = append(results, &schedulespb.InternalState{
						LastProcessedTime: timestamppb.New(watermark),
						BufferedStarts:    buffered,
					})
				}
			default:
				t.Fatalf("unknown scheduler backend %q", backend)
			}

			// Compare the watermark and scheduled times. V2's request/workflow IDs
			// are unrelated to whether the pending slot survives the jitter gap.
			type actionTime struct{ nominal, actual time.Time }
			type result struct {
				watermark time.Time
				actions   []actionTime
			}
			var actual []result
			for _, state := range results {
				r := result{watermark: state.LastProcessedTime.AsTime()}
				for _, action := range state.BufferedStarts {
					r.actions = append(r.actions, actionTime{action.NominalTime.AsTime(), action.ActualTime.AsTime()})
				}
				actual = append(actual, r)
			}
			require.Equal(t, []result{
				{watermark: start},
				{watermark: next.Next, actions: []actionTime{{next.Nominal, next.Next}}},
			}, actual)
		})
	}
}
