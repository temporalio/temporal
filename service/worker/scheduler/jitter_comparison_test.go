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
	for _, backend := range []struct {
		name string
		run  func(*testing.T, *schedulepb.Schedule, time.Time, []time.Time) []legacyscheduler.ScheduleGenerationForTest
	}{
		{"v1", generateV1Actions},
		{"v2", generateV2Actions},
	} {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			// Fixed namespace/schedule IDs in the adapters give the 22:00 slot
			// a deterministic fire time of 22:07:26.245. An extra wakeup at
			// 22:04 must not prevent that action from being generated at its fire time.
			schedule := &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(15 * time.Minute)}},
					Jitter:   durationpb.New(10 * time.Minute),
				},
				Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE},
				State:    &schedulepb.ScheduleState{},
			}
			createdAt := time.Date(2026, 10, 26, 21, 59, 59, 0, time.UTC)
			wakeups := []time.Time{
				time.Date(2026, 10, 26, 22, 2, 0, 0, time.UTC),
				time.Date(2026, 10, 26, 22, 4, 0, 0, time.UTC),
				time.Date(2026, 10, 26, 22, 6, 0, 0, time.UTC),
				time.Date(2026, 10, 26, 22, 7, 26, 245000000, time.UTC),
			}
			type action struct{ NominalTime, FireTime string }
			type generation struct {
				Actions        []action
				NextWakeupTime string
			}
			expected := []generation{
				{NextWakeupTime: "2026-10-26T22:07:26.245Z"}, // 22:02: nothing due.
				{NextWakeupTime: "2026-10-26T22:07:26.245Z"}, // 22:04: same pending slot.
				{NextWakeupTime: "2026-10-26T22:07:26.245Z"}, // 22:06: same pending slot.
				{
					Actions:        []action{{NominalTime: "2026-10-26T22:00:00Z", FireTime: "2026-10-26T22:07:26.245Z"}},
					NextWakeupTime: "2026-10-26T22:20:57.173Z",
				},
			}

			var actual []generation
			for _, result := range backend.run(t, schedule, createdAt, wakeups) {
				output := generation{NextWakeupTime: result.NextWakeupTime.Format(time.RFC3339Nano)}
				for _, start := range result.BufferedStarts {
					output.Actions = append(output.Actions, action{
						NominalTime: start.NominalTime.AsTime().Format(time.RFC3339Nano),
						FireTime:    start.ActualTime.AsTime().Format(time.RFC3339Nano),
					})
				}
				actual = append(actual, output)
			}
			require.Equal(t, expected, actual)
		})
	}
}

func generateV1Actions(t *testing.T, schedule *schedulepb.Schedule, createdAt time.Time, wakeups []time.Time) []legacyscheduler.ScheduleGenerationForTest {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(legacyscheduler.GenerateScheduledActionsForTest, &schedulespb.StartScheduleArgs{
		Schedule: schedule,
		Info:     &schedulepb.ScheduleInfo{},
		State: &schedulespb.InternalState{
			NamespaceId:       "namespace-id",
			ScheduleId:        "schedule-id",
			LastProcessedTime: timestamppb.New(createdAt),
		},
	}, wakeups)
	require.NoError(t, env.GetWorkflowError())
	var actions []legacyscheduler.ScheduleGenerationForTest
	require.NoError(t, env.GetWorkflowResult(&actions))
	return actions
}

func generateV2Actions(t *testing.T, schedule *schedulepb.Schedule, createdAt time.Time, wakeups []time.Time) []legacyscheduler.ScheduleGenerationForTest {
	t.Helper()
	sched := &chasmscheduler.Scheduler{SchedulerState: &schedulerpb.SchedulerState{
		Schedule: schedule, Info: &schedulepb.ScheduleInfo{},
		NamespaceId: "namespace-id", ScheduleId: "schedule-id",
	}}
	processor := chasmscheduler.NewSpecProcessor(&chasmscheduler.Config{
		Tweakables: func(string) chasmscheduler.Tweakables { return chasmscheduler.DefaultTweakables },
	}, metrics.NoopMetricsHandler, log.NewTestLogger(), legacyscheduler.NewSpecBuilder(
		dynamicconfig.GetIntPropertyFn(0), dynamicconfig.GetIntPropertyFn(0),
	))
	cursor := createdAt
	var actions []legacyscheduler.ScheduleGenerationForTest
	for _, now := range wakeups {
		result, err := processor.ProcessTimeRange(
			sched, cursor, now, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, "workflow-id", "", false, nil,
		)
		require.NoError(t, err)
		cursor = result.LastActionTime
		actions = append(actions, legacyscheduler.ScheduleGenerationForTest{
			BufferedStarts: result.BufferedStarts,
			NextWakeupTime: result.NextWakeupTime,
		})
	}
	return actions
}
