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
		run  func(*testing.T, *schedulepb.Schedule, string, string, time.Time, []time.Time) [][]*schedulespb.BufferedStart
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
				time.Date(2026, 10, 26, 22, 4, 0, 0, time.UTC),
				time.Date(2026, 10, 26, 22, 7, 26, 245000000, time.UTC),
			}
			type action struct{ NominalTime, FireTime string }
			expected := [][]action{
				nil, // 22:04: nothing is due yet.
				{{NominalTime: "2026-10-26T22:00:00Z", FireTime: "2026-10-26T22:07:26.245Z"}},
			}

			var actual [][]action
			for _, batch := range backend.run(t, schedule, "namespace-id", "schedule-id", createdAt, wakeups) {
				var actions []action
				for _, start := range batch {
					actions = append(actions, action{
						NominalTime: start.NominalTime.AsTime().Format(time.RFC3339Nano),
						FireTime:    start.ActualTime.AsTime().Format(time.RFC3339Nano),
					})
				}
				actual = append(actual, actions)
			}
			require.Equal(t, expected, actual)
		})
	}
}

// TestJitterTimelineFromV1PR6028 reuses the fixed namespace, schedule, and
// timestamps from TestSignalBetweenNominalAndJittered. The extra wakeup at
// 03:22:10 represents its refresh signal. This compares action generation;
// V1's workflow test separately exercises signal delivery and continue-as-new.
func TestJitterTimelineFromV1PR6028(t *testing.T) {
	t.Parallel()
	for _, backend := range []struct {
		name string
		run  func(*testing.T, *schedulepb.Schedule, string, string, time.Time, []time.Time) [][]*schedulespb.BufferedStart
	}{
		{"v1", generateV1Actions},
		{"v2", generateV2Actions},
	} {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			schedule := &schedulepb.Schedule{
				Spec: &schedulepb.ScheduleSpec{
					Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
					Jitter:   durationpb.New(time.Hour),
				},
				Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE},
				State:    &schedulepb.ScheduleState{},
			}
			var wakeups []time.Time
			for _, timestamp := range []string{
				"2022-06-01T01:49:22.594Z",
				"2022-06-01T01:53:00Z", // First workflow completes.
				"2022-06-01T02:02:39.204Z",
				"2022-06-01T02:11:00Z", // Second workflow completes.
				"2022-06-01T03:22:10Z", // Refresh during the third slot's jitter gap.
				"2022-06-01T03:37:29.538Z",
				"2022-06-01T03:41:00Z", // Third workflow completes.
				"2022-06-01T04:23:34.755Z",
				"2022-06-01T04:27:00Z", // Fourth workflow completes.
				"2022-06-01T05:00:00Z",
			} {
				now, err := time.Parse(time.RFC3339Nano, timestamp)
				require.NoError(t, err)
				wakeups = append(wakeups, now)
			}
			expected := [][]string{
				{"2022-06-01T01:00:00Z -> 2022-06-01T01:49:22.594Z"},
				nil,
				{"2022-06-01T02:00:00Z -> 2022-06-01T02:02:39.204Z"},
				nil,
				nil,
				{"2022-06-01T03:00:00Z -> 2022-06-01T03:37:29.538Z"},
				nil,
				{"2022-06-01T04:00:00Z -> 2022-06-01T04:23:34.755Z"},
				nil,
				nil,
			}
			var actual [][]string
			for _, batch := range backend.run(t, schedule, "mynsid", "myschedule",
				time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC), wakeups) {
				var actions []string
				for _, start := range batch {
					actions = append(actions, start.NominalTime.AsTime().Format(time.RFC3339Nano)+
						" -> "+start.ActualTime.AsTime().Format(time.RFC3339Nano))
				}
				actual = append(actual, actions)
			}
			require.Equal(t, expected, actual)
		})
	}
}

func generateV1Actions(t *testing.T, schedule *schedulepb.Schedule, namespaceID, scheduleID string, createdAt time.Time, wakeups []time.Time) [][]*schedulespb.BufferedStart {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(legacyscheduler.GenerateScheduledActionsForTest, &schedulespb.StartScheduleArgs{
		Schedule: schedule,
		Info:     &schedulepb.ScheduleInfo{},
		State: &schedulespb.InternalState{
			NamespaceId:       namespaceID,
			ScheduleId:        scheduleID,
			LastProcessedTime: timestamppb.New(createdAt),
		},
	}, wakeups)
	require.NoError(t, env.GetWorkflowError())
	var actions [][]*schedulespb.BufferedStart
	require.NoError(t, env.GetWorkflowResult(&actions))
	return actions
}

func generateV2Actions(t *testing.T, schedule *schedulepb.Schedule, namespaceID, scheduleID string, createdAt time.Time, wakeups []time.Time) [][]*schedulespb.BufferedStart {
	t.Helper()
	sched := &chasmscheduler.Scheduler{SchedulerState: &schedulerpb.SchedulerState{
		Schedule: schedule, Info: &schedulepb.ScheduleInfo{},
		NamespaceId: namespaceID, ScheduleId: scheduleID,
	}}
	processor := chasmscheduler.NewSpecProcessor(&chasmscheduler.Config{
		Tweakables: func(string) chasmscheduler.Tweakables { return chasmscheduler.DefaultTweakables },
	}, metrics.NoopMetricsHandler, log.NewTestLogger(), legacyscheduler.NewSpecBuilder(
		dynamicconfig.GetIntPropertyFn(0), dynamicconfig.GetIntPropertyFn(0),
	))
	cursor := createdAt
	var actions [][]*schedulespb.BufferedStart
	for _, now := range wakeups {
		result, err := processor.ProcessTimeRange(
			sched, cursor, now, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, "workflow-id", "", false, nil,
		)
		require.NoError(t, err)
		cursor = result.LastActionTime
		actions = append(actions, result.BufferedStarts)
	}
	return actions
}
