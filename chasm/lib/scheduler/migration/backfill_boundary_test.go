package migration_test

// This file lives in the external test package (migration_test) because it
// imports the V1 scheduler package to compile a real schedule spec, and
// service/worker/scheduler imports the migration package.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newHourlyTestSchedule returns a schedule whose spec matches exactly on the
// hour, so that a backfill starting at 12:00:00Z has a matching action at its
// own start boundary.
func newHourlyTestSchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "test-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "test-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{CatchupWindow: durationpb.New(time.Hour)},
		State:    &schedulepb.ScheduleState{},
	}
}

// nextTimeAfter compiles the schedule's spec with the real V1 spec builder and
// returns the first matching time strictly after the given cursor. This is the
// same search V1's processBackfills performs over OngoingBackfills.
func nextTimeAfter(t *testing.T, schedule *schedulepb.Schedule, after time.Time) time.Time {
	t.Helper()
	specBuilder := legacyscheduler.NewSpecBuilder(func() int { return 0 }, func() int { return 0 })
	cspec, err := specBuilder.NewCompiledSpec(schedule.Spec)
	require.NoError(t, err)
	res, err := cspec.GetNextTime("", after)
	require.NoError(t, err)
	return res.Nominal
}

// TestCHASMToLegacyBackfillStartBoundary verifies that a V2-to-V1 migration
// preserves the inclusivity of a backfill's start boundary. V1 stores an
// already-shifted cursor in OngoingBackfills (patch intake subtracts 1ms) and
// then searches strictly after it, so an unstarted backfiller must be exported
// pre-shifted or the action landing exactly on the backfill start is skipped.
func TestCHASMToLegacyBackfillStartBoundary(t *testing.T) {
	backfillStart := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	backfillEnd := time.Date(2024, 6, 1, 18, 0, 0, 0, time.UTC)
	migrationTime := time.Date(2024, 6, 1, 19, 0, 0, 0, time.UTC)
	schedule := newHourlyTestSchedule()

	newSchedulerState := func() *schedulerpb.SchedulerState {
		return &schedulerpb.SchedulerState{
			Namespace:     "ns",
			NamespaceId:   "ns-id",
			ScheduleId:    "sched-id",
			ConflictToken: 1,
			Schedule:      schedule,
			Info:          &schedulepb.ScheduleInfo{},
		}
	}
	newRangeBackfiller := func(attempt int64, lastProcessed *timestamppb.Timestamp) map[string]*schedulerpb.BackfillerState {
		return map[string]*schedulerpb.BackfillerState{
			"bf-1": {
				BackfillId:        "bf-1",
				Attempt:           attempt,
				LastProcessedTime: lastProcessed,
				Request: &schedulerpb.BackfillerState_BackfillRequest{
					BackfillRequest: &schedulepb.BackfillRequest{
						StartTime:     timestamppb.New(backfillStart),
						EndTime:       timestamppb.New(backfillEnd),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					},
				},
			},
		}
	}

	t.Run("unstarted backfiller keeps its start boundary inclusive", func(t *testing.T) {
		// Attempt == 0: CHASM has not processed anything yet, so the stored
		// request still carries the user's inclusive start time.
		backfillers := newRangeBackfiller(0, nil)

		args := migration.CHASMToLegacyStartScheduleArgs(
			newSchedulerState(), nil, nil, backfillers, nil, nil, nil, migrationTime)

		require.Len(t, args.State.OngoingBackfills, 1)
		cursor := args.State.OngoingBackfills[0].StartTime.AsTime()

		// Boundary semantics: V1 searches strictly after the stored cursor, so
		// the 12:00 action must still be generated.
		require.Equal(t, backfillStart, nextTimeAfter(t, schedule, cursor),
			"the action at the backfill start boundary must not be skipped after migration")

		// V1's own patch intake would have stored 11:59:59.999 for a backfill
		// requested to start (inclusively) at 12:00:00.
		require.Equal(t, backfillStart.Add(-time.Millisecond), cursor,
			"unstarted backfill must be exported with V1's inclusive-boundary offset applied")
	})

	t.Run("attempted backfiller keeps its exclusive watermark", func(t *testing.T) {
		// Attempt > 0: LastProcessedTime is a watermark for work already done,
		// so V1 must resume strictly after it. No offset may be applied here.
		lastProcessed := timestamppb.New(backfillStart)
		backfillers := newRangeBackfiller(2, lastProcessed)

		args := migration.CHASMToLegacyStartScheduleArgs(
			newSchedulerState(), nil, nil, backfillers, nil, nil, nil, migrationTime)

		require.Len(t, args.State.OngoingBackfills, 1)
		cursor := args.State.OngoingBackfills[0].StartTime.AsTime()

		require.Equal(t, backfillStart, cursor,
			"an attempted backfiller must export its watermark unshifted")
		require.Equal(t, backfillStart.Add(time.Hour), nextTimeAfter(t, schedule, cursor),
			"an already-processed action must not be regenerated after migration")
	})
}
