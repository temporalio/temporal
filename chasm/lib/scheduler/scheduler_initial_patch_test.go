package scheduler_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/history/tasks"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
)

// hasImmediateTask reports whether any immediate CHASM task was queued on the
// node backend. Used to show that generator/backfiller work really was armed
// during construction, so the pause state asserted before commit is the state
// those tasks will observe when they run.
func hasImmediateTask(nodeBackend *chasm.MockNodeBackend) bool {
	want := reflect.TypeOf(&tasks.ChasmTask{})
	for _, categoryTasks := range nodeBackend.TasksByCategory {
		for _, t := range categoryTasks {
			if reflect.TypeOf(t) == want &&
				t.GetVisibilityTime().Equal(chasm.TaskScheduledTimeImmediate) {
				return true
			}
		}
	}
	return false
}

// TestCreateScheduler_InitialPauseState pins that CreateSchedule honors the
// Pause and Unpause members of InitialPatch, matching the workflow-backed
// scheduler, whose initial patch runs through processPatch.
//
// Regression test for SCH-035: the CHASM creation path fed InitialPatch only to
// handlePatch (trigger/backfill), so a schedule requested paused at creation
// started active and could immediately generate and start actions.
func TestCreateScheduler_InitialPauseState(t *testing.T) {
	testCases := []struct {
		name         string
		inputPaused  bool
		inputNotes   string
		patch        *schedulepb.SchedulePatch
		expectPaused bool
		expectNotes  string
	}{
		{
			name:         "pause applied to active schedule",
			inputPaused:  false,
			patch:        &schedulepb.SchedulePatch{Pause: "maintenance"},
			expectPaused: true,
			expectNotes:  "maintenance",
		},
		{
			name:         "unpause applied to paused schedule",
			inputPaused:  true,
			inputNotes:   "created paused",
			patch:        &schedulepb.SchedulePatch{Unpause: "resume"},
			expectPaused: false,
			expectNotes:  "resume",
		},
		{
			// Contradictory patch. V1's processPatch applies Pause and then
			// Unpause, so Unpause wins; CHASM must tie-break identically.
			name:         "both set, unpause wins",
			inputPaused:  false,
			patch:        &schedulepb.SchedulePatch{Pause: "maintenance", Unpause: "resume"},
			expectPaused: false,
			expectNotes:  "resume",
		},
		{
			name:        "patch without pause members leaves state untouched",
			inputPaused: true,
			inputNotes:  "created paused",
			patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
			},
			expectPaused: true,
			expectNotes:  "created paused",
		},
		{
			name:         "nil patch leaves state untouched",
			inputPaused:  true,
			inputNotes:   "created paused",
			patch:        nil,
			expectPaused: true,
			expectNotes:  "created paused",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			// The immediate generator task runs as a pure task while the
			// creating transaction closes. Record the paused state it sees, so
			// the test can prove the task didn't run against pre-patch state.
			var generatorSawPaused []bool
			specProcessor := scheduler.NewMockSpecProcessor(ctrl)
			specProcessor.EXPECT().ProcessTimeRange(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			).DoAndReturn(func(
				sched *scheduler.Scheduler,
				_, _ time.Time,
				_ enumspb.ScheduleOverlapPolicy,
				_, _ string,
				_ bool,
				_ *int,
			) (*scheduler.ProcessedTimeRange, error) {
				generatorSawPaused = append(generatorSawPaused, sched.Schedule.GetState().GetPaused())
				return &scheduler.ProcessedTimeRange{
					NextWakeupTime: time.Now().Add(time.Hour),
					LastActionTime: time.Now(),
				}, nil
			}).AnyTimes()
			specProcessor.EXPECT().NextTime(gomock.Any(), gomock.Any()).Return(legacyscheduler.GetNextTimeResult{
				Next:    time.Now().Add(time.Hour),
				Nominal: time.Now().Add(time.Hour),
			}, nil).AnyTimes()

			infra := setupTestInfra(t, specProcessor)

			input := defaultSchedule()
			input.State.Paused = tc.inputPaused
			input.State.Notes = tc.inputNotes

			ctx := chasm.NewMutableContext(context.Background(), infra.node)
			sched, err := scheduler.CreateScheduler(ctx, &schedulerpb.CreateScheduleRequest{
				NamespaceId: namespaceID,
				FrontendRequest: &workflowservice.CreateScheduleRequest{
					Namespace:    namespace,
					ScheduleId:   scheduleID,
					Schedule:     input,
					InitialPatch: tc.patch,
				},
			})
			require.NoError(t, err)
			require.NoError(t, infra.node.SetRootComponent(sched))

			// Assert before closing the transaction. Tasks queued during
			// construction (the immediate generator run, plus any backfiller)
			// only become runnable once the transaction commits, so the state
			// seen here is the state those tasks will observe.
			require.Equal(t, tc.expectPaused, sched.Schedule.GetState().GetPaused(),
				"paused state must be applied during construction")
			require.Equal(t, tc.expectNotes, sched.Schedule.GetState().GetNotes())

			// Creation returns the schedule with its initial conflict token: the
			// CreateSchedule response hands the caller initialSerializedConflictToken,
			// so bumping here would immediately invalidate it.
			require.Equal(t, int64(legacyscheduler.InitialConflictToken), sched.ConflictToken)

			_, err = infra.node.CloseTransaction()
			require.NoError(t, err)

			// Work really was armed during construction, and the state it runs
			// against is the requested one.
			require.True(t, hasImmediateTask(infra.nodeBackend),
				"expected construction to arm immediate work")
			require.Equal(t, tc.expectPaused, sched.Schedule.GetState().GetPaused())
			require.Equal(t, tc.expectNotes, sched.Schedule.GetState().GetNotes())
			for _, sawPaused := range generatorSawPaused {
				require.Equal(t, tc.expectPaused, sawPaused,
					"generator ran against pre-patch pause state")
			}
		})
	}
}
