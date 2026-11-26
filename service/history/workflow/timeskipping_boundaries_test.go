package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/protorequire"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActivityReschedulingUsesVirtualTime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		run  func(context.Context, historyi.MutableState, *persistencespb.ActivityInfo) error
	}{
		{
			name: "Unpause",
			run: func(_ context.Context, ms historyi.MutableState, ai *persistencespb.ActivityInfo) error {
				return UnpauseActivity(ms, ai, false, false, 0)
			},
		},
		{
			name: "Reset",
			run: func(ctx context.Context, ms historyi.MutableState, ai *persistencespb.ActivityInfo) error {
				return ResetActivity(ctx, ms, ai.ActivityId, false, false, false, 0)
			},
		},
		{
			name: "ResumeNewRun",
			run: func(_ context.Context, ms historyi.MutableState, ai *persistencespb.ActivityInfo) error {
				_, err := UnpauseActivityWithResume(ms, ai, true, 0)
				return err
			},
		},
		{
			name: "ResetNewRun",
			run: func(_ context.Context, ms historyi.MutableState, ai *persistencespb.ActivityInfo) error {
				_, err := UnpauseActivityWithReset(ms, ai, true, false, 0)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			virtualNow := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
			ms := historyi.NewMockMutableState(ctrl)
			ms.EXPECT().Now().Return(virtualNow)
			ai := &persistencespb.ActivityInfo{
				ActivityId: "activity", ScheduledEventId: 1, StartedEventId: common.EmptyEventID, Paused: true,
			}
			ms.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
			ms.EXPECT().GetActivityByActivityID(ai.ActivityId).Return(ai, true).AnyTimes()
			ms.EXPECT().UpdateActivity(ai.ScheduledEventId, gomock.Any()).DoAndReturn(
				func(_ int64, updater historyi.ActivityUpdater) error { return updater(ai, ms) },
			)
			var scheduledTime time.Time
			ms.EXPECT().RegenerateActivityRetryTask(ai, gomock.Any()).DoAndReturn(
				func(_ *persistencespb.ActivityInfo, when time.Time) error {
					scheduledTime = when
					return nil
				},
			)

			require.NoError(t, tc.run(t.Context(), ms, ai))
			require.Equal(t, virtualNow, scheduledTime)
		})
	}
}

func TestPendingActivityRetryUsesVirtualTime(t *testing.T) {
	t.Parallel()
	virtualNow := time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		delay     time.Duration
		inBackoff bool
	}{
		{name: "BeforeDispatch", delay: time.Minute, inBackoff: true},
		{name: "DueNow"},
		{name: "AfterDispatch", delay: -time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ms := historyi.NewMockMutableState(ctrl)
			ms.EXPECT().Now().Return(virtualNow)
			ms.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&commonpb.ActivityType{Name: "activity"}, nil)
			ai := &persistencespb.ActivityInfo{
				StartedEventId: common.EmptyEventID, HasRetryPolicy: true,
				ScheduledTime:           timestamppb.New(virtualNow.Add(tc.delay)),
				LastAttemptCompleteTime: timestamppb.New(virtualNow.Add(-2 * time.Minute)),
			}

			result, err := GetPendingActivityInfo(t.Context(), ms, ai)
			require.NoError(t, err)
			if tc.inBackoff {
				protorequire.ProtoEqual(t, ai.ScheduledTime, result.NextAttemptScheduleTime)
				protorequire.ProtoEqual(t, durationpb.New(2*time.Minute+tc.delay), result.CurrentRetryInterval)
			} else {
				require.Nil(t, result.NextAttemptScheduleTime)
				require.Nil(t, result.CurrentRetryInterval)
			}
		})
	}
}

func TestWorkflowTaskTimeoutDeletionWithTimeSkipping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		info    *persistencespb.TimeSkippingInfo
		skipped time.Duration
	}{
		{name: "NilInfo"},
		{name: "EmptyInfo", info: &persistencespb.TimeSkippingInfo{}},
		{name: "ZeroOffset", info: &persistencespb.TimeSkippingInfo{AccumulatedSkippedDuration: durationpb.New(0)}},
		{name: "Skipped", info: &persistencespb.TimeSkippingInfo{AccumulatedSkippedDuration: durationpb.New(time.Hour)}, skipped: time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, timeout := range []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute} {
				t.Run(timeout.String(), func(t *testing.T) {
					t.Parallel()
					for _, scheduleToStart := range []bool{false, true} {
						name := "StartToClose"
						if scheduleToStart {
							name = "ScheduleToStart"
						}
						t.Run(name, func(t *testing.T) {
							t.Parallel()
							virtualNow := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).Add(tc.skipped)
							ts := clock.NewEventTimeSource()
							ts.Update(virtualNow)
							ms := &MutableStateImpl{
								timeSource: ts,
								executionInfo: &persistencespb.WorkflowExecutionInfo{
									TimeSkippingInfo: tc.info,
								},
								InsertTasks:           make(map[tasks.Category][]tasks.Task),
								BestEffortDeleteTasks: make(map[tasks.Category][]tasks.Key),
							}
							timeoutTask := &tasks.WorkflowTaskTimeoutTask{VisibilityTimestamp: virtualNow.Add(timeout)}
							ms.AddTasks(timeoutTask)
							workflowTask := &historyi.WorkflowTaskInfo{StartedTime: virtualNow, ScheduledTime: virtualNow}
							if scheduleToStart {
								workflowTask.ScheduleToStartTimeoutTask = timeoutTask
							} else {
								workflowTask.StartToCloseTimeoutTask = timeoutTask
							}

							sm := &workflowTaskStateMachine{ms: ms}
							sm.recordTimeoutTasksForDeletion(workflowTask)

							expected := make(map[tasks.Category][]tasks.Key)
							if timeout < 2*time.Minute {
								expected[tasks.CategoryTimer] = []tasks.Key{timeoutTask.GetKey()}
							}
							require.Equal(t, expected, ms.BestEffortDeleteTasks)
						})
					}
				})
			}
		})
	}
}
