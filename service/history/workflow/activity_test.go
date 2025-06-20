package workflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	activitySuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockNamespaceRegistry *namespace.MockRegistry

		mockMutableState *historyi.MockMutableState
		mutableState     *MutableStateImpl
	}
)

func TestActivitySuite(t *testing.T) {
	s := new(activitySuite)
	suite.Run(t, s)
}

func (s *activitySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()
	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{ShardId: 1},
		config,
	)

	s.mockMutableState = historyi.NewMockMutableState(s.controller)

	s.mockNamespaceRegistry = s.mockShard.Resource.NamespaceCache

	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.GlobalNamespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	reg := hsm.NewRegistry()
	err := RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)

	s.mutableState = NewMutableState(
		s.mockShard, s.mockShard.MockEventsCache, s.mockShard.GetLogger(), tests.GlobalNamespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC(),
	)
}

func (s *activitySuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *activitySuite) TestGetActivityState() {
	testCases := []struct {
		ai    *persistencespb.ActivityInfo
		state enumspb.PendingActivityState
	}{
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: true,
				StartedEventId:  1,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: true,
				StartedEventId:  common.EmptyEventID,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: false,
				StartedEventId:  common.EmptyEventID,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: false,
				StartedEventId:  1,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
	}

	for _, tc := range testCases {
		state := GetActivityState(tc.ai)
		s.Equal(tc.state, state)
	}
}

func (s *activitySuite) TestGetPendingActivityInfoAcceptance() {
	now := s.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}

	s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mockMutableState, ai)
	s.NoError(err)
	s.NotNil(pi)
}

func (s *activitySuite) TestGetPendingActivityInfo_ActivityState() {
	testCases := []struct {
		paused          bool
		cancelRequested bool
		startedEventId  int64
		expectedState   enumspb.PendingActivityState
	}{
		{
			paused:          false,
			cancelRequested: false,
			startedEventId:  common.EmptyEventID,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		},
		{
			paused:          false,
			cancelRequested: false,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
		{
			paused:          false,
			cancelRequested: true,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			paused:          true,
			cancelRequested: false,
			startedEventId:  common.EmptyEventID,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_PAUSED,
		},
		{
			paused:          true,
			cancelRequested: false,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
		},
		{
			paused:          true,
			cancelRequested: true,
			startedEventId:  1,
			expectedState:   enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
	}

	now := s.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}

	for _, tc := range testCases {
		ai.Paused = tc.paused
		ai.CancelRequested = tc.cancelRequested
		ai.StartedEventId = tc.startedEventId

		s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
		pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mockMutableState, ai)
		s.NoError(err)
		s.NotNil(pi)

		s.Equal(tc.expectedState, pi.State, fmt.Sprintf("failed for paused: %v, cancelRequested: %v, startedEventId: %v", tc.paused, tc.cancelRequested, tc.startedEventId))
	}
}

func (s *activitySuite) TestGetPendingActivityInfoNoRetryPolicy() {
	now := s.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}

	s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mockMutableState, ai)
	s.NoError(err)
	s.NotNil(pi)
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_STARTED, pi.State)
	s.Equal(int32(1), pi.Attempt)
	s.Nil(pi.NextAttemptScheduleTime)
	s.Nil(pi.CurrentRetryInterval)
}

func (s *activitySuite) TestGetPendingActivityInfoHasRetryPolicy() {
	now := s.mockShard.GetTimeSource().Now().UTC()
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          common.EmptyEventID,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now.Add(1 * time.Minute)),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Minute)),
		HasRetryPolicy:          true,
		RetryMaximumInterval:    nil,
		RetryInitialInterval:    durationpb.New(time.Minute),
		RetryBackoffCoefficient: 1.0,
		RetryMaximumAttempts:    10,
		TaskQueue:               "task-queue",
	}

	s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mockMutableState, ai)
	s.NoError(err)
	s.NotNil(pi)
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pi.State)
	s.Equal(int32(2), pi.Attempt)
	s.Equal(ai.ActivityId, pi.ActivityId)
	s.Nil(pi.HeartbeatDetails)
	s.Nil(pi.LastHeartbeatTime)
	s.Nil(pi.LastStartedTime)
	s.NotNil(pi.NextAttemptScheduleTime) // activity is waiting for retry
	s.Equal(ai.RetryMaximumAttempts, pi.MaximumAttempts)
	s.Nil(pi.AssignedBuildId)
	s.Equal(durationpb.New(2*time.Minute), pi.CurrentRetryInterval)
	s.Equal(ai.ScheduledTime, pi.ScheduledTime)
	s.Equal(ai.LastAttemptCompleteTime, pi.LastAttemptCompleteTime)
	s.NotNil(pi.ActivityOptions)
	s.NotNil(pi.ActivityOptions.RetryPolicy)
	s.Equal(ai.TaskQueue, pi.ActivityOptions.TaskQueue.Name)
	s.Equal(ai.ScheduleToCloseTimeout, pi.ActivityOptions.ScheduleToCloseTimeout)
	s.Equal(ai.ScheduleToStartTimeout, pi.ActivityOptions.ScheduleToStartTimeout)
	s.Equal(ai.StartToCloseTimeout, pi.ActivityOptions.StartToCloseTimeout)
	s.Equal(ai.HeartbeatTimeout, pi.ActivityOptions.HeartbeatTimeout)
	s.Equal(ai.RetryMaximumInterval, pi.ActivityOptions.RetryPolicy.MaximumInterval)
	s.Equal(ai.RetryInitialInterval, pi.ActivityOptions.RetryPolicy.InitialInterval)
	s.Equal(ai.RetryBackoffCoefficient, pi.ActivityOptions.RetryPolicy.BackoffCoefficient)
	s.Equal(ai.RetryMaximumAttempts, pi.ActivityOptions.RetryPolicy.MaximumAttempts)
}

func (s *activitySuite) AddActivityInfo() *persistencespb.ActivityInfo {
	activityId := "activity-id"
	activityScheduledEvent := &historypb.HistoryEvent{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				WorkflowTaskCompletedEventId: 4,
				ActivityId:                   activityId,
				ActivityType:                 &commonpb.ActivityType{Name: "activity-type"},
				TaskQueue:                    &taskqueuepb.TaskQueue{Name: "task-queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:                        nil,
				ScheduleToCloseTimeout:       durationpb.New(20 * time.Second),
				ScheduleToStartTimeout:       durationpb.New(20 * time.Second),
				StartToCloseTimeout:          durationpb.New(20 * time.Second),
				HeartbeatTimeout:             durationpb.New(20 * time.Second),
			}},
	}

	s.mockShard.MockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	ai, err := s.mutableState.ApplyActivityTaskScheduledEvent(0, activityScheduledEvent)
	s.NoError(err)
	return ai
}

func (s *activitySuite) TestResetPausedActivityAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.Stamp
	pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
		PauseTime: timestamppb.New(time.Now()),
		PausedBy: &persistencespb.ActivityInfo_PauseInfo_Manual_{
			Manual: &persistencespb.ActivityInfo_PauseInfo_Manual{
				Identity: "test_identity",
				Reason:   "test_reason",
			},
		},
	}

	err := PauseActivity(s.mutableState, ai.ActivityId, pauseInfo)
	s.NoError(err)
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.NotNil(ai.PauseInfo)
	s.Equal(ai.PauseInfo.GetManual().Identity, "test_identity")
	s.Equal(ai.PauseInfo.GetManual().Reason, "test_reason")

	prevStamp = ai.Stamp
	err = ResetActivity(context.Background(), s.mockShard, s.mutableState, ai.ActivityId,
		false, true, false, 0)
	s.NoError(err)
	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is not reset")
	s.Equal(prevStamp, ai.Stamp, "ActivityInfo.Stamp should not change")
	s.True(ai.Paused, "ActivityInfo.Paused shouldn't change by reset")
}

func (s *activitySuite) TestResetAndUnPauseActivityAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.Stamp
	pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
		PauseTime: timestamppb.New(time.Now()),
		PausedBy: &persistencespb.ActivityInfo_PauseInfo_Manual_{
			Manual: &persistencespb.ActivityInfo_PauseInfo_Manual{
				Identity: "test_identity",
				Reason:   "test_reason",
			},
		},
	}

	err := PauseActivity(s.mutableState, ai.ActivityId, pauseInfo)
	s.NoError(err)
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.NotNil(ai.PauseInfo)
	s.Equal(ai.PauseInfo.GetManual().Identity, "test_identity")
	s.Equal(ai.PauseInfo.GetManual().Reason, "test_reason")

	prevStamp = ai.Stamp
	err = ResetActivity(context.Background(), s.mockShard, s.mutableState, ai.ActivityId,
		false, false, false, 0)
	s.NoError(err)
	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is not reset")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.False(ai.Paused, "ActivityInfo.Paused shouldn't change by reset")
}

func (s *activitySuite) TestUnpauseActivityWithResumeAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.Stamp
	err := PauseActivity(s.mutableState, ai.ActivityId, nil)
	s.NoError(err)
	s.Nil(ai.PauseInfo)

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(true, ai.Paused, "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.Stamp
	_, err = UnpauseActivityWithResume(s.mockShard, s.mutableState, ai, false, 0)
	s.NoError(err)

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(false, ai.Paused, "ActivityInfo.Paused was not unpaused")
}

func (s *activitySuite) TestUnpauseActivityWithNewRun() {
	ai := s.AddActivityInfo()

	prevStamp := ai.Stamp
	err := PauseActivity(s.mutableState, ai.ActivityId, nil)
	s.NoError(err)

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(true, ai.Paused, "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.Stamp
	fakeScheduledTime := time.Now().UTC().Add(5 * time.Minute)
	ai.ScheduledTime = timestamppb.New(fakeScheduledTime)
	_, err = UnpauseActivityWithResume(s.mockShard, s.mutableState, ai, true, 0)
	s.NoError(err)

	// scheduled time should be reset to
	s.NotEqual(fakeScheduledTime, ai.ScheduledTime.AsTime())
	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(false, ai.Paused, "ActivityInfo.Paused was not unpaused")
}

func (s *activitySuite) TestUnpauseActivityWithResetAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.Stamp
	pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
		PauseTime: timestamppb.New(time.Now()),
		PausedBy: &persistencespb.ActivityInfo_PauseInfo_RuleId{
			RuleId: "rule_id",
		},
	}

	err := PauseActivity(s.mutableState, ai.ActivityId, pauseInfo)
	s.NoError(err)
	s.NotNil(ai.PauseInfo)
	s.Equal(ai.PauseInfo.GetRuleId(), "rule_id")

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(true, ai.Paused, "ActivityInfo.Paused was not unpaused")

	prevStamp = ai.Stamp
	_, err = UnpauseActivityWithReset(s.mockShard, s.mutableState, ai, false, true, 0)
	s.NoError(err)
	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.Equal(false, ai.Paused, "ActivityInfo.Paused was not unpaused")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Nil(ai.LastHeartbeatUpdateTime)
	s.Nil(ai.LastHeartbeatDetails)
}
