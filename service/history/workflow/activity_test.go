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
	"google.golang.org/protobuf/proto"
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
		persistencespb.ShardInfo_builder{ShardId: 1}.Build(),
		config,
	)

	s.mockMutableState = historyi.NewMockMutableState(s.controller)

	s.mockNamespaceRegistry = s.mockShard.Resource.NamespaceCache

	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.GlobalNamespaceEntry.FailoverVersion(tests.WorkflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()

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
			ai: persistencespb.ActivityInfo_builder{
				CancelRequested: true,
				StartedEventId:  1,
			}.Build(),
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: persistencespb.ActivityInfo_builder{
				CancelRequested: true,
				StartedEventId:  common.EmptyEventID,
			}.Build(),
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: persistencespb.ActivityInfo_builder{
				CancelRequested: false,
				StartedEventId:  common.EmptyEventID,
			}.Build(),
			state: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		},
		{
			ai: persistencespb.ActivityInfo_builder{
				CancelRequested: false,
				StartedEventId:  1,
			}.Build(),
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
	activityType := commonpb.ActivityType_builder{
		Name: "activityType",
	}.Build()
	ai := persistencespb.ActivityInfo_builder{
		ActivityType:            activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}.Build()

	s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(activityType, nil).Times(1)
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
	activityType := commonpb.ActivityType_builder{
		Name: "activityType",
	}.Build()
	ai := persistencespb.ActivityInfo_builder{
		ActivityType:            activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}.Build()

	for _, tc := range testCases {
		ai.SetPaused(tc.paused)
		ai.SetCancelRequested(tc.cancelRequested)
		ai.SetStartedEventId(tc.startedEventId)

		s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(activityType, nil).Times(1)
		pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mockMutableState, ai)
		s.NoError(err)
		s.NotNil(pi)

		s.Equal(tc.expectedState, pi.GetState(), fmt.Sprintf("failed for paused: %v, cancelRequested: %v, startedEventId: %v", tc.paused, tc.cancelRequested, tc.startedEventId))
	}
}

func (s *activitySuite) TestGetPendingActivityInfoNoRetryPolicy() {
	now := s.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType_builder{
		Name: "activityType",
	}.Build()
	ai := persistencespb.ActivityInfo_builder{
		ActivityType:            activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}.Build()

	s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mockMutableState, ai)
	s.NoError(err)
	s.NotNil(pi)
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_STARTED, pi.GetState())
	s.Equal(int32(1), pi.GetAttempt())
	s.Nil(pi.GetNextAttemptScheduleTime())
	s.Nil(pi.GetCurrentRetryInterval())
}

func (s *activitySuite) TestGetPendingActivityInfoHasRetryPolicy() {
	now := s.mockShard.GetTimeSource().Now().UTC()
	activityType := commonpb.ActivityType_builder{
		Name: "activityType",
	}.Build()
	ai := persistencespb.ActivityInfo_builder{
		ActivityType:            activityType,
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
	}.Build()

	s.mockMutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mockMutableState, ai)
	s.NoError(err)
	s.NotNil(pi)
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pi.GetState())
	s.Equal(int32(2), pi.GetAttempt())
	s.Equal(ai.GetActivityId(), pi.GetActivityId())
	s.Nil(pi.GetHeartbeatDetails())
	s.Nil(pi.GetLastHeartbeatTime())
	s.Nil(pi.GetLastStartedTime())
	s.NotNil(pi.GetNextAttemptScheduleTime()) // activity is waiting for retry
	s.Equal(ai.GetRetryMaximumAttempts(), pi.GetMaximumAttempts())
	s.Nil(pi.AssignedBuildId)
	s.Equal(durationpb.New(2*time.Minute), pi.GetCurrentRetryInterval())
	s.Equal(ai.GetScheduledTime(), pi.GetScheduledTime())
	s.Equal(ai.GetLastAttemptCompleteTime(), pi.GetLastAttemptCompleteTime())
	s.NotNil(pi.GetActivityOptions())
	s.NotNil(pi.GetActivityOptions().GetRetryPolicy())
	s.Equal(ai.GetTaskQueue(), pi.GetActivityOptions().GetTaskQueue().GetName())
	s.Equal(ai.GetScheduleToCloseTimeout(), pi.GetActivityOptions().GetScheduleToCloseTimeout())
	s.Equal(ai.GetScheduleToStartTimeout(), pi.GetActivityOptions().GetScheduleToStartTimeout())
	s.Equal(ai.GetStartToCloseTimeout(), pi.GetActivityOptions().GetStartToCloseTimeout())
	s.Equal(ai.GetHeartbeatTimeout(), pi.GetActivityOptions().GetHeartbeatTimeout())
	s.Equal(ai.GetRetryMaximumInterval(), pi.GetActivityOptions().GetRetryPolicy().GetMaximumInterval())
	s.Equal(ai.GetRetryInitialInterval(), pi.GetActivityOptions().GetRetryPolicy().GetInitialInterval())
	s.Equal(ai.GetRetryBackoffCoefficient(), pi.GetActivityOptions().GetRetryPolicy().GetBackoffCoefficient())
	s.Equal(ai.GetRetryMaximumAttempts(), pi.GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
}

func (s *activitySuite) AddActivityInfo() *persistencespb.ActivityInfo {
	activityId := "activity-id"
	activityScheduledEvent := historypb.HistoryEvent_builder{
		EventId:   1,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		ActivityTaskScheduledEventAttributes: historypb.ActivityTaskScheduledEventAttributes_builder{
			WorkflowTaskCompletedEventId: 4,
			ActivityId:                   activityId,
			ActivityType:                 commonpb.ActivityType_builder{Name: "activity-type"}.Build(),
			TaskQueue:                    taskqueuepb.TaskQueue_builder{Name: "task-queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			Input:                        nil,
			ScheduleToCloseTimeout:       durationpb.New(20 * time.Second),
			ScheduleToStartTimeout:       durationpb.New(20 * time.Second),
			StartToCloseTimeout:          durationpb.New(20 * time.Second),
			HeartbeatTimeout:             durationpb.New(20 * time.Second),
		}.Build(),
	}.Build()

	s.mockShard.MockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	ai, err := s.mutableState.ApplyActivityTaskScheduledEvent(0, activityScheduledEvent)
	s.NoError(err)
	return ai
}

func (s *activitySuite) TestResetPausedActivityAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.GetStamp()
	pauseInfo := persistencespb.ActivityInfo_PauseInfo_builder{
		PauseTime: timestamppb.New(time.Now()),
		Manual: persistencespb.ActivityInfo_PauseInfo_Manual_builder{
			Identity: "test_identity",
			Reason:   "test_reason",
		}.Build(),
	}.Build()

	err := PauseActivity(s.mutableState, ai.GetActivityId(), pauseInfo)
	s.NoError(err)
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.NotNil(ai.GetPauseInfo())
	s.Equal(ai.GetPauseInfo().GetManual().GetIdentity(), "test_identity")
	s.Equal(ai.GetPauseInfo().GetManual().GetReason(), "test_reason")

	prevStamp = ai.GetStamp()
	err = ResetActivity(context.Background(), s.mockShard, s.mutableState, ai.GetActivityId(),
		false, true, false, 0)
	s.NoError(err)
	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is not reset")
	s.Equal(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should not change")
	s.True(ai.GetPaused(), "ActivityInfo.Paused shouldn't change by reset")
}

func (s *activitySuite) TestResetAndUnPauseActivityAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.GetStamp()
	pauseInfo := persistencespb.ActivityInfo_PauseInfo_builder{
		PauseTime: timestamppb.New(time.Now()),
		Manual: persistencespb.ActivityInfo_PauseInfo_Manual_builder{
			Identity: "test_identity",
			Reason:   "test_reason",
		}.Build(),
	}.Build()

	err := PauseActivity(s.mutableState, ai.GetActivityId(), pauseInfo)
	s.NoError(err)
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.NotNil(ai.GetPauseInfo())
	s.Equal(ai.GetPauseInfo().GetManual().GetIdentity(), "test_identity")
	s.Equal(ai.GetPauseInfo().GetManual().GetReason(), "test_reason")

	prevStamp = ai.GetStamp()
	err = ResetActivity(context.Background(), s.mockShard, s.mutableState, ai.GetActivityId(),
		false, false, false, 0)
	s.NoError(err)
	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is not reset")
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.False(ai.GetPaused(), "ActivityInfo.Paused shouldn't change by reset")
}

func (s *activitySuite) TestUnpauseActivityWithResumeAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.GetStamp()
	err := PauseActivity(s.mutableState, ai.GetActivityId(), nil)
	s.NoError(err)
	s.Nil(ai.GetPauseInfo())

	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.Equal(true, ai.GetPaused(), "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.GetStamp()
	_, err = UnpauseActivityWithResume(s.mockShard, s.mutableState, ai, false, 0)
	s.NoError(err)

	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.Equal(false, ai.GetPaused(), "ActivityInfo.Paused was not unpaused")
}

func (s *activitySuite) TestUnpauseActivityWithNewRun() {
	ai := s.AddActivityInfo()

	prevStamp := ai.GetStamp()
	err := PauseActivity(s.mutableState, ai.GetActivityId(), nil)
	s.NoError(err)

	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.Equal(true, ai.GetPaused(), "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.GetStamp()
	fakeScheduledTime := time.Now().UTC().Add(5 * time.Minute)
	ai.SetScheduledTime(timestamppb.New(fakeScheduledTime))
	_, err = UnpauseActivityWithResume(s.mockShard, s.mutableState, ai, true, 0)
	s.NoError(err)

	// scheduled time should be reset to
	s.NotEqual(fakeScheduledTime, ai.GetScheduledTime().AsTime())
	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.Equal(false, ai.GetPaused(), "ActivityInfo.Paused was not unpaused")
}

func (s *activitySuite) TestUnpauseActivityWithResetAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.GetStamp()
	pauseInfo := persistencespb.ActivityInfo_PauseInfo_builder{
		PauseTime: timestamppb.New(time.Now()),
		RuleId:    proto.String("rule_id"),
	}.Build()

	err := PauseActivity(s.mutableState, ai.GetActivityId(), pauseInfo)
	s.NoError(err)
	s.NotNil(ai.GetPauseInfo())
	s.Equal(ai.GetPauseInfo().GetRuleId(), "rule_id")

	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.Equal(true, ai.GetPaused(), "ActivityInfo.Paused was not unpaused")

	prevStamp = ai.GetStamp()
	_, err = UnpauseActivityWithReset(s.mockShard, s.mutableState, ai, false, true, 0)
	s.NoError(err)
	s.Equal(int32(1), ai.GetAttempt(), "ActivityInfo.Attempt is shouldn't change")
	s.Equal(false, ai.GetPaused(), "ActivityInfo.Paused was not unpaused")
	s.NotEqual(prevStamp, ai.GetStamp(), "ActivityInfo.Stamp should change")
	s.Nil(ai.GetLastHeartbeatUpdateTime())
	s.Nil(ai.GetLastHeartbeatDetails())
}
