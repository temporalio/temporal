// The MIT License
//
// Copyright (c) 2024 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"context"
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

		mockMutableState *MockMutableState
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

	s.mockMutableState = NewMockMutableState(s.controller)

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
	err := PauseActivityById(s.mutableState, ai.ActivityId)
	s.NoError(err)
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")

	prevStamp = ai.Stamp
	err = ResetActivityById(s.mockShard, s.mutableState, ai.ActivityId, false, false)
	s.NoError(err)
	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is not reset")
	s.Equal(prevStamp, ai.Stamp, "ActivityInfo.Stamp should not change")
	s.True(ai.Paused, "ActivityInfo.Paused shouldn't change by reset")
}

func (s *activitySuite) TestUnpauseActivityWithResumeAcceptance() {
	ai := s.AddActivityInfo()

	prevStamp := ai.Stamp
	err := PauseActivityById(s.mutableState, ai.ActivityId)
	s.NoError(err)

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(true, ai.Paused, "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.Stamp
	_, err = UnpauseActivityWithResume(s.mockShard, s.mutableState, ai, false)
	s.NoError(err)

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(false, ai.Paused, "ActivityInfo.Paused was not unpaused")
}

func (s *activitySuite) TestUnpauseActivityWithNewRun() {
	ai := s.AddActivityInfo()

	prevStamp := ai.Stamp
	err := PauseActivityById(s.mutableState, ai.ActivityId)
	s.NoError(err)

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(true, ai.Paused, "ActivityInfo.Paused was not unpaused")
	prevStamp = ai.Stamp
	fakeScheduledTime := time.Now().UTC().Add(5 * time.Minute)
	ai.ScheduledTime = timestamppb.New(fakeScheduledTime)
	_, err = UnpauseActivityWithResume(s.mockShard, s.mutableState, ai, true)
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
	err := PauseActivityById(s.mutableState, ai.ActivityId)
	s.NoError(err)

	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Equal(true, ai.Paused, "ActivityInfo.Paused was not unpaused")

	prevStamp = ai.Stamp
	_, err = UnpauseActivityWithReset(s.mockShard, s.mutableState, ai, false, true)
	s.NoError(err)
	s.Equal(int32(1), ai.Attempt, "ActivityInfo.Attempt is shouldn't change")
	s.Equal(false, ai.Paused, "ActivityInfo.Paused was not unpaused")
	s.NotEqual(prevStamp, ai.Stamp, "ActivityInfo.Stamp should change")
	s.Nil(ai.LastHeartbeatUpdateTime)
	s.Nil(ai.LastHeartbeatDetails)
}
