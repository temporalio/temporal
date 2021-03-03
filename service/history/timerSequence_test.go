// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package history

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	timerSequenceSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockMutableState    *MockmutableState
		mockEventTimeSource clock.TimeSource

		timerSequence *timerSequenceImpl
	}
)

func TestTimerSequenceSuite(t *testing.T) {
	s := new(timerSequenceSuite)
	suite.Run(t, s)
}

func (s *timerSequenceSuite) SetupSuite() {

}

func (s *timerSequenceSuite) TearDownSuite() {

}

func (s *timerSequenceSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockMutableState = NewMockmutableState(s.controller)
	s.mockEventTimeSource = clock.NewEventTimeSource()

	s.timerSequence = newTimerSequence(s.mockEventTimeSource, s.mockMutableState)
}

func (s *timerSequenceSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_AlreadyCreated_AfterWorkflowExpiry() {
	now := timestamp.TimeNowPtrUtc()
	timerExpiry := timestamp.TimePtr(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timerExpiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(timerExpiry.Add(-1 * time.Second)),
	})

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_AlreadyCreated_BeforeWorkflowExpiry() {
	now := timestamp.TimeNowPtrUtc()
	timerExpiry := timestamp.TimePtr(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timerExpiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(timerExpiry.Add(1 * time.Second)),
	})

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_AlreadyCreated_NoWorkflowExpiry() {
	now := timestamp.TimeNowPtrUtc()
	timer1Expiry := timestamp.TimePtr(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timer1Expiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: &time.Time{},
	})

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_NotCreated_AfterWorkflowExpiry() {
	now := timestamp.TimeNowPtrUtc()
	timerExpiry := timestamp.TimePtr(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timerExpiry,
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(timerExpiry.Add(-1 * time.Second)),
	})

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_NotCreated_BeforeWorkflowExpiry() {
	now := timestamp.TimeNowPtrUtc()
	timerExpiry := timestamp.TimePtr(now.Add(100))
	currentVersion := int64(999)
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timerExpiry,
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	s.mockMutableState.EXPECT().GetUserTimerInfoByEventID(timerInfo.StartedId).Return(timerInfo, true)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(timerExpiry.Add(1 * time.Second)),
	})

	var timerInfoUpdated = *timerInfo // make a copy
	timerInfoUpdated.TaskStatus = timerTaskStatusCreated
	s.mockMutableState.EXPECT().UpdateUserTimer(&timerInfoUpdated).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.UserTimerTask{
		// TaskID is set by shard
		VisibilityTimestamp: *timerExpiry,
		EventID:             timerInfo.GetStartedId(),
		Version:             currentVersion,
	})

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_NotCreated_NoWorkflowExpiry() {
	now := timestamp.TimeNowPtrUtc()
	timerExpiry := timestamp.TimePtr(now.Add(100))
	currentVersion := int64(999)
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timerExpiry,
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)
	s.mockMutableState.EXPECT().GetUserTimerInfoByEventID(timerInfo.StartedId).Return(timerInfo, true)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: &time.Time{},
	})

	var timerInfoUpdated = *timerInfo // make a copy
	timerInfoUpdated.TaskStatus = timerTaskStatusCreated
	s.mockMutableState.EXPECT().UpdateUserTimer(&timerInfoUpdated).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.UserTimerTask{
		// TaskID is set by shard
		VisibilityTimestamp: *timerExpiry,
		EventID:             timerInfo.GetStartedId(),
		Version:             currentVersion,
	})

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_AlreadyCreated_AfterWorkflowExpiry() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(now.Add(-2000 * time.Second)),
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_AlreadyCreated_BeforeWorkflowExpiry() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(now.Add(2000 * time.Second)),
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_AlreadyCreated_NoWorkflowExpiry() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(time.Time{}),
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_NotCreated_AfterWorkflowExpiry() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(now.Add(-2000 * time.Second)),
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_NotCreated_BeforeWorkflowExpiry() {
	now := time.Now().UTC()
	currentVersion := int64(999)
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduleId).Return(activityInfo, true)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(now.Add(2000 * time.Second)),
	})

	var activityInfoUpdated = *activityInfo // make a copy
	activityInfoUpdated.TimerTaskStatus = timerTaskStatusCreatedScheduleToStart
	s.mockMutableState.EXPECT().UpdateActivity(&activityInfoUpdated).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToStartTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             activityInfo.ScheduleId,
		Attempt:             activityInfo.Attempt,
		Version:             currentVersion,
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_NotCreated_NoWorkflowExpiry() {
	now := time.Now().UTC()
	currentVersion := int64(999)
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduleId).Return(activityInfo, true)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(time.Time{}),
	})

	var activityInfoUpdated = *activityInfo // make a copy
	activityInfoUpdated.TimerTaskStatus = timerTaskStatusCreatedScheduleToStart
	s.mockMutableState.EXPECT().UpdateActivity(&activityInfoUpdated).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToStartTimeout),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             activityInfo.ScheduleId,
		Attempt:             activityInfo.Attempt,
		Version:             currentVersion,
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_HeartbeatTimer_AfterWorkflowExpiry() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(now.Add(-2000 * time.Second)),
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_HeartbeatTimer_BeforeWorkflowExpiry() {
	now := time.Now().UTC()
	currentVersion := int64(999)
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduleId).Return(activityInfo, true)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(now.Add(2000 * time.Second)),
	})

	taskVisibilityTimestamp := activityInfo.StartedTime.Add(*activityInfo.HeartbeatTimeout)

	var activityInfoUpdated = *activityInfo // make a copy
	activityInfoUpdated.TimerTaskStatus = timerTaskStatusCreatedHeartbeat
	s.mockMutableState.EXPECT().UpdateActivityWithTimerHeartbeat(&activityInfoUpdated, taskVisibilityTimestamp).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: taskVisibilityTimestamp,
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		EventID:             activityInfo.ScheduleId,
		Attempt:             activityInfo.Attempt,
		Version:             currentVersion,
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_HeartbeatTimer_NoWorkflowExpiry() {
	now := time.Now().UTC()
	currentVersion := int64(999)
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	s.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduleId).Return(activityInfo, true)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowRunExpirationTime: timestamp.TimePtr(time.Time{}),
	})

	taskVisibilityTimestamp := activityInfo.StartedTime.Add(*activityInfo.HeartbeatTimeout)

	var activityInfoUpdated = *activityInfo // make a copy
	activityInfoUpdated.TimerTaskStatus = timerTaskStatusCreatedHeartbeat
	s.mockMutableState.EXPECT().UpdateActivityWithTimerHeartbeat(&activityInfoUpdated, taskVisibilityTimestamp).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: taskVisibilityTimestamp,
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		EventID:             activityInfo.ScheduleId,
		Attempt:             activityInfo.Attempt,
		Version:             currentVersion,
	})

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_None() {
	timerInfos := map[string]*persistencespb.TimerInfo{}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Empty(timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_One() {
	now := timestamp.TimeNowPtrUtc()
	timer1Expiry := timestamp.TimePtr(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timer1Expiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Equal([]timerSequenceID{{
		eventID:      timerInfo.GetStartedId(),
		timestamp:    *timer1Expiry,
		timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		timerCreated: true,
		attempt:      1,
	}}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_Multiple() {
	now := timestamp.TimeNowPtrUtc()
	timer1Expiry := timestamp.TimePtr(now.Add(100))
	timer2Expiry := timestamp.TimePtr(now.Add(200))
	timerInfo1 := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timer1Expiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfo2 := &persistencespb.TimerInfo{
		Version:    1234,
		TimerId:    "other random timer ID",
		StartedId:  4567,
		ExpiryTime: timestamp.TimePtr(now.Add(200)),
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistencespb.TimerInfo{
		timerInfo1.TimerId: timerInfo1,
		timerInfo2.TimerId: timerInfo2,
	}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      timerInfo1.GetStartedId(),
			timestamp:    *timer1Expiry,
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      1,
		},
		{
			eventID:      timerInfo2.GetStartedId(),
			timestamp:    *timer2Expiry,
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: false,
			attempt:      1,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_None() {
	activityInfos := map[int64]*persistencespb.ActivityInfo{}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Empty(timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_NotScheduled() {
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              common.EmptyEventID,
		ScheduledTime:           timestamp.TimePtr(time.Time{}),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusNone,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Empty(timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_NotStarted() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToStartTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_WithHeartbeatTimeout() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose | timerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.StartedTime.Add(*activityInfo.HeartbeatTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.StartedTime.Add(*activityInfo.StartToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_WithoutHeartbeatTimeout() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.StartedTime.Add(*activityInfo.StartToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_Heartbeated_WithHeartbeatTimeout() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose | timerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.LastHeartbeatUpdateTime.Add(*activityInfo.HeartbeatTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.StartedTime.Add(*activityInfo.StartToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_Heartbeated_WithoutHeartbeatTimeout() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{activityInfo.ScheduleId: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.EqualValues([]timerSequenceID{
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.StartedTime.Add(*activityInfo.StartToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID:      activityInfo.ScheduleId,
			timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_Multiple() {
	now := time.Now().UTC()
	activityInfo1 := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}
	activityInfo2 := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              2345,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "other random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(11),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1001),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(101),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(6),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(800 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 21,
	}
	activityInfos := map[int64]*persistencespb.ActivityInfo{
		activityInfo1.ScheduleId: activityInfo1,
		activityInfo2.ScheduleId: activityInfo2,
	}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      activityInfo2.ScheduleId,
			timestamp:    activityInfo2.ScheduledTime.Add(*activityInfo2.ScheduleToStartTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			timerCreated: false,
			attempt:      activityInfo2.Attempt,
		},
		{
			eventID:      activityInfo1.ScheduleId,
			timestamp:    activityInfo1.StartedTime.Add(*activityInfo1.StartToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: false,
			attempt:      activityInfo1.Attempt,
		},
		{
			eventID:      activityInfo1.ScheduleId,
			timestamp:    activityInfo1.ScheduledTime.Add(*activityInfo1.ScheduleToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: false,
			attempt:      activityInfo1.Attempt,
		},
		{
			eventID:      activityInfo2.ScheduleId,
			timestamp:    activityInfo2.ScheduledTime.Add(*activityInfo2.ScheduleToCloseTimeout),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: false,
			attempt:      activityInfo2.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestGetUserTimerTimeout() {
	now := timestamp.TimeNowPtrUtc()
	timerExpiry := timestamp.TimePtr(now.Add(100))
	timerInfo := &persistencespb.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timerExpiry,
		TaskStatus: timerTaskStatusCreated,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      timerInfo.StartedId,
		timestamp:    *timerExpiry,
		timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		timerCreated: true,
		attempt:      1,
	}

	timerSequence := s.timerSequence.getUserTimerTimeout(timerInfo)
	s.Equal(expectedTimerSequence, timerSequence)

	timerInfo.TaskStatus = timerTaskStatusNone
	expectedTimerSequence.timerCreated = false
	timerSequence = s.timerSequence.getUserTimerTimeout(timerInfo)
	s.Equal(expectedTimerSequence, timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_WithTimeout_NotScheduled() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              common.EmptyEventID,
		ScheduledTime:           timestamp.TimePtr(time.Time{}),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_WithTimeout_Scheduled_NotStarted() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      activityInfo.ScheduleId,
		timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToStartTimeout),
		timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		timerCreated: true,
		attempt:      12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	expectedTimerSequence.timerCreated = false
	timerSequence = s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_WithTimeout_Scheduled_Started() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Second)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_WithoutTimeout_NotScheduled() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              common.EmptyEventID,
		ScheduledTime:           timestamp.TimePtr(time.Time{}),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(0),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_WithoutTimeout_Scheduled_NotStarted() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(0),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_WithoutTimeout_Scheduled_Started() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Second)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(0),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToStart,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToCloseTimeout_WithTimeout_NotScheduled() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              common.EmptyEventID,
		ScheduledTime:           timestamp.TimePtr(time.Time{}),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToCloseTimeout_WithTimeout_Scheduled() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose,
		Attempt:                 12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      activityInfo.ScheduleId,
		timestamp:    activityInfo.ScheduledTime.Add(*activityInfo.ScheduleToCloseTimeout),
		timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		timerCreated: true,
		attempt:      12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	expectedTimerSequence.timerCreated = false
	timerSequence = s.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToCloseTimeout_WithoutTimeout_NotScheduled() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              common.EmptyEventID,
		ScheduledTime:           timestamp.TimePtr(time.Time{}),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(0),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToCloseTimeout_WithoutTimeout_Scheduled() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(0),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedScheduleToClose,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityStartToCloseTimeout_WithTimeout_NotStarted() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityStartToCloseTimeout_WithTimeout_Started() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      activityInfo.ScheduleId,
		timestamp:    activityInfo.StartedTime.Add(*activityInfo.StartToCloseTimeout),
		timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		timerCreated: true,
		attempt:      12,
	}

	timerSequence := s.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	expectedTimerSequence.timerCreated = false
	timerSequence = s.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityStartToCloseTimeout_WithoutTimeout_NotStarted() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(0),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityStartToCloseTimeout_WithoutTimeout_Started() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(0),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedStartToClose,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithHeartbeat_NotStarted() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithHeartbeat_Started_NoHeartbeat() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      activityInfo.ScheduleId,
		timestamp:    activityInfo.StartedTime.Add(*activityInfo.HeartbeatTimeout),
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: true,
		attempt:      12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	expectedTimerSequence.timerCreated = false
	timerSequence = s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithHeartbeat_Started_Heartbeated() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(1),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      activityInfo.ScheduleId,
		timestamp:    activityInfo.LastHeartbeatUpdateTime.Add(*activityInfo.HeartbeatTimeout),
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: true,
		attempt:      12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	expectedTimerSequence.timerCreated = false
	timerSequence = s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Equal(expectedTimerSequence, timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithoutHeartbeat_NotStarted() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               common.EmptyEventID,
		StartedTime:             timestamp.TimePtr(time.Time{}),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusNone,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithoutHeartbeat_Started_NoHeartbeat() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(time.Time{}),
		TimerTaskStatus:         timerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithoutHeartbeat_Started_Heartbeated() {
	now := time.Now().UTC()
	activityInfo := &persistencespb.ActivityInfo{
		Version:                 123,
		ScheduleId:              234,
		ScheduledTime:           timestamp.TimePtr(now),
		StartedId:               345,
		StartedTime:             timestamp.TimePtr(now.Add(200 * time.Millisecond)),
		ActivityId:              "some random activity ID",
		ScheduleToStartTimeout:  timestamp.DurationFromSeconds(10),
		ScheduleToCloseTimeout:  timestamp.DurationFromSeconds(1000),
		StartToCloseTimeout:     timestamp.DurationFromSeconds(100),
		HeartbeatTimeout:        timestamp.DurationFromSeconds(0),
		LastHeartbeatUpdateTime: timestamp.TimePtr(now.Add(400 * time.Millisecond)),
		TimerTaskStatus:         timerTaskStatusCreatedHeartbeat,
		Attempt:                 12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestConversion() {
	s.Equal(int32(timerTaskStatusCreatedStartToClose), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_START_TO_CLOSE))
	s.Equal(int32(timerTaskStatusCreatedScheduleToStart), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START))
	s.Equal(int32(timerTaskStatusCreatedScheduleToClose), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE))
	s.Equal(int32(timerTaskStatusCreatedHeartbeat), timerTypeToTimerMask(enumspb.TIMEOUT_TYPE_HEARTBEAT))

	s.Equal(timerTaskStatusNone, 0)
	s.Equal(timerTaskStatusCreated, 1)
	s.Equal(timerTaskStatusCreatedStartToClose, 1)
	s.Equal(timerTaskStatusCreatedScheduleToStart, 2)
	s.Equal(timerTaskStatusCreatedScheduleToClose, 4)
	s.Equal(timerTaskStatusCreatedHeartbeat, 8)
}

func (s *timerSequenceSuite) TestLess_CompareTime() {
	now := time.Now().UTC()
	timerSequenceID1 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceID2 := timerSequenceID{
		eventID:      123,
		timestamp:    now.Add(time.Second),
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceIDs := timerSequenceIDs([]timerSequenceID{timerSequenceID1, timerSequenceID2})
	s.True(timerSequenceIDs.Less(0, 1))
	s.False(timerSequenceIDs.Less(1, 0))
}

func (s *timerSequenceSuite) TestLess_CompareEventID() {
	now := time.Now().UTC()
	timerSequenceID1 := timerSequenceID{
		eventID:      122,
		timestamp:    now,
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceID2 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceIDs := timerSequenceIDs([]timerSequenceID{timerSequenceID1, timerSequenceID2})
	s.True(timerSequenceIDs.Less(0, 1))
	s.False(timerSequenceIDs.Less(1, 0))
}

func (s *timerSequenceSuite) TestLess_CompareType() {
	now := time.Now().UTC()
	timerSequenceID1 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceID2 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceIDs := timerSequenceIDs([]timerSequenceID{timerSequenceID1, timerSequenceID2})
	s.True(timerSequenceIDs.Less(0, 1))
	s.False(timerSequenceIDs.Less(1, 0))
}
