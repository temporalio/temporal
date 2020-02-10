// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/persistence"
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

func (s *timerSequenceSuite) TestCreateNextUserTimer_AlreadyCreated() {
	now := time.Now()
	timerInfo := &persistence.TimerInfo{
		Version:    123,
		TimerID:    "some random timer ID",
		StartedID:  456,
		ExpiryTime: now.Add(100 * time.Second),
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistence.TimerInfo{timerInfo.TimerID: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_NotCreated() {
	now := time.Now()
	currentVersion := int64(999)
	timerInfo := &persistence.TimerInfo{
		Version:    123,
		TimerID:    "some random timer ID",
		StartedID:  456,
		ExpiryTime: now.Add(100 * time.Second),
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistence.TimerInfo{timerInfo.TimerID: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)
	s.mockMutableState.EXPECT().GetUserTimerInfoByEventID(timerInfo.StartedID).Return(timerInfo, true).Times(1)

	var timerInfoUpdated = *timerInfo // make a copy
	timerInfoUpdated.TaskStatus = timerTaskStatusCreated
	s.mockMutableState.EXPECT().UpdateUserTimer(&timerInfoUpdated).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion).Times(1)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.UserTimerTask{
		// TaskID is set by shard
		VisibilityTimestamp: timerInfo.ExpiryTime,
		EventID:             timerInfo.StartedID,
		Version:             currentVersion,
	}).Times(1)

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_AlreadyCreated() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedScheduleToStart,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.False(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_NotCreated() {
	now := time.Now()
	currentVersion := int64(999)
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)
	s.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduleID).Return(activityInfo, true).Times(1)

	var activityInfoUpdated = *activityInfo // make a copy
	activityInfoUpdated.TimerTaskStatus = timerTaskStatusCreatedScheduleToStart
	s.mockMutableState.EXPECT().UpdateActivity(&activityInfoUpdated).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion).Times(1)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: activityInfo.ScheduledTime.Add(
			time.Duration(activityInfo.ScheduleToStartTimeout) * time.Second,
		),
		TimeoutType: int(shared.TimeoutTypeScheduleToStart),
		EventID:     activityInfo.ScheduleID,
		Attempt:     int64(activityInfo.Attempt),
		Version:     currentVersion,
	}).Times(1)

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestCreateNextActivityTimer_HeartbeatTimer() {
	now := time.Now()
	currentVersion := int64(999)
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)
	s.mockMutableState.EXPECT().GetActivityInfo(activityInfo.ScheduleID).Return(activityInfo, true).Times(1)

	taskVisibilityTimestamp := activityInfo.StartedTime.Add(
		time.Duration(activityInfo.HeartbeatTimeout) * time.Second,
	)

	var activityInfoUpdated = *activityInfo // make a copy
	activityInfoUpdated.TimerTaskStatus = timerTaskStatusCreatedHeartbeat
	activityInfoUpdated.LastHeartbeatTimeoutVisibility = taskVisibilityTimestamp.Unix()
	s.mockMutableState.EXPECT().UpdateActivity(&activityInfoUpdated).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion).Times(1)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: taskVisibilityTimestamp,
		TimeoutType:         int(shared.TimeoutTypeHeartbeat),
		EventID:             activityInfo.ScheduleID,
		Attempt:             int64(activityInfo.Attempt),
		Version:             currentVersion,
	}).Times(1)

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_None() {
	timerInfos := map[string]*persistence.TimerInfo{}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Empty(timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_One() {
	now := time.Now()
	timerInfo := &persistence.TimerInfo{
		Version:    123,
		TimerID:    "some random timer ID",
		StartedID:  456,
		ExpiryTime: now.Add(100 * time.Second),
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistence.TimerInfo{timerInfo.TimerID: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Equal([]timerSequenceID{{
		eventID:      timerInfo.StartedID,
		timestamp:    timerInfo.ExpiryTime,
		timerType:    timerTypeStartToClose,
		timerCreated: true,
		attempt:      0,
	}}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_Multiple() {
	now := time.Now()
	timerInfo1 := &persistence.TimerInfo{
		Version:    123,
		TimerID:    "some random timer ID",
		StartedID:  456,
		ExpiryTime: now.Add(100 * time.Second),
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfo2 := &persistence.TimerInfo{
		Version:    1234,
		TimerID:    "other random timer ID",
		StartedID:  4567,
		ExpiryTime: now.Add(200 * time.Second),
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistence.TimerInfo{
		timerInfo1.TimerID: timerInfo1,
		timerInfo2.TimerID: timerInfo2,
	}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      timerInfo1.StartedID,
			timestamp:    timerInfo1.ExpiryTime,
			timerType:    timerTypeStartToClose,
			timerCreated: true,
			attempt:      0,
		},
		{
			eventID:      timerInfo2.StartedID,
			timestamp:    timerInfo2.ExpiryTime,
			timerType:    timerTypeStartToClose,
			timerCreated: false,
			attempt:      0,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_None() {
	activityInfos := map[int64]*persistence.ActivityInfo{}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Empty(timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_NotScheduled() {
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               common.EmptyEventID,
		ScheduledTime:            time.Time{},
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusNone,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Empty(timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_NotStarted() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedScheduleToStart,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToStartTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToStart,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_WithHeartbeatTimeout() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose | timerTaskStatusCreatedHeartbeat,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.StartedTime.Add(
				time.Duration(activityInfo.HeartbeatTimeout) * time.Second,
			),
			timerType:    timerTypeHeartbeat,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.StartedTime.Add(
				time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeStartToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_WithoutHeartbeatTimeout() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.StartedTime.Add(
				time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeStartToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_Heartbeated_WithHeartbeatTimeout() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose | timerTaskStatusCreatedHeartbeat,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.LastHeartBeatUpdatedTime.Add(
				time.Duration(activityInfo.HeartbeatTimeout) * time.Second,
			),
			timerType:    timerTypeHeartbeat,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.StartedTime.Add(
				time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeStartToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_One_Scheduled_Started_Heartbeated_WithoutHeartbeatTimeout() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToClose | timerTaskStatusCreatedStartToClose,
		Attempt:                  12,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{activityInfo.ScheduleID: activityInfo}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.StartedTime.Add(
				time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeStartToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToClose,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortActivityTimers_Multiple() {
	now := time.Now()
	activityInfo1 := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}
	activityInfo2 := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               2345,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "other random activity ID",
		ScheduleToStartTimeout:   11,
		ScheduleToCloseTimeout:   1001,
		StartToCloseTimeout:      101,
		HeartbeatTimeout:         6,
		LastHeartBeatUpdatedTime: now.Add(800 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  21,
	}
	activityInfos := map[int64]*persistence.ActivityInfo{
		activityInfo1.ScheduleID: activityInfo1,
		activityInfo2.ScheduleID: activityInfo2,
	}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortActivityTimers()
	s.Equal([]timerSequenceID{
		{
			eventID: activityInfo2.ScheduleID,
			timestamp: activityInfo2.ScheduledTime.Add(
				time.Duration(activityInfo2.ScheduleToStartTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToStart,
			timerCreated: false,
			attempt:      activityInfo2.Attempt,
		},
		{
			eventID: activityInfo1.ScheduleID,
			timestamp: activityInfo1.StartedTime.Add(
				time.Duration(activityInfo1.StartToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeStartToClose,
			timerCreated: false,
			attempt:      activityInfo1.Attempt,
		},
		{
			eventID: activityInfo1.ScheduleID,
			timestamp: activityInfo1.ScheduledTime.Add(
				time.Duration(activityInfo1.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToClose,
			timerCreated: false,
			attempt:      activityInfo1.Attempt,
		},
		{
			eventID: activityInfo2.ScheduleID,
			timestamp: activityInfo2.ScheduledTime.Add(
				time.Duration(activityInfo2.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    timerTypeScheduleToClose,
			timerCreated: false,
			attempt:      activityInfo2.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestGetUserTimerTimeout() {
	now := time.Now()
	timerInfo := &persistence.TimerInfo{
		Version:    123,
		TimerID:    "some random timer ID",
		StartedID:  456,
		ExpiryTime: now.Add(100 * time.Second),
		TaskStatus: timerTaskStatusCreated,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      timerInfo.StartedID,
		timestamp:    timerInfo.ExpiryTime,
		timerType:    timerTypeStartToClose,
		timerCreated: true,
		attempt:      0,
	}

	timerSequence := s.timerSequence.getUserTimerTimeout(timerInfo)
	s.Equal(expectedTimerSequence, timerSequence)

	timerInfo.TaskStatus = timerTaskStatusNone
	expectedTimerSequence.timerCreated = false
	timerSequence = s.timerSequence.getUserTimerTimeout(timerInfo)
	s.Equal(expectedTimerSequence, timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_NotScheduled() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               common.EmptyEventID,
		ScheduledTime:            time.Time{},
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_Scheduled_NotStarted() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToStart,
		Attempt:                  12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID: activityInfo.ScheduleID,
		timestamp: activityInfo.ScheduledTime.Add(
			time.Duration(activityInfo.ScheduleToStartTimeout) * time.Second,
		),
		timerType:    timerTypeScheduleToStart,
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

func (s *timerSequenceSuite) TestGetActivityScheduleToStartTimeout_Scheduled_Started() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Second),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToStart,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityScheduleToStartTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToCloseTimeout_NotScheduled() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               common.EmptyEventID,
		ScheduledTime:            time.Time{},
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityScheduleToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityScheduleToCloseTimeout_Scheduled() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedScheduleToClose,
		Attempt:                  12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID: activityInfo.ScheduleID,
		timestamp: activityInfo.ScheduledTime.Add(
			time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
		),
		timerType:    timerTypeScheduleToClose,
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

func (s *timerSequenceSuite) TestGetActivityStartToCloseTimeout_NotStarted() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityStartToCloseTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityStartToCloseTimeout_Started() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedStartToClose,
		Attempt:                  12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID: activityInfo.ScheduleID,
		timestamp: activityInfo.StartedTime.Add(
			time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
		),
		timerType:    timerTypeStartToClose,
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

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithHeartbeat_NotStarted() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithHeartbeat_Started_NoHeartbeat() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusCreatedHeartbeat,
		Attempt:                  12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID: activityInfo.ScheduleID,
		timestamp: activityInfo.StartedTime.Add(
			time.Duration(activityInfo.HeartbeatTimeout) * time.Second,
		),
		timerType:    timerTypeHeartbeat,
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
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         1,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedHeartbeat,
		Attempt:                  12,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID: activityInfo.ScheduleID,
		timestamp: activityInfo.LastHeartBeatUpdatedTime.Add(
			time.Duration(activityInfo.HeartbeatTimeout) * time.Second,
		),
		timerType:    timerTypeHeartbeat,
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
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                common.EmptyEventID,
		StartedTime:              time.Time{},
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusNone,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithoutHeartbeat_Started_NoHeartbeat() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: time.Time{},
		TimerTaskStatus:          timerTaskStatusCreatedHeartbeat,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestGetActivityHeartbeatTimeout_WithoutHeartbeat_Started_Heartbeated() {
	now := time.Now()
	activityInfo := &persistence.ActivityInfo{
		Version:                  123,
		ScheduleID:               234,
		ScheduledTime:            now,
		StartedID:                345,
		StartedTime:              now.Add(200 * time.Millisecond),
		ActivityID:               "some random activity ID",
		ScheduleToStartTimeout:   10,
		ScheduleToCloseTimeout:   1000,
		StartToCloseTimeout:      100,
		HeartbeatTimeout:         0,
		LastHeartBeatUpdatedTime: now.Add(400 * time.Millisecond),
		TimerTaskStatus:          timerTaskStatusCreatedHeartbeat,
		Attempt:                  12,
	}

	timerSequence := s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)

	activityInfo.TimerTaskStatus = timerTaskStatusNone
	timerSequence = s.timerSequence.getActivityHeartbeatTimeout(activityInfo)
	s.Empty(timerSequence)
}

func (s *timerSequenceSuite) TestConversion() {
	s.Equal(shared.TimeoutTypeStartToClose, timerTypeToThrift(timerTypeStartToClose))
	s.Equal(shared.TimeoutTypeScheduleToStart, timerTypeToThrift(timerTypeScheduleToStart))
	s.Equal(shared.TimeoutTypeScheduleToClose, timerTypeToThrift(timerTypeScheduleToClose))
	s.Equal(shared.TimeoutTypeHeartbeat, timerTypeToThrift(timerTypeHeartbeat))

	s.Equal(timerTypeFromThrift(shared.TimeoutTypeStartToClose), timerTypeStartToClose)
	s.Equal(timerTypeFromThrift(shared.TimeoutTypeScheduleToStart), timerTypeScheduleToStart)
	s.Equal(timerTypeFromThrift(shared.TimeoutTypeScheduleToClose), timerTypeScheduleToClose)
	s.Equal(timerTypeFromThrift(shared.TimeoutTypeHeartbeat), timerTypeHeartbeat)

	s.Equal(int32(timerTaskStatusCreatedStartToClose), timerTypeToTimerMask(timerTypeStartToClose))
	s.Equal(int32(timerTaskStatusCreatedScheduleToStart), timerTypeToTimerMask(timerTypeScheduleToStart))
	s.Equal(int32(timerTaskStatusCreatedScheduleToClose), timerTypeToTimerMask(timerTypeScheduleToClose))
	s.Equal(int32(timerTaskStatusCreatedHeartbeat), timerTypeToTimerMask(timerTypeHeartbeat))

	s.Equal(timerTaskStatusNone, 0)
	s.Equal(timerTaskStatusCreated, 1)
	s.Equal(timerTaskStatusCreatedStartToClose, 1)
	s.Equal(timerTaskStatusCreatedScheduleToStart, 2)
	s.Equal(timerTaskStatusCreatedScheduleToClose, 4)
	s.Equal(timerTaskStatusCreatedHeartbeat, 8)
}

func (s *timerSequenceSuite) TestLess_CompareTime() {
	now := time.Now()
	timerSequenceID1 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    timerTypeHeartbeat,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceID2 := timerSequenceID{
		eventID:      123,
		timestamp:    now.Add(time.Second),
		timerType:    timerTypeHeartbeat,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceIDs := timerSequenceIDs([]timerSequenceID{timerSequenceID1, timerSequenceID2})
	s.True(timerSequenceIDs.Less(0, 1))
	s.False(timerSequenceIDs.Less(1, 0))
}

func (s *timerSequenceSuite) TestLess_CompareEventID() {
	now := time.Now()
	timerSequenceID1 := timerSequenceID{
		eventID:      122,
		timestamp:    now,
		timerType:    timerTypeHeartbeat,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceID2 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    timerTypeHeartbeat,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceIDs := timerSequenceIDs([]timerSequenceID{timerSequenceID1, timerSequenceID2})
	s.True(timerSequenceIDs.Less(0, 1))
	s.False(timerSequenceIDs.Less(1, 0))
}

func (s *timerSequenceSuite) TestLess_CompareType() {
	now := time.Now()
	timerSequenceID1 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    timerTypeScheduleToClose,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceID2 := timerSequenceID{
		eventID:      123,
		timestamp:    now,
		timerType:    timerTypeHeartbeat,
		timerCreated: true,
		attempt:      12,
	}

	timerSequenceIDs := timerSequenceIDs([]timerSequenceID{timerSequenceID1, timerSequenceID2})
	s.True(timerSequenceIDs.Less(0, 1))
	s.False(timerSequenceIDs.Less(1, 0))
}
