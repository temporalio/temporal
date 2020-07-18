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

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence"
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
	now := types.TimestampNow()
	timer1Expiry := addDurationToGogoTime(now, 100)
	timerInfo := &persistenceblobs.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timer1Expiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistenceblobs.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	modified, err := s.timerSequence.createNextUserTimer()
	s.NoError(err)
	s.False(modified)
}

func addDurationToGogoTime(timestamp *types.Timestamp, seconds int) *types.Timestamp {
	return &types.Timestamp{Seconds: timestamp.Seconds + int64(seconds), Nanos: timestamp.Nanos}
}

func (s *timerSequenceSuite) TestCreateNextUserTimer_NotCreated() {
	now := types.TimestampNow()
	now = addDurationToGogoTime(now, 100)
	currentVersion := int64(999)
	timerInfo := &persistenceblobs.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: now,
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistenceblobs.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)
	s.mockMutableState.EXPECT().GetUserTimerInfoByEventID(timerInfo.StartedId).Return(timerInfo, true).Times(1)

	var timerInfoUpdated = *timerInfo // make a copy
	timerInfoUpdated.TaskStatus = timerTaskStatusCreated
	s.mockMutableState.EXPECT().UpdateUserTimer(&timerInfoUpdated).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion).Times(1)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.UserTimerTask{
		// TaskID is set by shard
		VisibilityTimestamp: time.Unix(now.Seconds, int64(now.Nanos)).UTC(),
		EventID:             timerInfo.GetStartedId(),
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
		TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
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
	activityInfoUpdated.LastHeartbeatTimeoutVisibilityInSeconds = taskVisibilityTimestamp.Unix()
	s.mockMutableState.EXPECT().UpdateActivity(&activityInfoUpdated).Return(nil).Times(1)
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(currentVersion).Times(1)
	s.mockMutableState.EXPECT().AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: taskVisibilityTimestamp,
		TimeoutType:         enumspb.TIMEOUT_TYPE_HEARTBEAT,
		EventID:             activityInfo.ScheduleID,
		Attempt:             int64(activityInfo.Attempt),
		Version:             currentVersion,
	}).Times(1)

	modified, err := s.timerSequence.createNextActivityTimer()
	s.NoError(err)
	s.True(modified)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_None() {
	timerInfos := map[string]*persistenceblobs.TimerInfo{}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Empty(timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_One() {
	now := types.TimestampNow()
	timer1Expiry := addDurationToGogoTime(now, 100)
	timerInfo := &persistenceblobs.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timer1Expiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfos := map[string]*persistenceblobs.TimerInfo{timerInfo.TimerId: timerInfo}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Equal([]timerSequenceID{{
		eventID:      timerInfo.GetStartedId(),
		timestamp:    time.Unix(timer1Expiry.Seconds, int64(timer1Expiry.Nanos)).UTC(),
		timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		timerCreated: true,
		attempt:      1,
	}}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestLoadAndSortUserTimers_Multiple() {
	now := types.TimestampNow()
	timer1Expiry := addDurationToGogoTime(now, 100)
	timer2Expiry := addDurationToGogoTime(now, 200)
	timerInfo1 := &persistenceblobs.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timer1Expiry,
		TaskStatus: timerTaskStatusCreated,
	}
	timerInfo2 := &persistenceblobs.TimerInfo{
		Version:    1234,
		TimerId:    "other random timer ID",
		StartedId:  4567,
		ExpiryTime: addDurationToGogoTime(now, 200),
		TaskStatus: timerTaskStatusNone,
	}
	timerInfos := map[string]*persistenceblobs.TimerInfo{
		timerInfo1.TimerId: timerInfo1,
		timerInfo2.TimerId: timerInfo2,
	}
	s.mockMutableState.EXPECT().GetPendingTimerInfos().Return(timerInfos).Times(1)

	timerSequenceIDs := s.timerSequence.loadAndSortUserTimers()
	s.Equal([]timerSequenceID{
		{
			eventID:      timerInfo1.GetStartedId(),
			timestamp:    time.Unix(timer1Expiry.Seconds, int64(timer1Expiry.Nanos)).UTC(),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      1,
		},
		{
			eventID:      timerInfo2.GetStartedId(),
			timestamp:    time.Unix(timer2Expiry.Seconds, int64(timer2Expiry.Nanos)).UTC(),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: false,
			attempt:      1,
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
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
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
			timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.StartedTime.Add(
				time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
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
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
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
			timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.StartedTime.Add(
				time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
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
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: true,
			attempt:      activityInfo.Attempt,
		},
		{
			eventID: activityInfo.ScheduleID,
			timestamp: activityInfo.ScheduledTime.Add(
				time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
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
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			timerCreated: false,
			attempt:      activityInfo2.Attempt,
		},
		{
			eventID: activityInfo1.ScheduleID,
			timestamp: activityInfo1.StartedTime.Add(
				time.Duration(activityInfo1.StartToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			timerCreated: false,
			attempt:      activityInfo1.Attempt,
		},
		{
			eventID: activityInfo1.ScheduleID,
			timestamp: activityInfo1.ScheduledTime.Add(
				time.Duration(activityInfo1.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: false,
			attempt:      activityInfo1.Attempt,
		},
		{
			eventID: activityInfo2.ScheduleID,
			timestamp: activityInfo2.ScheduledTime.Add(
				time.Duration(activityInfo2.ScheduleToCloseTimeout) * time.Second,
			),
			timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			timerCreated: false,
			attempt:      activityInfo2.Attempt,
		},
	}, timerSequenceIDs)
}

func (s *timerSequenceSuite) TestGetUserTimerTimeout() {
	now := types.TimestampNow()
	timer1Expiry := addDurationToGogoTime(now, 100)
	timerInfo := &persistenceblobs.TimerInfo{
		Version:    123,
		TimerId:    "some random timer ID",
		StartedId:  456,
		ExpiryTime: timer1Expiry,
		TaskStatus: timerTaskStatusCreated,
	}

	expectedTimerSequence := &timerSequenceID{
		eventID:      timerInfo.StartedId,
		timestamp:    time.Unix(timer1Expiry.Seconds, int64(timer1Expiry.Nanos)).UTC(),
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
	now := time.Now()
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
	now := time.Now()
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
	now := time.Now()
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
