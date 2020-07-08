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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination timerSequence_mock.go

package history

import (
	"fmt"
	"sort"
	"time"

	"github.com/gogo/protobuf/types"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence"
)

const (
	// activity / user timer task not created
	timerTaskStatusNone = iota
	timerTaskStatusCreated
)

const (
	// activity timer task status
	timerTaskStatusCreatedStartToClose = 1 << iota
	timerTaskStatusCreatedScheduleToStart
	timerTaskStatusCreatedScheduleToClose
	timerTaskStatusCreatedHeartbeat
)

type (
	// timerSequenceID
	timerSequenceID struct {
		eventID      int64
		timestamp    time.Time
		timerType    enumspb.TimeoutType
		timerCreated bool
		attempt      int32
	}

	timerSequenceIDs []timerSequenceID

	timerSequence interface {
		isExpired(referenceTime time.Time, timerSequenceID timerSequenceID) bool

		createNextUserTimer() (bool, error)
		createNextActivityTimer() (bool, error)

		loadAndSortUserTimers() []timerSequenceID
		loadAndSortActivityTimers() []timerSequenceID
	}

	timerSequenceImpl struct {
		timeSource   clock.TimeSource
		mutableState mutableState
	}
)

var _ timerSequence = (*timerSequenceImpl)(nil)

func newTimerSequence(
	timeSource clock.TimeSource,
	mutableState mutableState,
) *timerSequenceImpl {
	return &timerSequenceImpl{
		timeSource:   timeSource,
		mutableState: mutableState,
	}
}

func (t *timerSequenceImpl) isExpired(
	referenceTime time.Time,
	timerSequenceID timerSequenceID,
) bool {

	// Cassandra timestamp resolution is in millisecond
	// here we do the check in terms of second resolution.
	return timerSequenceID.timestamp.Unix() <= referenceTime.Unix()
}

func (t *timerSequenceImpl) createNextUserTimer() (bool, error) {

	sequenceIDs := t.loadAndSortUserTimers()
	if len(sequenceIDs) == 0 {
		return false, nil
	}

	firstTimerTask := sequenceIDs[0]

	// timer has already been created
	if firstTimerTask.timerCreated {
		return false, nil
	}

	timerInfo, ok := t.mutableState.GetUserTimerInfoByEventID(firstTimerTask.eventID)
	if !ok {
		return false, serviceerror.NewInternal(fmt.Sprintf("unable to load activity info %v", firstTimerTask.eventID))
	}
	// mark timer task mask as indication that timer task is generated
	// here TaskID is misleading attr, should be called timer created flag or something
	timerInfo.TaskStatus = timerTaskStatusCreated
	if err := t.mutableState.UpdateUserTimer(timerInfo); err != nil {
		return false, err
	}
	t.mutableState.AddTimerTasks(&persistence.UserTimerTask{
		// TaskID is set by shard
		VisibilityTimestamp: firstTimerTask.timestamp,
		EventID:             firstTimerTask.eventID,
		Version:             t.mutableState.GetCurrentVersion(),
	})
	return true, nil
}

func (t *timerSequenceImpl) createNextActivityTimer() (bool, error) {

	sequenceIDs := t.loadAndSortActivityTimers()
	if len(sequenceIDs) == 0 {
		return false, nil
	}

	firstTimerTask := sequenceIDs[0]

	// timer has already been created
	if firstTimerTask.timerCreated {
		return false, nil
	}

	activityInfo, ok := t.mutableState.GetActivityInfo(firstTimerTask.eventID)
	if !ok {
		return false, serviceerror.NewInternal(fmt.Sprintf("unable to load activity info %v", firstTimerTask.eventID))
	}
	// mark timer task mask as indication that timer task is generated
	activityInfo.TimerTaskStatus |= timerTypeToTimerMask(firstTimerTask.timerType)
	if firstTimerTask.timerType == enumspb.TIMEOUT_TYPE_HEARTBEAT {
		activityInfo.LastHeartbeatTimeoutVisibilityInSeconds = firstTimerTask.timestamp.Unix()
	}
	if err := t.mutableState.UpdateActivity(activityInfo); err != nil {
		return false, err
	}
	t.mutableState.AddTimerTasks(&persistence.ActivityTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: firstTimerTask.timestamp,
		TimeoutType:         firstTimerTask.timerType,
		EventID:             firstTimerTask.eventID,
		Attempt:             int64(firstTimerTask.attempt),
		Version:             t.mutableState.GetCurrentVersion(),
	})
	return true, nil
}

func (t *timerSequenceImpl) loadAndSortUserTimers() []timerSequenceID {

	pendingTimers := t.mutableState.GetPendingTimerInfos()
	timers := make(timerSequenceIDs, 0, len(pendingTimers))

	for _, timerInfo := range pendingTimers {

		if sequenceID := t.getUserTimerTimeout(
			timerInfo,
		); sequenceID != nil {
			timers = append(timers, *sequenceID)
		}
	}

	sort.Sort(timers)
	return timers
}

func (t *timerSequenceImpl) loadAndSortActivityTimers() []timerSequenceID {
	// there can be 4 timer per activity
	// see timerType
	pendingActivities := t.mutableState.GetPendingActivityInfos()
	activityTimers := make(timerSequenceIDs, 0, len(pendingActivities)*4)

	for _, activityInfo := range pendingActivities {

		if sequenceID := t.getActivityScheduleToCloseTimeout(
			activityInfo,
		); sequenceID != nil {
			activityTimers = append(activityTimers, *sequenceID)
		}

		if sequenceID := t.getActivityScheduleToStartTimeout(
			activityInfo,
		); sequenceID != nil {
			activityTimers = append(activityTimers, *sequenceID)
		}

		if sequenceID := t.getActivityStartToCloseTimeout(
			activityInfo,
		); sequenceID != nil {
			activityTimers = append(activityTimers, *sequenceID)
		}

		if sequenceID := t.getActivityHeartbeatTimeout(
			activityInfo,
		); sequenceID != nil {
			activityTimers = append(activityTimers, *sequenceID)
		}
	}

	sort.Sort(activityTimers)
	return activityTimers
}

func (t *timerSequenceImpl) getUserTimerTimeout(
	timerInfo *persistenceblobs.TimerInfo,
) *timerSequenceID {

	expiryTime, _ := types.TimestampFromProto(timerInfo.ExpiryTime)

	return &timerSequenceID{
		eventID:      timerInfo.GetStartedId(),
		timestamp:    expiryTime,
		timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		timerCreated: timerInfo.TaskStatus == timerTaskStatusCreated,
		attempt:      0,
	}
}

func (t *timerSequenceImpl) getActivityScheduleToStartTimeout(
	activityInfo *persistence.ActivityInfo,
) *timerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduleID == common.EmptyEventID {
		return nil
	}

	// activity is already started
	if activityInfo.StartedID != common.EmptyEventID {
		return nil
	}

	startTimeout := activityInfo.ScheduledTime.Add(
		time.Duration(activityInfo.ScheduleToStartTimeout) * time.Second,
	)

	return &timerSequenceID{
		eventID:      activityInfo.ScheduleID,
		timestamp:    startTimeout,
		timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		timerCreated: (activityInfo.TimerTaskStatus & timerTaskStatusCreatedScheduleToStart) > 0,
		attempt:      activityInfo.Attempt,
	}
}

func (t *timerSequenceImpl) getActivityScheduleToCloseTimeout(
	activityInfo *persistence.ActivityInfo,
) *timerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduleID == common.EmptyEventID {
		return nil
	}

	closeTimeout := activityInfo.ScheduledTime.Add(
		time.Duration(activityInfo.ScheduleToCloseTimeout) * time.Second,
	)

	return &timerSequenceID{
		eventID:      activityInfo.ScheduleID,
		timestamp:    closeTimeout,
		timerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		timerCreated: (activityInfo.TimerTaskStatus & timerTaskStatusCreatedScheduleToClose) > 0,
		attempt:      activityInfo.Attempt,
	}
}

func (t *timerSequenceImpl) getActivityStartToCloseTimeout(
	activityInfo *persistence.ActivityInfo,
) *timerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduleID == common.EmptyEventID {
		return nil
	}

	// activity is not started yet
	if activityInfo.StartedID == common.EmptyEventID {
		return nil
	}

	closeTimeout := activityInfo.StartedTime.Add(
		time.Duration(activityInfo.StartToCloseTimeout) * time.Second,
	)

	return &timerSequenceID{
		eventID:      activityInfo.ScheduleID,
		timestamp:    closeTimeout,
		timerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		timerCreated: (activityInfo.TimerTaskStatus & timerTaskStatusCreatedStartToClose) > 0,
		attempt:      activityInfo.Attempt,
	}
}

func (t *timerSequenceImpl) getActivityHeartbeatTimeout(
	activityInfo *persistence.ActivityInfo,
) *timerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduleID == common.EmptyEventID {
		return nil
	}

	// activity is not started yet
	if activityInfo.StartedID == common.EmptyEventID {
		return nil
	}

	// not heartbeat timeout configured
	if activityInfo.HeartbeatTimeout <= 0 {
		return nil
	}

	// use the latest time as last heartbeat time
	lastHeartbeat := activityInfo.StartedTime
	if activityInfo.LastHeartBeatUpdatedTime.After(lastHeartbeat) {
		lastHeartbeat = activityInfo.LastHeartBeatUpdatedTime
	}

	heartbeatTimeout := lastHeartbeat.Add(
		time.Duration(activityInfo.HeartbeatTimeout) * time.Second,
	)

	return &timerSequenceID{
		eventID:      activityInfo.ScheduleID,
		timestamp:    heartbeatTimeout,
		timerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		timerCreated: (activityInfo.TimerTaskStatus & timerTaskStatusCreatedHeartbeat) > 0,
		attempt:      activityInfo.Attempt,
	}
}

func timerTypeToTimerMask(
	timerType enumspb.TimeoutType,
) int32 {

	switch timerType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		return timerTaskStatusCreatedStartToClose
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		return timerTaskStatusCreatedScheduleToStart
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		return timerTaskStatusCreatedScheduleToClose
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		return timerTaskStatusCreatedHeartbeat
	default:
		panic("invalid timeout type")
	}
}

// Len implements sort.Interface
func (s timerSequenceIDs) Len() int {
	return len(s)
}

// Swap implements sort.Interface.
func (s timerSequenceIDs) Swap(
	this int,
	that int,
) {
	s[this], s[that] = s[that], s[this]
}

// Less implements sort.Interface
func (s timerSequenceIDs) Less(
	this int,
	that int,
) bool {

	thisSequenceID := s[this]
	thatSequenceID := s[that]

	// order: timeout time, event ID, timeout type

	if thisSequenceID.timestamp.Before(thatSequenceID.timestamp) {
		return true
	} else if thisSequenceID.timestamp.After(thatSequenceID.timestamp) {
		return false
	}

	// timeout time are the same
	if thisSequenceID.eventID < thatSequenceID.eventID {
		return true
	} else if thisSequenceID.eventID > thatSequenceID.eventID {
		return false
	}

	// timeout time & event ID are the same
	if thisSequenceID.timerType < thatSequenceID.timerType {
		return true
	} else if thisSequenceID.timerType > thatSequenceID.timerType {
		return false
	}

	// thisSequenceID && thatSequenceID are the same
	return true
}
