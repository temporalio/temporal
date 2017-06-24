// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	w "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

// Timer constants
const (
	DefaultScheduleToStartActivityTimeoutInSecs = 10
	DefaultScheduleToCloseActivityTimeoutInSecs = 10
	DefaultStartToCloseActivityTimeoutInSecs    = 10

	emptyTimerID = -1
)

type (
	timerDetails struct {
		SequenceID  SequenceID
		TimerTask   persistence.Task
		TaskCreated bool
	}

	timers []*timerDetails

	timerBuilder struct {
		timers            timers
		pendingUserTimers map[SequenceID]*persistence.TimerInfo
		logger            bark.Logger
		localSeqNumGen    SequenceNumberGenerator // This one used to order in-memory list.
		timeSource        common.TimeSource
	}

	// SequenceID - Visibility timer stamp + Sequence Number.
	SequenceID struct {
		VisibilityTimestamp time.Time
		TaskID              int64
	}

	// SequenceNumberGenerator - Generates next sequence number.
	SequenceNumberGenerator interface {
		NextSeq() int64
	}

	localSeqNumGenerator struct {
		counter int64
	}
)

func (s SequenceID) String() string {
	return fmt.Sprintf("timestamp: %v, seq: %v", s.VisibilityTimestamp.UTC(), s.TaskID)
}

// Len implements sort.Interace
func (t timers) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
// Swap implements sort.Interface.
func (t timers) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t timers) Less(i, j int) bool {
	return compareTimerIDLess(&t[i].SequenceID, &t[j].SequenceID)
}

func (td *timerDetails) String() string {
	return fmt.Sprintf("timerDetails: %s", td.SequenceID)
}

func (l *localSeqNumGenerator) NextSeq() int64 {
	return atomic.AddInt64(&l.counter, 1)
}

// newTimerBuilder creates a timer builder.
func newTimerBuilder(logger bark.Logger, timeSource common.TimeSource) *timerBuilder {
	return &timerBuilder{
		timers:            timers{},
		pendingUserTimers: make(map[SequenceID]*persistence.TimerInfo),
		logger:            logger.WithField(logging.TagWorkflowComponent, "timer"),
		localSeqNumGen:    &localSeqNumGenerator{counter: 1},
		timeSource:        timeSource,
	}
}

// AllTimers - Get all timers.
func (tb *timerBuilder) AllTimers() timers {
	return tb.timers
}

// UserTimer - Get a specific timer info.
func (tb *timerBuilder) UserTimer(taskID SequenceID) (bool, *persistence.TimerInfo) {
	ti, ok := tb.pendingUserTimers[taskID]
	return ok, ti
}

// AddDecisionTimeoutTask - Add a decision timeout task.
func (tb *timerBuilder) AddDecisionTimoutTask(scheduleID int64,
	startToCloseTimeout int32) *persistence.DecisionTimeoutTask {
	timeOutTask := tb.createDecisionTimeoutTask(startToCloseTimeout, scheduleID)
	tb.logger.Debugf("Adding Decision Timeout: with timeout: %v sec, EventID: %v",
		startToCloseTimeout, timeOutTask.EventID)
	return timeOutTask
}

func (tb *timerBuilder) AddScheduleToStartActivityTimeout(
	ai *persistence.ActivityInfo) *persistence.ActivityTimeoutTask {
	return tb.AddActivityTimeoutTask(ai.ScheduleID, w.TimeoutType_SCHEDULE_TO_START, ai.ScheduleToStartTimeout, nil)
}

func (tb *timerBuilder) AddScheduleToCloseActivityTimeout(
	ai *persistence.ActivityInfo) (*persistence.ActivityTimeoutTask, error) {
	return tb.AddActivityTimeoutTask(ai.ScheduleID, w.TimeoutType_SCHEDULE_TO_CLOSE, ai.ScheduleToCloseTimeout, nil), nil
}

func (tb *timerBuilder) AddStartToCloseActivityTimeout(ai *persistence.ActivityInfo) (*persistence.ActivityTimeoutTask,
	error) {
	return tb.AddActivityTimeoutTask(ai.ScheduleID, w.TimeoutType_START_TO_CLOSE, ai.StartToCloseTimeout, nil), nil
}

func (tb *timerBuilder) AddHeartBeatActivityTimeout(ai *persistence.ActivityInfo) (*persistence.ActivityTimeoutTask,
	error) {
	// We want to create the timer starting from the last heart beat time stamp but
	// avoid creating timers before the current timer frame.
	targetTime := common.AddSecondsToBaseTime(ai.LastHeartBeatUpdatedTime.UnixNano(), int64(ai.HeartbeatTimeout))
	if targetTime > tb.timeSource.Now().UnixNano() {
		return tb.AddActivityTimeoutTask(ai.ScheduleID, w.TimeoutType_HEARTBEAT, ai.HeartbeatTimeout, &ai.LastHeartBeatUpdatedTime), nil
	}
	return tb.AddActivityTimeoutTask(ai.ScheduleID, w.TimeoutType_HEARTBEAT, ai.HeartbeatTimeout, nil), nil
}

// AddActivityTimeoutTask - Adds an activity timeout task.
func (tb *timerBuilder) AddActivityTimeoutTask(scheduleID int64,
	timeoutType w.TimeoutType, fireTimeout int32, baseTime *time.Time) *persistence.ActivityTimeoutTask {
	if fireTimeout <= 0 {
		return nil
	}

	timeOutTask := tb.createActivityTimeoutTask(fireTimeout, timeoutType, scheduleID, baseTime)
	tb.logger.Debugf("%s: Adding Activity Timeout: with timeout: %v sec, TimeoutType: %v, EventID: %v",
		time.Now(), fireTimeout, timeoutType.String(), timeOutTask.EventID)
	return timeOutTask
}

// AddUserTimer - Adds an user timeout request.
func (tb *timerBuilder) AddUserTimer(ti *persistence.TimerInfo, msBuilder *mutableStateBuilder) persistence.Task {
	tb.logger.Debugf("Adding User Timeout for timer ID: %s", ti.TimerID)

	// TODO: This is broken.  We need to comeup with a better way to implement this
	tb.LoadUserTimers(msBuilder)
	timerTask := tb.firstTimer()
	if timerTask != nil {
		// Update the task ID tracking the corresponding timer task.
		ti := tb.pendingUserTimers[tb.timers[0].SequenceID]
		ti.TaskID = timerTask.GetTaskID()
		// TODO: We append updates to timer tasks twice.  Why?
		msBuilder.UpdateUserTimer(ti.TimerID, ti)
	}

	return timerTask
}

// LoadUserTimers - Load all user timers from mutable state.
func (tb *timerBuilder) LoadUserTimers(msBuilder *mutableStateBuilder) {
	tb.timers = timers{}
	tb.pendingUserTimers = make(map[SequenceID]*persistence.TimerInfo)
	for _, v := range msBuilder.pendingTimerInfoIDs {
		td, _ := tb.loadUserTimer(v.ExpiryTime.UnixNano(),
			&persistence.UserTimerTask{EventID: v.StartedID},
			v.TaskID != emptyTimerID)
		tb.pendingUserTimers[td.SequenceID] = v
	}
}

// IsTimerExpired - Whether a timer is expired w.r.t reference time.
func (tb *timerBuilder) IsTimerExpired(td *timerDetails, referenceTime time.Time) bool {
	// Cql timestamp is in milli sec resolution, here we do the check in terms of second resolution.
	expiry := td.SequenceID.VisibilityTimestamp.Unix()
	return expiry <= referenceTime.Unix()
}

func (tb *timerBuilder) createDeleteHistoryEventTimerTask(d time.Duration) *persistence.DeleteHistoryEventTask {
	expiryTime := tb.timeSource.Now().Add(d)
	return &persistence.DeleteHistoryEventTask{
		VisibilityTimestamp: expiryTime,
	}
}

// createDecisionTimeoutTask - Creates a decision timeout task.
func (tb *timerBuilder) createDecisionTimeoutTask(fireTimeOut int32, eventID int64) *persistence.DecisionTimeoutTask {
	expiryTime := tb.timeSource.Now().Add(time.Duration(fireTimeOut) * time.Second)
	return &persistence.DecisionTimeoutTask{
		VisibilityTimestamp: expiryTime,
		EventID:             eventID,
	}
}

// createActivityTimeoutTask - Creates a activity timeout task.
func (tb *timerBuilder) createActivityTimeoutTask(fireTimeOut int32, timeoutType w.TimeoutType,
	eventID int64, baseTime *time.Time) *persistence.ActivityTimeoutTask {
	var expiryTime time.Time
	if baseTime != nil {
		expiryTime = baseTime.Add(time.Duration(fireTimeOut) * time.Second)
	} else {
		expiryTime = tb.timeSource.Now().Add(time.Duration(fireTimeOut) * time.Second)
	}

	return &persistence.ActivityTimeoutTask{
		VisibilityTimestamp: expiryTime,
		TimeoutType:         int(timeoutType),
		EventID:             eventID,
	}
}

// createUserTimerTask - Creates a user timer task.
func (tb *timerBuilder) createUserTimerTask(expiryTime time.Time, startedEventID int64) *persistence.UserTimerTask {
	t := &persistence.UserTimerTask{
		VisibilityTimestamp: expiryTime,
		EventID:             startedEventID,
	}
	tb.logger.Debugf("createUserTimerTask: with an expiry time: %v", expiryTime.UTC())
	return t
}

func (tb *timerBuilder) loadUserTimer(expires int64, task *persistence.UserTimerTask, taskCreated bool) (*timerDetails, bool) {
	return tb.createTimer(expires, task, taskCreated)
}

func (tb *timerBuilder) createTimer(expires int64, task *persistence.UserTimerTask, taskCreated bool) (*timerDetails, bool) {
	seqNum := tb.localSeqNumGen.NextSeq()
	timer := &timerDetails{
		SequenceID:  SequenceID{VisibilityTimestamp: time.Unix(0, expires), TaskID: seqNum},
		TimerTask:   task,
		TaskCreated: taskCreated}
	isFirst := tb.insertTimer(timer)
	return timer, isFirst
}

func (tb *timerBuilder) insertTimer(td *timerDetails) bool {
	size := len(tb.timers)
	i := sort.Search(size,
		func(i int) bool { return !compareTimerIDLess(&tb.timers[i].SequenceID, &td.SequenceID) })
	if i == size {
		tb.timers = append(tb.timers, td)
	} else {
		tb.timers = append(tb.timers[:i], append(timers{td}, tb.timers[i:]...)...)
	}
	return i == 0 // This is the first timer in the list.
}

func (tb *timerBuilder) firstTimer() persistence.Task {
	if len(tb.timers) > 0 && !tb.timers[0].TaskCreated {
		return tb.createNewTask(tb.timers[0])
	}
	return nil
}

func (tb *timerBuilder) createNewTask(td *timerDetails) persistence.Task {
	task := td.TimerTask

	// Create a copy of this task.
	switch task.GetType() {
	case persistence.TaskTypeUserTimer:
		userTimerTask := task.(*persistence.UserTimerTask)
		return tb.createUserTimerTask(td.SequenceID.VisibilityTimestamp, userTimerTask.EventID)
	}
	return nil
}

func compareTimerIDLess(first *SequenceID, second *SequenceID) bool {
	if first.VisibilityTimestamp.Before(second.VisibilityTimestamp) {
		return true
	}
	if first.VisibilityTimestamp.Equal(second.VisibilityTimestamp) {
		return first.TaskID < second.TaskID
	}
	return false
}
