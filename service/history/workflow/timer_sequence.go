//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination timer_sequence_mock.go

package workflow

import (
	"sort"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/primitives/timestamp"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

const (
	// user timer task not created / created

	TimerTaskStatusNone = iota
	TimerTaskStatusCreated
)

const (
	// activity timer task status

	TimerTaskStatusCreatedStartToClose = 1 << iota
	TimerTaskStatusCreatedScheduleToStart
	TimerTaskStatusCreatedScheduleToClose
	TimerTaskStatusCreatedHeartbeat
)

// TimerTaskStatusCreatedPerAttempt is the set of activity timer task bits scoped to a
// single attempt. They must be cleared when an activity moves to a new attempt so the
// timers are recreated against the new deadlines. TimerTaskStatusCreatedScheduleToClose
// is deliberately excluded: it is a whole-activity deadline that spans retries, so
// clearing it would regenerate a timeout task that is already pending.
const TimerTaskStatusCreatedPerAttempt = TimerTaskStatusCreatedStartToClose |
	TimerTaskStatusCreatedScheduleToStart |
	TimerTaskStatusCreatedHeartbeat

type (
	// TimerSequenceID represent a in mem timer
	TimerSequenceID struct {
		EventID      int64
		Timestamp    time.Time
		TimerType    enumspb.TimeoutType
		TimerCreated bool
		Attempt      int32
	}

	TimerSequenceIDs []TimerSequenceID

	TimerSequence interface {
		CreateNextUserTimer() (bool, error)
		CreateNextActivityTimer() (bool, error)

		LoadAndSortUserTimers() []TimerSequenceID
		LoadAndSortActivityTimers() []TimerSequenceID
	}

	timerSequenceImpl struct {
		mutableState historyi.MutableState
	}
)

var _ TimerSequence = (*timerSequenceImpl)(nil)

func NewTimerSequence(
	mutableState historyi.MutableState,
) *timerSequenceImpl {
	return &timerSequenceImpl{
		mutableState: mutableState,
	}
}

func (t *timerSequenceImpl) CreateNextUserTimer() (bool, error) {

	sequenceIDs := t.LoadAndSortUserTimers()
	if len(sequenceIDs) == 0 {
		return false, nil
	}

	firstTimerTask := sequenceIDs[0]

	// user timer after workflow timeout, skip
	execInfo := t.mutableState.GetExecutionInfo()
	workflowRunExpirationTime := timestamp.TimeValue(execInfo.WorkflowRunExpirationTime)
	if !workflowRunExpirationTime.IsZero() && firstTimerTask.Timestamp.After(workflowRunExpirationTime) {
		return false, nil
	}

	// timer has already been created
	if firstTimerTask.TimerCreated {
		return false, nil
	}

	timerInfo, ok := t.mutableState.GetUserTimerInfoByEventID(firstTimerTask.EventID)
	if !ok {
		return false, serviceerror.NewInternalf("unable to load timer info %v", firstTimerTask.EventID)
	}
	// mark timer task mask as indication that timer task is generated
	// here TaskID is misleading attr, should be called timer created flag or something
	timerInfo.TaskStatus = TimerTaskStatusCreated
	if err := t.mutableState.UpdateUserTimerTaskStatus(timerInfo.TimerId, TimerTaskStatusCreated); err != nil {
		return false, err
	}
	t.mutableState.AddTasks(&tasks.UserTimerTask{
		// TaskID is set by shard
		WorkflowKey:         t.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: firstTimerTask.Timestamp,
		EventID:             firstTimerTask.EventID,
	})
	return true, nil
}

func (t *timerSequenceImpl) CreateNextActivityTimer() (bool, error) {

	sequenceIDs := t.LoadAndSortActivityTimers()
	if len(sequenceIDs) == 0 {
		return false, nil
	}

	firstTimerTask := sequenceIDs[0]

	// activity timer after workflow timeout, skip
	execInfo := t.mutableState.GetExecutionInfo()
	workflowRunExpirationTime := timestamp.TimeValue(execInfo.WorkflowRunExpirationTime)
	if !workflowRunExpirationTime.IsZero() && firstTimerTask.Timestamp.After(workflowRunExpirationTime) {
		return false, nil
	}

	// timer has already been created
	if firstTimerTask.TimerCreated {
		return false, nil
	}

	activityInfo, ok := t.mutableState.GetActivityInfo(firstTimerTask.EventID)
	if !ok {
		return false, serviceerror.NewInternalf("unable to load activity info %v", firstTimerTask.EventID)
	}
	// mark timer task mask as indication that timer task is generated
	activityInfo.TimerTaskStatus |= timerTypeToTimerMask(firstTimerTask.TimerType)
	var err error
	var timerTaskStamp *time.Time
	if firstTimerTask.TimerType == enumspb.TIMEOUT_TYPE_HEARTBEAT {
		timerTaskStamp = &firstTimerTask.Timestamp
	}
	err = t.mutableState.UpdateActivityTaskStatusWithTimerHeartbeat(activityInfo.ScheduledEventId, activityInfo.TimerTaskStatus, timerTaskStamp)

	if err != nil {
		return false, err
	}
	t.mutableState.AddTasks(&tasks.ActivityTimeoutTask{
		// TaskID is set by shard
		WorkflowKey:         t.mutableState.GetWorkflowKey(),
		VisibilityTimestamp: firstTimerTask.Timestamp,
		TimeoutType:         firstTimerTask.TimerType,
		EventID:             firstTimerTask.EventID,
		Attempt:             firstTimerTask.Attempt,
		Stamp:               activityInfo.Stamp,
	})
	return true, nil
}

func (t *timerSequenceImpl) LoadAndSortUserTimers() []TimerSequenceID {

	pendingTimers := t.mutableState.GetPendingTimerInfos()
	timers := make(TimerSequenceIDs, 0, len(pendingTimers))

	for _, timerInfo := range pendingTimers {

		timers = append(timers, t.getUserTimerTimeout(timerInfo))
	}

	sort.Sort(timers)
	return timers
}

func (t *timerSequenceImpl) LoadAndSortActivityTimers() []TimerSequenceID {
	// there can be 4 timer per activity
	// see TimerType
	pendingActivities := t.mutableState.GetPendingActivityInfos()
	activityTimers := make(TimerSequenceIDs, 0, len(pendingActivities)*4)

	for _, activityInfo := range pendingActivities {
		// skip activities that are paused
		if activityInfo.Paused {
			continue
		}
		if sequenceID, ok := t.getActivityScheduleToCloseTimeout(
			activityInfo,
		); ok {
			activityTimers = append(activityTimers, sequenceID)
		}

		if sequenceID, ok := t.getActivityScheduleToStartTimeout(
			activityInfo,
		); ok {
			activityTimers = append(activityTimers, sequenceID)
		}

		if sequenceID, ok := t.getActivityStartToCloseTimeout(
			activityInfo,
		); ok {
			activityTimers = append(activityTimers, sequenceID)
		}

		if sequenceID, ok := t.getActivityHeartbeatTimeout(
			activityInfo,
		); ok {
			activityTimers = append(activityTimers, sequenceID)
		}
	}

	sort.Sort(activityTimers)
	return activityTimers
}

func (t *timerSequenceImpl) getUserTimerTimeout(
	timerInfo *persistencespb.TimerInfo,
) TimerSequenceID {

	expiryTime := timerInfo.ExpiryTime

	return TimerSequenceID{
		EventID:      timerInfo.GetStartedEventId(),
		Timestamp:    timestamp.TimeValue(expiryTime),
		TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimerCreated: timerInfo.TaskStatus == TimerTaskStatusCreated,
		Attempt:      1,
	}
}

// activityTimerMasks is every activity timer task bit, ordered to match the entries of
// activityTimerDeadlines.
var activityTimerMasks = [4]int32{
	TimerTaskStatusCreatedScheduleToClose,
	TimerTaskStatusCreatedScheduleToStart,
	TimerTaskStatusCreatedStartToClose,
	TimerTaskStatusCreatedHeartbeat,
}

// activityTimerDeadlines holds, for each entry of activityTimerMasks, when that timer
// fires. A nil entry means the timer does not currently apply to the activity: it is
// not scheduled, not started, or has no timeout configured.
type activityTimerDeadlines [4]*time.Time

// getActivityTimerDeadlines computes all four timer deadlines for a single activity. It
// goes through the same getters the timer sequence itself uses, so there is exactly one
// definition of each deadline.
func getActivityTimerDeadlines(activityInfo *persistencespb.ActivityInfo) activityTimerDeadlines {
	var deadlines activityTimerDeadlines
	for i, getTimeout := range [4]func(*persistencespb.ActivityInfo) (TimerSequenceID, bool){
		getActivityScheduleToCloseTimeout,
		getActivityScheduleToStartTimeout,
		getActivityStartToCloseTimeout,
		getActivityHeartbeatTimeout,
	} {
		if sequenceID, ok := getTimeout(activityInfo); ok {
			deadline := sequenceID.Timestamp
			deadlines[i] = &deadline
		}
	}
	return deadlines
}

// changedMask returns the timer task bits whose deadline differs between the two
// snapshots, i.e. the timers whose already-created task no longer matches the activity
// and has to be recreated. A timer that appears or disappears counts as changed; a
// timer whose deadline is untouched keeps its bit, leaving its pending task alone.
func (d activityTimerDeadlines) changedMask(other activityTimerDeadlines) int32 {
	var changed int32
	for i, mask := range activityTimerMasks {
		curr, next := d[i], other[i]
		if curr == nil && next == nil {
			// timer applies to neither state, so there is nothing to recreate
			continue
		}
		if curr == nil || next == nil || !curr.Equal(*next) {
			// timer appeared, disappeared, or moved
			changed |= mask
		}
	}
	return changed
}

func (t *timerSequenceImpl) getActivityScheduleToStartTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {
	return getActivityScheduleToStartTimeout(activityInfo)
}

func getActivityScheduleToStartTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return TimerSequenceID{}, false
	}

	// activity is already started
	if activityInfo.StartedEventId != common.EmptyEventID {
		return TimerSequenceID{}, false
	}

	scheduleToStartDuration := timestamp.DurationValue(activityInfo.ScheduleToStartTimeout)
	if scheduleToStartDuration == 0 {
		return TimerSequenceID{}, false
	}

	timeoutTime := timestamp.TimeValue(activityInfo.ScheduledTime).Add(scheduleToStartDuration)

	return TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    timeoutTime,
		TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedScheduleToStart) > 0,
		Attempt:      activityInfo.Attempt,
	}, true
}

func (t *timerSequenceImpl) getActivityScheduleToCloseTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {
	return getActivityScheduleToCloseTimeout(activityInfo)
}

func getActivityScheduleToCloseTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return TimerSequenceID{}, false
	}

	scheduleToCloseDuration := timestamp.DurationValue(activityInfo.ScheduleToCloseTimeout)
	if scheduleToCloseDuration == 0 {
		return TimerSequenceID{}, false
	}

	var timeoutTime time.Time
	// for backward compatibility. FirstScheduledTime can be null if mutable state was
	// restored from the version before this field was introduce
	if activityInfo.FirstScheduledTime != nil {
		timeoutTime = timestamp.TimeValue(activityInfo.FirstScheduledTime).Add(scheduleToCloseDuration)
	} else {
		timeoutTime = timestamp.TimeValue(activityInfo.ScheduledTime).Add(scheduleToCloseDuration)
	}

	return TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    timeoutTime,
		TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedScheduleToClose) > 0,
		Attempt:      activityInfo.Attempt,
	}, true
}

func (t *timerSequenceImpl) getActivityStartToCloseTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {
	return getActivityStartToCloseTimeout(activityInfo)
}

func getActivityStartToCloseTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return TimerSequenceID{}, false
	}

	// activity is not started yet
	if activityInfo.StartedEventId == common.EmptyEventID {
		return TimerSequenceID{}, false
	}

	startToCloseDuration := timestamp.DurationValue(activityInfo.StartToCloseTimeout)
	if startToCloseDuration == 0 {
		return TimerSequenceID{}, false
	}

	timeoutTime := timestamp.TimeValue(activityInfo.StartedTime).Add(startToCloseDuration)

	return TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    timeoutTime,
		TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedStartToClose) > 0,
		Attempt:      activityInfo.Attempt,
	}, true
}

func (t *timerSequenceImpl) getActivityHeartbeatTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {
	return getActivityHeartbeatTimeout(activityInfo)
}

func getActivityHeartbeatTimeout(
	activityInfo *persistencespb.ActivityInfo,
) (TimerSequenceID, bool) {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return TimerSequenceID{}, false
	}

	// activity is not started yet
	if activityInfo.StartedEventId == common.EmptyEventID {
		return TimerSequenceID{}, false
	}

	// not heartbeat timeout configured
	heartbeatDuration := timestamp.DurationValue(activityInfo.HeartbeatTimeout)
	if heartbeatDuration == 0 {
		return TimerSequenceID{}, false
	}

	// use the latest time as last heartbeat time
	var lastHeartbeat time.Time
	if activityInfo.StartedTime != nil {
		lastHeartbeat = timestamp.TimeValue(activityInfo.StartedTime)
	}

	if !timestamp.TimeValue(activityInfo.LastHeartbeatUpdateTime).IsZero() && activityInfo.LastHeartbeatUpdateTime.AsTime().After(lastHeartbeat) {
		lastHeartbeat = timestamp.TimeValue(activityInfo.LastHeartbeatUpdateTime)
	}

	heartbeatTimeout := lastHeartbeat.Add(heartbeatDuration)

	return TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    heartbeatTimeout,
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedHeartbeat) > 0,
		Attempt:      activityInfo.Attempt,
	}, true
}

func timerTypeToTimerMask(
	timerType enumspb.TimeoutType,
) int32 {

	switch timerType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		return TimerTaskStatusCreatedStartToClose
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START:
		return TimerTaskStatusCreatedScheduleToStart
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		return TimerTaskStatusCreatedScheduleToClose
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		return TimerTaskStatusCreatedHeartbeat
	default:
		panic("invalid timeout type")
	}
}

// Len implements sort.Interface
func (s TimerSequenceIDs) Len() int {
	return len(s)
}

// Swap implements sort.Interface.
func (s TimerSequenceIDs) Swap(
	this int,
	that int,
) {
	s[this], s[that] = s[that], s[this]
}

// Less implements sort.Interface
func (s TimerSequenceIDs) Less(
	this int,
	that int,
) bool {

	thisSequenceID := s[this]
	thatSequenceID := s[that]

	// order: timeout time, event ID, timeout type

	if thisSequenceID.Timestamp.Before(thatSequenceID.Timestamp) {
		return true
	} else if thisSequenceID.Timestamp.After(thatSequenceID.Timestamp) {
		return false
	}

	// timeout time are the same
	if thisSequenceID.EventID < thatSequenceID.EventID {
		return true
	} else if thisSequenceID.EventID > thatSequenceID.EventID {
		return false
	}

	// timeout time & event ID are the same
	if thisSequenceID.TimerType < thatSequenceID.TimerType {
		return true
	} else if thisSequenceID.TimerType > thatSequenceID.TimerType {
		return false
	}

	// thisSequenceID && thatSequenceID are the same
	return true
}
