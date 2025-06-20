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
	workflowRunExpirationTime := timestamp.TimeValue(t.mutableState.GetExecutionInfo().WorkflowRunExpirationTime)
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
	workflowRunExpirationTime := timestamp.TimeValue(t.mutableState.GetExecutionInfo().WorkflowRunExpirationTime)
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

		if sequenceID := t.getUserTimerTimeout(
			timerInfo,
		); sequenceID != nil {
			timers = append(timers, *sequenceID)
		}
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
	timerInfo *persistencespb.TimerInfo,
) *TimerSequenceID {

	expiryTime := timerInfo.ExpiryTime

	return &TimerSequenceID{
		EventID:      timerInfo.GetStartedEventId(),
		Timestamp:    timestamp.TimeValue(expiryTime),
		TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimerCreated: timerInfo.TaskStatus == TimerTaskStatusCreated,
		Attempt:      1,
	}
}

func (t *timerSequenceImpl) getActivityScheduleToStartTimeout(
	activityInfo *persistencespb.ActivityInfo,
) *TimerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return nil
	}

	// activity is already started
	if activityInfo.StartedEventId != common.EmptyEventID {
		return nil
	}

	scheduleToStartDuration := timestamp.DurationValue(activityInfo.ScheduleToStartTimeout)
	if scheduleToStartDuration == 0 {
		return nil
	}

	timeoutTime := timestamp.TimeValue(activityInfo.ScheduledTime).Add(scheduleToStartDuration)

	return &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    timeoutTime,
		TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedScheduleToStart) > 0,
		Attempt:      activityInfo.Attempt,
	}
}

func (t *timerSequenceImpl) getActivityScheduleToCloseTimeout(
	activityInfo *persistencespb.ActivityInfo,
) *TimerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return nil
	}

	scheduleToCloseDuration := timestamp.DurationValue(activityInfo.ScheduleToCloseTimeout)
	if scheduleToCloseDuration == 0 {
		return nil
	}

	var timeoutTime time.Time
	// for backward compatibility. FirstScheduledTime can be null if mutable state was
	// restored from the version before this field was introduce
	if activityInfo.FirstScheduledTime != nil {
		timeoutTime = timestamp.TimeValue(activityInfo.FirstScheduledTime).Add(scheduleToCloseDuration)
	} else {
		timeoutTime = timestamp.TimeValue(activityInfo.ScheduledTime).Add(scheduleToCloseDuration)
	}

	return &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    timeoutTime,
		TimerType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedScheduleToClose) > 0,
		Attempt:      activityInfo.Attempt,
	}
}

func (t *timerSequenceImpl) getActivityStartToCloseTimeout(
	activityInfo *persistencespb.ActivityInfo,
) *TimerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return nil
	}

	// activity is not started yet
	if activityInfo.StartedEventId == common.EmptyEventID {
		return nil
	}

	startToCloseDuration := timestamp.DurationValue(activityInfo.StartToCloseTimeout)
	if startToCloseDuration == 0 {
		return nil
	}

	timeoutTime := timestamp.TimeValue(activityInfo.StartedTime).Add(startToCloseDuration)

	return &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    timeoutTime,
		TimerType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedStartToClose) > 0,
		Attempt:      activityInfo.Attempt,
	}
}

func (t *timerSequenceImpl) getActivityHeartbeatTimeout(
	activityInfo *persistencespb.ActivityInfo,
) *TimerSequenceID {

	// activity is not scheduled yet, probably due to retry & backoff
	if activityInfo.ScheduledEventId == common.EmptyEventID {
		return nil
	}

	// activity is not started yet
	if activityInfo.StartedEventId == common.EmptyEventID {
		return nil
	}

	// not heartbeat timeout configured
	heartbeatDuration := timestamp.DurationValue(activityInfo.HeartbeatTimeout)
	if heartbeatDuration == 0 {
		return nil
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

	return &TimerSequenceID{
		EventID:      activityInfo.ScheduledEventId,
		Timestamp:    heartbeatTimeout,
		TimerType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		TimerCreated: (activityInfo.TimerTaskStatus & TimerTaskStatusCreatedHeartbeat) > 0,
		Attempt:      activityInfo.Attempt,
	}
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
