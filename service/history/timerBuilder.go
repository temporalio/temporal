package history

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	w "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

// Timer constansts
const (
	TimerQueueSeqNumBits                  = 26 // For timer-queues, use 38 bits of (expiry) timestamp, 26 bits of seqnum
	TimerQueueSeqNumBitmask               = (int64(1) << TimerQueueSeqNumBits) - 1
	TimerQueueTimeStampBitmask            = math.MaxInt64 &^ TimerQueueSeqNumBitmask
	SeqNumMax                             = math.MaxInt64 & TimerQueueSeqNumBitmask // The max allowed seqnum (subject to mode-specific bitmask)
	MinTimerKey                SequenceID = -1
	MaxTimerKey                SequenceID = math.MaxInt64

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
		seqNumGen         SequenceNumberGenerator // The real sequence number generator
		localSeqNumGen    SequenceNumberGenerator // This one used to order in-memory list.
	}

	// SequenceID - Visibility timer stamp + Sequence Number.
	SequenceID int64

	// SequenceNumberGenerator - Generates next sequence number.
	SequenceNumberGenerator interface {
		NextSeq() int64
	}

	localSeqNumGenerator struct {
		counter int64
	}

	shardSeqNumGenerator struct {
		context ShardContext
	}
)

// ConstructTimerKey forms a unique sequence number given a expiry and sequence number.
func ConstructTimerKey(expiryTime int64, seqNum int64) SequenceID {
	return SequenceID((expiryTime & TimerQueueTimeStampBitmask) | (seqNum & TimerQueueSeqNumBitmask))
}

// DeconstructTimerKey decomoposes a unique sequence number to an expiry and sequence number.
func DeconstructTimerKey(key SequenceID) (expiryTime int64, seqNum int64) {
	return int64(int64(key) & TimerQueueTimeStampBitmask), int64(int64(key) & TimerQueueSeqNumBitmask)
}

func (s SequenceID) String() string {
	expiry, seqNum := DeconstructTimerKey(s)
	return fmt.Sprintf("SequenceID=%v(%x %x) %s", int64(s), expiry, seqNum, time.Unix(0, int64(expiry)))
}

// Len implements sort.Interace
func (t timers) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t timers) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t timers) Less(i, j int) bool {
	return t[i].SequenceID < t[j].SequenceID
}

func (td *timerDetails) String() string {
	return fmt.Sprintf("timerDetails: [%s expiry=%s]", td.SequenceID, time.Unix(0, int64(td.SequenceID)))
}

func (s *shardSeqNumGenerator) NextSeq() int64 {
	return s.context.GetTimerSequenceNumber()
}

func (l *localSeqNumGenerator) NextSeq() int64 {
	return atomic.AddInt64(&l.counter, 1)
}

// newTimerBuilder creates a timer builder.
func newTimerBuilder(seqNumGen SequenceNumberGenerator, logger bark.Logger) *timerBuilder {
	return &timerBuilder{
		timers:            timers{},
		pendingUserTimers: make(map[SequenceID]*persistence.TimerInfo),
		logger:            logger.WithField(logging.TagWorkflowComponent, "timer"),
		seqNumGen:         seqNumGen,
		localSeqNumGen:    &localSeqNumGenerator{counter: 1}}
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
	tb.logger.Debugf("Adding Decision Timeout: SequenceID: %v, EventID: %v",
		SequenceID(timeOutTask.TaskID), timeOutTask.EventID)
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
	if targetTime > time.Now().UnixNano() {
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
	tb.logger.Debugf("Adding Activity Timeout: SequenceID: %v, TimeoutType: %v, EventID: %v",
		SequenceID(timeOutTask.TaskID), timeoutType.String(), timeOutTask.EventID)
	return timeOutTask
}

// AddUserTimer - Adds an user timeout request.
func (tb *timerBuilder) AddUserTimer(ti *persistence.TimerInfo, msBuilder *mutableStateBuilder) persistence.Task {
	tb.logger.Debugf("Adding User Timeout: %s", ti.TimerID)

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
func (tb *timerBuilder) IsTimerExpired(td *timerDetails, referenceTime int64) bool {
	expiry, _ := DeconstructTimerKey(td.SequenceID)
	return expiry <= referenceTime
}

// createDecisionTimeoutTask - Creates a decision timeout task.
func (tb *timerBuilder) createDecisionTimeoutTask(fireTimeOut int32, eventID int64) *persistence.DecisionTimeoutTask {
	expiryTime := common.AddSecondsToBaseTime(time.Now().UnixNano(), int64(fireTimeOut))
	seqID := ConstructTimerKey(expiryTime, tb.seqNumGen.NextSeq())
	return &persistence.DecisionTimeoutTask{
		TaskID:  int64(seqID),
		EventID: eventID,
	}
}

// createActivityTimeoutTask - Creates a activity timeout task.
func (tb *timerBuilder) createActivityTimeoutTask(fireTimeOut int32, timeoutType w.TimeoutType,
	eventID int64, baseTime *time.Time) *persistence.ActivityTimeoutTask {
	var expiryTime int64
	if baseTime != nil {
		expiryTime = common.AddSecondsToBaseTime(baseTime.UnixNano(), int64(fireTimeOut))
	} else {
		expiryTime = common.AddSecondsToBaseTime(time.Now().UnixNano(), int64(fireTimeOut))
	}

	seqID := ConstructTimerKey(expiryTime, tb.seqNumGen.NextSeq())
	return &persistence.ActivityTimeoutTask{
		TaskID:      int64(seqID),
		TimeoutType: int(timeoutType),
		EventID:     eventID,
	}
}

// createUserTimerTask - Creates a user timer task.
func (tb *timerBuilder) createUserTimerTask(expiryTime int64, startedEventID int64) *persistence.UserTimerTask {
	seqID := ConstructTimerKey(expiryTime, tb.seqNumGen.NextSeq())
	t := &persistence.UserTimerTask{
		TaskID:  int64(seqID),
		EventID: startedEventID,
	}
	tb.logger.Debugf("createUserTimerTask: %v", t)
	return t
}

func (tb *timerBuilder) loadUserTimer(expires int64, task *persistence.UserTimerTask, taskCreated bool) (*timerDetails, bool) {
	return tb.createTimer(expires, task, taskCreated)
}

func (tb *timerBuilder) createTimer(expires int64, task *persistence.UserTimerTask, taskCreated bool) (*timerDetails, bool) {
	seqNum := tb.localSeqNumGen.NextSeq()
	timer := &timerDetails{
		SequenceID:  ConstructTimerKey(expires, seqNum),
		TimerTask:   task,
		TaskCreated: taskCreated}
	isFirst := tb.insertTimer(timer)
	return timer, isFirst
}

func (tb *timerBuilder) insertTimer(td *timerDetails) bool {
	size := len(tb.timers)
	i := sort.Search(size,
		func(i int) bool { return tb.timers[i].SequenceID >= td.SequenceID })
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

	// Allocate real sequence number
	expiry, _ := DeconstructTimerKey(td.SequenceID)

	// Create a copy of this task.
	switch task.GetType() {
	case persistence.TaskTypeUserTimer:
		userTimerTask := task.(*persistence.UserTimerTask)
		return tb.createUserTimerTask(expiry, userTimerTask.EventID)
	}
	return nil
}
