package workflow

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"code.uber.internal/devexp/minions/common/util"
	"code.uber.internal/devexp/minions/persistence"
	"github.com/uber-common/bark"
)

// Timer constansts
const (
	TimerQueueSeqNumBits       = 26 // For timer-queues, use 38 bits of (expiry) timestamp, 26 bits of seqnum
	TimerQueueSeqNumBitmask    = (int64(1) << TimerQueueSeqNumBits) - 1
	TimerQueueTimeStampBitmask = math.MaxInt64 &^ TimerQueueSeqNumBitmask
	SeqNumMax                  = math.MaxInt64 & TimerQueueSeqNumBitmask // The max allowed seqnum (subject to mode-specific bitmask)
	MinTimerKey                = -1
	MaxTimerKey                = math.MaxInt64
)

type (
	timerDetails struct {
		SequenceID SequenceID
		TimerTask  persistence.Task
	}

	timers []*timerDetails

	timerBuilder struct {
		timers         timers
		logger         bark.Logger
		seqNumGen      SequenceNumberGenerator // The real sequence number generator
		localSeqNumGen SequenceNumberGenerator // This one used to order in-memory list.
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
	return fmt.Sprintf("SequenceID=%x(%x %x)", int64(s), expiry, seqNum)
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
		timers:         timers{},
		logger:         logger.WithField(tagWorkflowComponent, "timer"),
		seqNumGen:      seqNumGen,
		localSeqNumGen: &localSeqNumGenerator{counter: 1}}
}

// CreateDecisionTimeoutTask - Creates a decision timeout task.
func (tb *timerBuilder) CreateDecisionTimeoutTask(fireTimeOut int32) *persistence.DecisionTimeoutTask {
	expiryTime := util.AddSecondsToBaseTime(time.Now().UnixNano(), int64(fireTimeOut))
	seqID := ConstructTimerKey(expiryTime, tb.seqNumGen.NextSeq())
	return &persistence.DecisionTimeoutTask{
		TaskID: int64(seqID),
	}
}

// CreateActivityTimeoutTask - Creates a activity timeout task.
func (tb *timerBuilder) CreateActivityTimeoutTask(fireTimeOut int32) *persistence.ActivityTimeoutTask {
	expiryTime := util.AddSecondsToBaseTime(time.Now().UnixNano(), int64(fireTimeOut))
	seqID := ConstructTimerKey(expiryTime, tb.seqNumGen.NextSeq())
	return &persistence.ActivityTimeoutTask{
		TaskID: int64(seqID),
	}
}

// CreateUserTimerTask - Creates a user timer task.
func (tb *timerBuilder) CreateUserTimerTask(fireTimeOut int32, taskList string, startedEventID int64) *persistence.UserTimerTask {
	expiryTime := util.AddSecondsToBaseTime(time.Now().UnixNano(), int64(fireTimeOut))
	seqID := ConstructTimerKey(expiryTime, tb.seqNumGen.NextSeq())
	return &persistence.UserTimerTask{
		TaskID:   int64(seqID),
		TaskList: taskList,
		EventID:  startedEventID,
	}
}

func (tb *timerBuilder) LoadUserTimer(expires int64, task *persistence.UserTimerTask) (*timerDetails, bool) {
	return tb.createTimer(expires, task)
}

func (tb *timerBuilder) UnLoadUserTimer(startedEventID int64) {
	for i, t := range tb.timers {
		if t.TimerTask.GetType() == persistence.TaskTypeUserTimer {
			userTimerTask := t.TimerTask.(*persistence.UserTimerTask)
			if userTimerTask.EventID == startedEventID {
				tb.timers = append(tb.timers[:i], tb.timers[i+1:]...)
				return
			}
		}
	}
}

func (tb *timerBuilder) RemoveTimer(task persistence.Task) (persistence.Task, error) {
	if len(tb.timers) > 0 {
		// // Sanity check.
		// if tb.timers[0].SequenceID != taskSeqID {
		// 	return nil, fmt.Errorf("The first timer: (%s) doesn't match with asked timer: (%s).",
		// 		taskSeqID, tb.timers[0].SequenceID)
		// }
		tb.timers = tb.timers[1:]
	}

	if len(tb.timers) > 0 {
		nextTask := tb.timers[0]

		// Allocate real sequence number
		expiry, _ := DeconstructTimerKey(nextTask.SequenceID)
		newNewTaskSeqID := ConstructTimerKey(expiry, tb.seqNumGen.NextSeq())

		// Create a copy of this task.
		switch task.GetType() {
		case persistence.TaskTypeUserTimer:
			userTimerTask := nextTask.TimerTask.(*persistence.UserTimerTask)
			return &persistence.UserTimerTask{
				TaskID:   int64(newNewTaskSeqID),
				EventID:  userTimerTask.EventID,
				TaskList: userTimerTask.TaskList}, nil
		}
	}

	return nil, nil
}

func (tb *timerBuilder) createTimer(expires int64, task *persistence.UserTimerTask) (*timerDetails, bool) {
	seqNum := tb.localSeqNumGen.NextSeq()
	timer := &timerDetails{
		SequenceID: ConstructTimerKey(expires, seqNum),
		TimerTask:  task}
	isFirst := tb.insertTimer(timer)
	tb.logger.Debugf("createTimer: td: %s \n", timer)
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
	if len(tb.timers) > 0 {
		return tb.timers[0].TimerTask
	}
	return nil
}
