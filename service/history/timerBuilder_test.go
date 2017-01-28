package history

import (
	"os"
	"testing"
	"time"

	"code.uber.internal/devexp/minions/common/persistence"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
)

type (
	timerBuilderProcessorSuite struct {
		suite.Suite
		tb     *timerBuilder
		logger bark.Logger
	}
)

func TestTimerBuilderSuite(t *testing.T) {
	s := new(timerBuilderProcessorSuite)
	suite.Run(t, s)
}

func (s *timerBuilderProcessorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	logger := log.New()
	//logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)
	s.tb = newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
}

func (s *timerBuilderProcessorSuite) TestTimerBuilder_SingleUserTimer() {
	tb := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)

	// Add one timer.
	msb := newMutableStateBuilder(s.logger)
	t1, err := tb.AddUserTimer("tid1", 1, 201, msb)
	s.Nil(err)
	s.NotNil(t1)
	s.True(t1.GetTaskID() > 0)
	s.Equal(int64(201), t1.(*persistence.UserTimerTask).EventID)
	s.Equal(t1.GetTaskID(), t1.(*persistence.UserTimerTask).TaskID)

	isRunning, ti := msb.isTimerRunning("tid1")
	s.True(isRunning)
	s.NotNil(ti)
	s.Equal(int64(201), ti.StartedID)
	s.Equal(t1.GetTaskID(), ti.TaskID)
	s.Equal("tid1", ti.TimerID)
	s.False(ti.ExpiryTime.IsZero())
}

func (s *timerBuilderProcessorSuite) TestTimerBuilder_MulitpleUserTimer() {
	tb := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)

	// Add two timers. (before and after)
	tp := &persistence.TimerInfo{TimerID: "tid1", StartedID: 201, TaskID: 101, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos := map[string]*persistence.TimerInfo{"tid1": tp}
	msb := newMutableStateBuilder(s.logger)
	msb.Load(nil, timerInfos)
	t1, err := tb.AddUserTimer("tid-before", 1, 202, msb)
	s.Nil(err)
	s.NotNil(t1)
	s.True(t1.GetTaskID() > 0)

	timerInfos = map[string]*persistence.TimerInfo{"tid1": tp}
	msb = newMutableStateBuilder(s.logger)
	msb.Load(nil, timerInfos)
	t1, err = tb.AddUserTimer("tid-after", 15, 203, msb)
	s.Nil(err)
	s.Nil(t1) // we don't get any timer since there is one in progress.

	// -- timer wth out a timer task.
	tp2 := &persistence.TimerInfo{TimerID: "tid1", StartedID: 201, TaskID: emptyTimerID, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos = map[string]*persistence.TimerInfo{"tid1": tp2}
	msb = newMutableStateBuilder(s.logger)
	msb.Load(nil, timerInfos)
	t1, err = tb.AddUserTimer("tid-after", 15, 203, msb)
	s.Nil(err)
	s.NotNil(t1)
	s.Equal(int64(201), t1.(*persistence.UserTimerTask).EventID)
	s.Equal(t1.GetTaskID(), t1.(*persistence.UserTimerTask).TaskID)

	isRunning, ti := msb.isTimerRunning("tid-after")
	s.True(isRunning)
	s.NotNil(ti)
	s.Equal(int64(emptyTimerID), ti.TaskID)
	s.Equal(int64(203), ti.StartedID)
}

func (s *timerBuilderProcessorSuite) TestTimerBuilder_DuplicateTimerID() {
	tb := newTimerBuilder(&localSeqNumGenerator{counter: 1}, s.logger)
	tp := &persistence.TimerInfo{TimerID: "tid-exist", StartedID: 201, TaskID: 101, ExpiryTime: time.Now().Add(10 * time.Second)}
	timerInfos := map[string]*persistence.TimerInfo{"tid-exist": tp}
	msb := newMutableStateBuilder(s.logger)
	msb.Load(nil, timerInfos)
	t1, err := tb.AddUserTimer("tid-exist", 1, 202, msb)
	s.NotNil(err)
	s.Nil(t1)
}
