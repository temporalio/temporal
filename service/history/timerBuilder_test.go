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
		tb *timerBuilder
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
	s.tb = newTimerBuilder(&localSeqNumGenerator{counter: 1}, bark.NewLoggerFromLogrus(log.New()))
}

func (s *timerBuilderProcessorSuite) TestTimerBuilder() {
	td, f := s.tb.LoadUserTimer(time.Now().UnixNano()+int64(time.Millisecond), s.tb.CreateUserTimerTask(1, "tid", 1))
	s.NotNil(td)
	s.True(f)
	td, f = s.tb.LoadUserTimer(time.Now().UnixNano()+int64(time.Second), s.tb.CreateUserTimerTask(1, "tid", 2))
	s.NotNil(td)
	s.False(f)
	td, f = s.tb.LoadUserTimer(time.Now().UnixNano()+int64(time.Millisecond), s.tb.CreateUserTimerTask(1, "tid", 3))
	s.NotNil(td)
	s.False(f)

	tNext, err := s.tb.RemoveTimer(&persistence.UserTimerTask{})
	s.NotNil(tNext)
	s.Nil(err)
	tNext, err = s.tb.RemoveTimer(&persistence.UserTimerTask{})
	s.NotNil(tNext)
	s.Nil(err)
	tNext, err = s.tb.RemoveTimer(&persistence.UserTimerTask{})
	s.Nil(tNext)
	s.Nil(err)
}
