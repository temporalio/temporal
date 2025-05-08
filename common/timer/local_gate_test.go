package timer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
)

type (
	localGateSuite struct {
		suite.Suite
		*require.Assertions

		localTimerGate LocalGate
	}
)

func BenchmarkLocalTimer(b *testing.B) {
	lg := NewLocalGate(clock.NewRealTimeSource())

	for i := 0; i < b.N; i++ {
		lg.Update(time.Now().UTC())
	}
}

func TestLocalTimerGateSuite(t *testing.T) {
	s := new(localGateSuite)
	suite.Run(t, s)
}

func (s *localGateSuite) SetupSuite() {

}

func (s *localGateSuite) TearDownSuite() {

}

func (s *localGateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.localTimerGate = NewLocalGate(clock.NewRealTimeSource())
}

func (s *localGateSuite) TearDownTest() {
	s.localTimerGate.Close()
}

func (s *localGateSuite) TestTimerFire() {
	now := time.Now().UTC()
	newTimer := now.Add(1 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.localTimerGate.Update(newTimer)

	select {
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localGateSuite) TestTimerFireAfterUpdate_Active_Updated_BeforeNow() {
	now := time.Now().UTC()
	newTimer := now.Add(9 * time.Second)
	updatedNewTimer := now.Add(-1 * time.Second)
	deadline := now.Add(3 * time.Second)

	s.localTimerGate.Update(newTimer)
	select {
	case <-s.localTimerGate.FireCh():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadline.Sub(now)).C:
	}

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localGateSuite) TestTimerFireAfterUpdate_Active_Updated() {
	now := time.Now().UTC()
	newTimer := now.Add(5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(3 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.True(s.localTimerGate.Update(updatedNewTimer))

	select {
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localGateSuite) TestTimerFireAfterUpdate_Active_NotUpdated() {
	now := time.Now().UTC()
	newTimer := now.Add(1 * time.Second)
	updatedNewTimer := now.Add(3 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.False(s.localTimerGate.Update(updatedNewTimer))

	select {
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localGateSuite) TestTimerFireAfterUpdate_NotActive_Updated() {
	now := time.Now().UTC()
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(3 * time.Second)

	s.localTimerGate.Update(newTimer)
	// this is to drain existing signal
	<-s.localTimerGate.FireCh()
	// test setup up complete

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localGateSuite) TestTimerFireAfterUpdate_NotActive_NotUpdated() {
	now := time.Now().UTC()
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(-1 * time.Second)
	deadline := now.Add(1 * time.Second)

	s.localTimerGate.Update(newTimer)
	// this is to drain existing signal
	<-s.localTimerGate.FireCh()
	// test setup up complete

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localGateSuite) TestTimerWillFire_Zero() {
	// this test is to validate initial notification will trigger a scan of timer
	s.localTimerGate.Update(time.Time{})
	s.False(s.localTimerGate.FireAfter(time.Now().UTC()))

	select { // this is to drain existing signal
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(time.Second).C:
	}

	now := time.Now().UTC()
	newTimer := now.Add(1 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.localTimerGate.Update(newTimer)
	select {
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire")
	}
	s.localTimerGate.Update(time.Time{})
	select { // this is to drain existing signal
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(time.Second).C:
		s.Fail("timer should fire")
	}

	now = time.Now().UTC()
	newTimer = now.Add(1 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.localTimerGate.Update(time.Time{})
	select { // this is to drain existing signal
	case <-s.localTimerGate.FireCh():
	case <-time.NewTimer(time.Second).C:
		s.Fail("timer should fire")
	}
}

func (s *localGateSuite) TestTimerWillFire() {
	now := time.Now().UTC()
	newTimer := now.Add(2 * time.Second)
	timeBeforeNewTimer := now.Add(1 * time.Second)
	timeAfterNewTimer := now.Add(3 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.True(s.localTimerGate.FireAfter(timeBeforeNewTimer))
	s.False(s.localTimerGate.FireAfter(timeAfterNewTimer))
}
