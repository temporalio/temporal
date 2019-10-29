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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/clock"
)

type (
	localTimerGateSuite struct {
		suite.Suite
		*require.Assertions

		localTimerGate LocalTimerGate
	}

	remoteTimerGateSuite struct {
		suite.Suite
		*require.Assertions

		currentTime     time.Time
		remoteTimerGate RemoteTimerGate
	}
)

func BenchmarkLocalTimer(b *testing.B) {
	timer := NewLocalTimerGate(clock.NewRealTimeSource())

	for i := 0; i < b.N; i++ {
		timer.Update(time.Now())
	}
}

func TestLocalTimerGeteSuite(t *testing.T) {
	s := new(localTimerGateSuite)
	suite.Run(t, s)
}

func TestRemoteTimerGeteSuite(t *testing.T) {
	s := new(remoteTimerGateSuite)
	suite.Run(t, s)
}

func (s *localTimerGateSuite) SetupSuite() {

}

func (s *localTimerGateSuite) TearDownSuite() {

}

func (s *localTimerGateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.localTimerGate = NewLocalTimerGate(clock.NewRealTimeSource())
}

func (s *localTimerGateSuite) TearDownTest() {
	s.localTimerGate.Close()
}

func (s *remoteTimerGateSuite) SetupSuite() {

}

func (s *remoteTimerGateSuite) TearDownSuite() {

}

func (s *remoteTimerGateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.currentTime = time.Now().Add(-10 * time.Minute)
	s.remoteTimerGate = NewRemoteTimerGate()
	s.remoteTimerGate.SetCurrentTime(s.currentTime)
}

func (s *remoteTimerGateSuite) TearDownTest() {

}

func (s *localTimerGateSuite) TestTimerFire() {
	now := time.Now()
	newTimer := now.Add(1 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.localTimerGate.Update(newTimer)

	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerFireAfterUpdate_Active_Updated_BeforeNow() {
	now := time.Now()
	newTimer := now.Add(9 * time.Second)
	updatedNewTimer := now.Add(-1 * time.Second)
	deadline := now.Add(3 * time.Second)

	s.localTimerGate.Update(newTimer)
	select {
	case <-s.localTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadline.Sub(now)).C:
	}

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerFireAfterUpdate_Active_Updated() {
	now := time.Now()
	newTimer := now.Add(5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(3 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.True(s.localTimerGate.Update(updatedNewTimer))

	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerFireAfterUpdate_Active_NotUpdated() {
	now := time.Now()
	newTimer := now.Add(1 * time.Second)
	updatedNewTimer := now.Add(3 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.False(s.localTimerGate.Update(updatedNewTimer))

	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerFireAfterUpdate_NotActive_Updated() {
	now := time.Now()
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(3 * time.Second)

	s.localTimerGate.Update(newTimer)
	select { // this is to drain existing signal
	case <-s.localTimerGate.FireChan():
	}
	// test setup up complete

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerFireAfterUpdate_NotActive_NotUpdated() {
	now := time.Now()
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(-1 * time.Second)
	deadline := now.Add(1 * time.Second)

	s.localTimerGate.Update(newTimer)
	select { // this is to drain existing signal
	case <-s.localTimerGate.FireChan():
	}
	// test setup up complete

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerWillFire_Zero() {
	// this test is to validate initial notification will trigger a scan of timer
	s.localTimerGate.Update(time.Time{})
	s.False(s.localTimerGate.FireAfter(time.Now()))

	select { // this is to drain existing signal
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(time.Second).C:
	}

	now := time.Now()
	newTimer := now.Add(1 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.localTimerGate.Update(newTimer)
	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire")
	}
	s.localTimerGate.Update(time.Time{})
	select { // this is to drain existing signal
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(time.Second).C:
		s.Fail("timer should fire")
	}

	now = time.Now()
	newTimer = now.Add(1 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.localTimerGate.Update(time.Time{})
	select { // this is to drain existing signal
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(time.Second).C:
		s.Fail("timer should fire")
	}
}

func (s *localTimerGateSuite) TestTimerWillFire() {
	now := time.Now()
	newTimer := now.Add(2 * time.Second)
	timeBeforeNewTimer := now.Add(1 * time.Second)
	timeAfterNewTimer := now.Add(3 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.True(s.localTimerGate.FireAfter(timeBeforeNewTimer))
	s.False(s.localTimerGate.FireAfter(timeAfterNewTimer))
}

func (s *remoteTimerGateSuite) TestTimerFire() {
	now := s.currentTime
	newTimer := now.Add(1 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.remoteTimerGate.Update(newTimer)

	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadline.Sub(now)).C:
	}

	s.remoteTimerGate.SetCurrentTime(deadline)
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireAfterUpdate_Active_Updated_BeforeNow() {
	now := s.currentTime
	newTimer := now.Add(5 * time.Second)
	updatedNewTimer := now.Add(-1 * time.Second)
	deadline := now.Add(3 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadline.Sub(now)).C:
	}

	s.True(s.remoteTimerGate.Update(updatedNewTimer))
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireAfterUpdate_Active_Updated() {
	now := s.currentTime
	newTimer := now.Add(5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(3 * time.Second)
	s.remoteTimerGate.Update(newTimer)
	s.True(s.remoteTimerGate.Update(updatedNewTimer))

	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadline.Sub(now)).C:
	}

	s.remoteTimerGate.SetCurrentTime(updatedNewTimer)
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireAfterUpdate_Active_NotUpdated() {
	now := s.currentTime
	newTimer := now.Add(1 * time.Second)
	updatedNewTimer := now.Add(3 * time.Second)
	deadline := now.Add(2 * time.Second)
	s.remoteTimerGate.Update(newTimer)
	s.False(s.remoteTimerGate.Update(updatedNewTimer))

	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadline.Sub(now)).C:
	}

	s.remoteTimerGate.SetCurrentTime(updatedNewTimer)
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireAfterUpdate_NotActive_Updated() {
	now := s.currentTime
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(2 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	select { // this is to drain existing signal
	case <-s.remoteTimerGate.FireChan():
	}
	// test setup up complete

	s.True(s.remoteTimerGate.Update(updatedNewTimer))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadline.Sub(now)).C:
	}

	s.remoteTimerGate.SetCurrentTime(updatedNewTimer)
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireAfterUpdate_NotActive_NotUpdated() {
	now := s.currentTime
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(-1 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	select { // this is to drain existing signal
	case <-s.remoteTimerGate.FireChan():
	}
	// test setup up complete

	s.True(s.remoteTimerGate.Update(updatedNewTimer))
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire when new timer is in the past")
	}
}

func (s *remoteTimerGateSuite) TestTimerSetCurrentTime_NoUpdate() {
	now := s.currentTime
	newCurrentTime := now.Add(-1 * time.Second)
	s.False(s.remoteTimerGate.SetCurrentTime(newCurrentTime))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire")
	default:
	}
}

func (s *remoteTimerGateSuite) TestTimerSetCurrentTime_Update_TimerAlreadyFired() {
	now := s.currentTime
	newTimer := now.Add(-1 * time.Second)
	newCurrentTime := now.Add(1 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	select { // this is to drain existing signal
	case <-s.remoteTimerGate.FireChan():
	}
	// test setup up complete

	s.True(s.remoteTimerGate.SetCurrentTime(newCurrentTime))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire")
	default:
	}
}

func (s *remoteTimerGateSuite) TestTimerSetCurrentTime_Update_TimerNotFired() {
	now := s.currentTime
	newTimer := now.Add(2 * time.Second)
	newCurrentTime := now.Add(1 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	s.True(s.remoteTimerGate.SetCurrentTime(newCurrentTime))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire")
	default:
	}
}

func (s *remoteTimerGateSuite) TestTimerSetCurrentTime_Update_TimerFired() {
	now := s.currentTime
	newTimer := now.Add(2 * time.Second)
	newCurrentTime := now.Add(2 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	s.True(s.remoteTimerGate.SetCurrentTime(newCurrentTime))
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}

	// should fire only once
	newCurrentTime = newCurrentTime.Add(1 * time.Second)
	s.True(s.remoteTimerGate.SetCurrentTime(newCurrentTime))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire")
	default:
	}
}

func (s *remoteTimerGateSuite) TestTimerWillFire_Zero() {
	// this test is to validate initial notification will trigger a scan of timer
	s.remoteTimerGate.Update(time.Time{})
	s.False(s.remoteTimerGate.FireAfter(time.Now()))
}

func (s *remoteTimerGateSuite) TestTimerWillFire_Active() {
	now := s.currentTime
	newTimer := now.Add(2 * time.Second)
	timeBeforeNewTimer := now.Add(1 * time.Second)
	timeAfterNewimer := now.Add(3 * time.Second)
	s.remoteTimerGate.Update(newTimer)
	s.True(s.remoteTimerGate.FireAfter(timeBeforeNewTimer))
	s.False(s.remoteTimerGate.FireAfter(timeAfterNewimer))
}

func (s *remoteTimerGateSuite) TestTimerWillFire_NotActive() {
	now := s.currentTime
	newTimer := now.Add(-2 * time.Second)
	timeBeforeTimer := now.Add(-3 * time.Second)
	timeAfterTimer := now.Add(1 * time.Second)
	s.remoteTimerGate.Update(newTimer)
	s.False(s.remoteTimerGate.FireAfter(timeBeforeTimer))
	s.False(s.remoteTimerGate.FireAfter(timeAfterTimer))
}
