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

package timer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	remoteGateSuite struct {
		suite.Suite
		*require.Assertions

		currentTime     time.Time
		remoteTimerGate RemoteGate
	}
)

func TestRemoteTimerGeteSuite(t *testing.T) {
	s := new(remoteGateSuite)
	suite.Run(t, s)
}

func (s *remoteGateSuite) SetupSuite() {

}

func (s *remoteGateSuite) TearDownSuite() {

}

func (s *remoteGateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.currentTime = time.Now().UTC().Add(-10 * time.Minute)
	s.remoteTimerGate = NewRemoteGate()
	s.remoteTimerGate.SetCurrentTime(s.currentTime)
}

func (s *remoteGateSuite) TearDownTest() {

}

func (s *remoteGateSuite) TestTimerFire() {
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

func (s *remoteGateSuite) TestTimerFireAfterUpdate_Active_Updated_BeforeNow() {
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

func (s *remoteGateSuite) TestTimerFireAfterUpdate_Active_Updated() {
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

func (s *remoteGateSuite) TestTimerFireAfterUpdate_Active_NotUpdated() {
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

func (s *remoteGateSuite) TestTimerFireAfterUpdate_NotActive_Updated() {
	now := s.currentTime
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(2 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	// this is to drain existing signal
	<-s.remoteTimerGate.FireChan()
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

func (s *remoteGateSuite) TestTimerFireAfterUpdate_NotActive_NotUpdated() {
	now := s.currentTime
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(-1 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	// this is to drain existing signal
	<-s.remoteTimerGate.FireChan()
	// test setup up complete

	s.True(s.remoteTimerGate.Update(updatedNewTimer))
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire when new timer is in the past")
	}
}

func (s *remoteGateSuite) TestTimerSetCurrentTime_NoUpdate() {
	now := s.currentTime
	newCurrentTime := now.Add(-1 * time.Second)
	s.False(s.remoteTimerGate.SetCurrentTime(newCurrentTime))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire")
	default:
	}
}

func (s *remoteGateSuite) TestTimerSetCurrentTime_Update_TimerAlreadyFired() {
	now := s.currentTime
	newTimer := now.Add(-1 * time.Second)
	newCurrentTime := now.Add(1 * time.Second)

	s.remoteTimerGate.Update(newTimer)
	// this is to drain existing signal
	<-s.remoteTimerGate.FireChan()
	// test setup up complete

	s.True(s.remoteTimerGate.SetCurrentTime(newCurrentTime))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire")
	default:
	}
}

func (s *remoteGateSuite) TestTimerSetCurrentTime_Update_TimerNotFired() {
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

func (s *remoteGateSuite) TestTimerSetCurrentTime_Update_TimerFired() {
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

func (s *remoteGateSuite) TestTimerWillFire_Zero() {
	// this test is to validate initial notification will trigger a scan of timer
	s.remoteTimerGate.Update(time.Time{})
	s.False(s.remoteTimerGate.FireAfter(time.Now().UTC()))
}

func (s *remoteGateSuite) TestTimerWillFire_Active() {
	now := s.currentTime
	newTimer := now.Add(2 * time.Second)
	timeBeforeNewTimer := now.Add(1 * time.Second)
	timeAfterNewimer := now.Add(3 * time.Second)
	s.remoteTimerGate.Update(newTimer)
	s.True(s.remoteTimerGate.FireAfter(timeBeforeNewTimer))
	s.False(s.remoteTimerGate.FireAfter(timeAfterNewimer))
}

func (s *remoteGateSuite) TestTimerWillFire_NotActive() {
	now := s.currentTime
	newTimer := now.Add(-2 * time.Second)
	timeBeforeTimer := now.Add(-3 * time.Second)
	timeAfterTimer := now.Add(1 * time.Second)
	s.remoteTimerGate.Update(newTimer)
	s.False(s.remoteTimerGate.FireAfter(timeBeforeTimer))
	s.False(s.remoteTimerGate.FireAfter(timeAfterTimer))
}
