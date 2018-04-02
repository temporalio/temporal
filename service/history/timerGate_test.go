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
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type (
	localTimerGateSuite struct {
		suite.Suite
		localTimerGate LocalTimerGate
	}

	remoteTimerGateSuite struct {
		suite.Suite
		currentTime     time.Time
		remoteTimerGate RemoteTimerGate
	}
)

func TestLocalTimerGeteSuite(t *testing.T) {
	s := new(localTimerGateSuite)
	suite.Run(t, s)
}

func TestRemoteTimerGeteSuite(t *testing.T) {
	s := new(remoteTimerGateSuite)
	suite.Run(t, s)
}

func (s *localTimerGateSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *localTimerGateSuite) TearDownSuite() {

}

func (s *localTimerGateSuite) SetupTest() {
	s.localTimerGate = NewLocalTimerGate()
}

func (s *localTimerGateSuite) TearDownTest() {
	s.localTimerGate.Close()
}

func (s *remoteTimerGateSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *remoteTimerGateSuite) TearDownSuite() {

}

func (s *remoteTimerGateSuite) SetupTest() {
	s.currentTime = time.Now().Add(-10 * time.Minute)
	s.remoteTimerGate = NewRemoteTimerGate()
}

func (s *remoteTimerGateSuite) TearDownTest() {

}

func (s *localTimerGateSuite) TestTimerFire() {
	now := time.Now()
	timerDelay := now.Add(1 * time.Second)
	deadlineDelay := now.Add(2 * time.Second)
	s.localTimerGate.Update(timerDelay)

	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerFireAfterUpdate_Updated() {
	now := time.Now()
	timerDelay := now.Add(5 * time.Second)
	updatedTimerDelay := now.Add(1 * time.Second)
	deadlineDelay := now.Add(3 * time.Second)
	s.localTimerGate.Update(timerDelay)
	s.True(s.localTimerGate.Update(updatedTimerDelay))

	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerFireAfterUpdate_NotUpdated() {
	now := time.Now()
	timerDelay := now.Add(1 * time.Second)
	updatedTimerDelay := now.Add(3 * time.Second)
	deadlineDelay := now.Add(2 * time.Second)
	s.localTimerGate.Update(timerDelay)
	s.False(s.localTimerGate.Update(updatedTimerDelay))

	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localTimerGateSuite) TestTimerWillFire() {
	now := time.Now()
	timerDelay := now.Add(2 * time.Second)
	timeBeforeTimer := now.Add(1 * time.Second)
	timeAfterTimer := now.Add(3 * time.Second)
	s.localTimerGate.Update(timerDelay)
	s.True(s.localTimerGate.FireAfter(timeBeforeTimer))
	s.False(s.localTimerGate.FireAfter(timeAfterTimer))
}

func (s *remoteTimerGateSuite) TestTimerFire() {
	now := s.currentTime
	timerDelay := now.Add(1 * time.Second)
	deadlineDelay := now.Add(2 * time.Second)
	s.remoteTimerGate.Update(timerDelay)

	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
	}

	s.remoteTimerGate.SetCurrentTime(deadlineDelay)
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireAfterUpdate_Updated() {
	now := s.currentTime
	timerDelay := now.Add(5 * time.Second)
	updatedTimerDelay := now.Add(1 * time.Second)
	deadlineDelay := now.Add(3 * time.Second)
	s.remoteTimerGate.Update(timerDelay)
	s.True(s.remoteTimerGate.Update(updatedTimerDelay))

	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
	}

	s.remoteTimerGate.SetCurrentTime(updatedTimerDelay)
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireAfterUpdate_NotUpdated() {
	now := s.currentTime
	timerDelay := now.Add(1 * time.Second)
	updatedTimerDelay := now.Add(3 * time.Second)
	deadlineDelay := now.Add(2 * time.Second)
	s.remoteTimerGate.Update(timerDelay)
	s.False(s.remoteTimerGate.Update(updatedTimerDelay))

	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire when current time not updated")
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
	}

	s.remoteTimerGate.SetCurrentTime(updatedTimerDelay)
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}
}

func (s *remoteTimerGateSuite) TestTimerFireOnlyOnce() {
	now := s.currentTime
	timerDelay := now.Add(1 * time.Second)
	deadlineDelay := now.Add(2 * time.Second)
	s.remoteTimerGate.Update(timerDelay)

	s.True(s.remoteTimerGate.SetCurrentTime(deadlineDelay))
	select {
	case <-s.remoteTimerGate.FireChan():
	default:
		s.Fail("timer should fire")
	}

	s.False(s.remoteTimerGate.SetCurrentTime(deadlineDelay))
	select {
	case <-s.remoteTimerGate.FireChan():
		s.Fail("timer should not fire twice")
	default:
	}
}

func (s *remoteTimerGateSuite) TestTimerWillFire() {
	now := s.currentTime
	timerDelay := now.Add(2 * time.Second)
	timeBeforeTimer := now.Add(1 * time.Second)
	timeAfterTimer := now.Add(3 * time.Second)
	s.remoteTimerGate.Update(timerDelay)
	s.True(s.remoteTimerGate.FireAfter(timeBeforeTimer))
	s.False(s.remoteTimerGate.FireAfter(timeAfterTimer))
}
