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
	timerSuite struct {
		suite.Suite
		timerGate TimerGate
	}
)

func TestTimerSuite(t *testing.T) {
	s := new(timerSuite)
	suite.Run(t, s)
}

func (s *timerSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *timerSuite) TearDownSuite() {

}

func (s *timerSuite) SetupTest() {
	s.timerGate = NewTimerGate()
}

func (s *timerSuite) TearDownTest() {
	s.timerGate.Close()
}

func (s *timerSuite) TestTimerFire() {
	now := time.Now()
	timerDelay := now.Add(1 * time.Second)
	deadlineDelay := now.Add(2 * time.Second)
	s.timerGate.Update(timerDelay)

	select {
	case <-s.timerGate.FireChan():
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *timerSuite) TestTimerFireAfterUpdate_Updated() {
	now := time.Now()
	timerDelay := now.Add(5 * time.Second)
	updatedTimerDelay := now.Add(1 * time.Second)
	deadlineDelay := now.Add(3 * time.Second)
	s.timerGate.Update(timerDelay)
	s.True(s.timerGate.Update(updatedTimerDelay))

	select {
	case <-s.timerGate.FireChan():
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *timerSuite) TestTimerFireAfterUpdate_NotUpdated() {
	now := time.Now()
	timerDelay := now.Add(1 * time.Second)
	updatedTimerDelay := now.Add(3 * time.Second)
	deadlineDelay := now.Add(2 * time.Second)
	s.timerGate.Update(timerDelay)
	s.False(s.timerGate.Update(updatedTimerDelay))

	select {
	case <-s.timerGate.FireChan():
	case <-time.NewTimer(deadlineDelay.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *timerSuite) TestTimerWillFire() {
	now := time.Now()
	timerDelay := now.Add(2 * time.Second)
	timeBeforeTimer := now.Add(1 * time.Second)
	timeAfterTimer := now.Add(3 * time.Second)
	s.timerGate.Update(timerDelay)
	s.True(s.timerGate.FireAfter(timeBeforeTimer))
	s.False(s.timerGate.FireAfter(timeAfterTimer))
}
