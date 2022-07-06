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
	timer := NewLocalGate(clock.NewRealTimeSource())

	for i := 0; i < b.N; i++ {
		timer.Update(time.Now().UTC())
	}
}

func TestLocalTimerGeteSuite(t *testing.T) {
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
	case <-s.localTimerGate.FireChan():
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

func (s *localGateSuite) TestTimerFireAfterUpdate_Active_Updated() {
	now := time.Now().UTC()
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

func (s *localGateSuite) TestTimerFireAfterUpdate_Active_NotUpdated() {
	now := time.Now().UTC()
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

func (s *localGateSuite) TestTimerFireAfterUpdate_NotActive_Updated() {
	now := time.Now().UTC()
	newTimer := now.Add(-5 * time.Second)
	updatedNewTimer := now.Add(1 * time.Second)
	deadline := now.Add(3 * time.Second)

	s.localTimerGate.Update(newTimer)
	// this is to drain existing signal
	<-s.localTimerGate.FireChan()
	// test setup up complete

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireChan():
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
	<-s.localTimerGate.FireChan()
	// test setup up complete

	s.True(s.localTimerGate.Update(updatedNewTimer))
	select {
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(deadline.Sub(now)).C:
		s.Fail("timer should fire before test deadline")
	}
}

func (s *localGateSuite) TestTimerWillFire_Zero() {
	// this test is to validate initial notification will trigger a scan of timer
	s.localTimerGate.Update(time.Time{})
	s.False(s.localTimerGate.FireAfter(time.Now().UTC()))

	select { // this is to drain existing signal
	case <-s.localTimerGate.FireChan():
	case <-time.NewTimer(time.Second).C:
	}

	now := time.Now().UTC()
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

	now = time.Now().UTC()
	newTimer = now.Add(1 * time.Second)
	s.localTimerGate.Update(newTimer)
	s.localTimerGate.Update(time.Time{})
	select { // this is to drain existing signal
	case <-s.localTimerGate.FireChan():
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
