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

package matching

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/clock"
)

type (
	livenessSuite struct {
		suite.Suite
		*require.Assertions

		timeSource   *clock.EventTimeSource
		ttl          time.Duration
		shutdownFlag int32
	}
)

func TestLivenessSuite(t *testing.T) {
	s := new(livenessSuite)
	suite.Run(t, s)
}

func (s *livenessSuite) SetupSuite() {
}

func (s *livenessSuite) TearDownSuite() {
}

func (s *livenessSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.ttl = 2 * time.Second
	s.timeSource = clock.NewEventTimeSource()
	s.timeSource.Update(time.Now())
	atomic.StoreInt32(&s.shutdownFlag, 0)
}

func (s *livenessSuite) TearDownTest() {

}

func (s *livenessSuite) TestIsAlive_No() {
	liveness := newLiveness(s.timeSource, s.ttl, func() { atomic.CompareAndSwapInt32(&s.shutdownFlag, 0, 1) })
	s.timeSource.Update(time.Now().Add(s.ttl * 2))
	alive := liveness.isAlive()
	s.False(alive)
}

func (s *livenessSuite) TestIsAlive_Yes() {
	liveness := newLiveness(s.timeSource, s.ttl, func() { atomic.CompareAndSwapInt32(&s.shutdownFlag, 0, 1) })
	s.timeSource.Update(time.Now().Add(s.ttl / 2))
	alive := liveness.isAlive()
	s.True(alive)
}

func (s *livenessSuite) TestMarkAlive_Noop() {
	liveness := newLiveness(s.timeSource, s.ttl, func() { atomic.CompareAndSwapInt32(&s.shutdownFlag, 0, 1) })
	lastEventTime := liveness.lastEventTime
	newEventTime := s.timeSource.Now().Add(-1)
	liveness.markAlive(newEventTime)
	s.Equal(lastEventTime, liveness.lastEventTime)
}

func (s *livenessSuite) TestMarkAlive_Updated() {
	liveness := newLiveness(s.timeSource, s.ttl, func() { atomic.CompareAndSwapInt32(&s.shutdownFlag, 0, 1) })
	newEventTime := s.timeSource.Now().Add(1)
	liveness.markAlive(newEventTime)
	s.Equal(newEventTime, liveness.lastEventTime)
}

func (s *livenessSuite) TestEventLoop_Noop() {
	liveness := newLiveness(s.timeSource, s.ttl, func() { atomic.CompareAndSwapInt32(&s.shutdownFlag, 0, 1) })
	liveness.Start()

	now := time.Now().Add(s.ttl * 4)
	s.timeSource.Update(now)
	liveness.markAlive(now)

	timer := time.NewTimer(s.ttl * 2)
	select {
	case <-liveness.shutdownChan:
		s.Fail("should not shutdown")
	case <-timer.C:
		s.Equal(int32(0), atomic.LoadInt32(&s.shutdownFlag))
	}
}

func (s *livenessSuite) TestEventLoop_Shutdown() {
	liveness := newLiveness(s.timeSource, s.ttl, func() { atomic.CompareAndSwapInt32(&s.shutdownFlag, 0, 1) })
	liveness.Start()

	s.timeSource.Update(time.Now().Add(s.ttl * 4))
	<-liveness.shutdownChan
	// pass
}
