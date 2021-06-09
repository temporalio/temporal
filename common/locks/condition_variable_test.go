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

package locks

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	conditionVariableSuite struct {
		*require.Assertions
		suite.Suite

		lock sync.Locker
		cv   ConditionVariable
	}
)

func TestConditionVariableSuite(t *testing.T) {
	s := new(conditionVariableSuite)
	suite.Run(t, s)
}

func (s *conditionVariableSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *conditionVariableSuite) TearDownSuite() {

}

func (s *conditionVariableSuite) SetupTest() {
	s.lock = &sync.Mutex{}
	s.cv = NewConditionVariable(s.lock)
}

func (s *conditionVariableSuite) TearDownTest() {

}

func (s *conditionVariableSuite) TestSignal() {
	signalWaitGroup := sync.WaitGroup{}
	signalWaitGroup.Add(1)

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)

	waitFn := func() {
		s.lock.Lock()
		defer s.lock.Unlock()

		signalWaitGroup.Done()
		s.cv.Wait(nil)
		waitGroup.Done()
	}
	go waitFn()

	signalWaitGroup.Wait()
	s.lock.Lock()
	s.lock.Unlock()
	s.cv.Signal()
	waitGroup.Wait()
}

func (s *conditionVariableSuite) TestInterrupt() {
	interruptWaitGroup := sync.WaitGroup{}
	interruptWaitGroup.Add(1)

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	interruptChan := make(chan struct{})

	waitFn := func() {
		s.lock.Lock()
		defer s.lock.Unlock()

		interruptWaitGroup.Done()
		s.cv.Wait(interruptChan)
		waitGroup.Done()
	}
	go waitFn()

	interruptWaitGroup.Wait()
	s.lock.Lock()
	s.lock.Unlock()
	interruptChan <- struct{}{}
	waitGroup.Wait()
}

func (s *conditionVariableSuite) TestBroadcast() {
	waitThreads := 256

	broadcastWaitGroup := sync.WaitGroup{}
	broadcastWaitGroup.Add(waitThreads)

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(waitThreads)

	waitFn := func() {
		s.lock.Lock()
		defer s.lock.Unlock()

		broadcastWaitGroup.Done()
		s.cv.Wait(nil)
		waitGroup.Done()
	}
	for i := 0; i < waitThreads; i++ {
		go waitFn()
	}

	broadcastWaitGroup.Wait()
	s.lock.Lock()
	s.lock.Unlock()
	s.cv.Broadcast()
	waitGroup.Wait()
}
