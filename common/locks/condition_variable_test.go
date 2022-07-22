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
	"math/rand"
	"sync"
	"testing"
	"time"

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
	rand.Seed(time.Now().UnixNano())
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
		defer waitGroup.Done()

		s.lock.Lock()
		defer s.lock.Unlock()

		signalWaitGroup.Done()
		s.cv.Wait(nil)
	}
	go waitFn()

	signalWaitGroup.Wait()
	s.lock.Lock()
	func() {}()
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
		defer waitGroup.Done()

		s.lock.Lock()
		defer s.lock.Unlock()

		interruptWaitGroup.Done()
		s.cv.Wait(interruptChan)
	}
	go waitFn()

	interruptWaitGroup.Wait()
	s.lock.Lock()
	func() {}()
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
		defer waitGroup.Done()

		s.lock.Lock()
		defer s.lock.Unlock()

		broadcastWaitGroup.Done()
		s.cv.Wait(nil)
	}
	for i := 0; i < waitThreads; i++ {
		go waitFn()
	}

	broadcastWaitGroup.Wait()
	s.lock.Lock()
	func() {}()
	s.lock.Unlock()
	s.cv.Broadcast()
	waitGroup.Wait()
}

func (s *conditionVariableSuite) TestCase_ProducerConsumer() {
	signalRatio := 0.8
	numProducer := 256
	numConsumer := 256
	totalToken := numProducer * numConsumer * 10
	tokenPerProducer := totalToken / numProducer
	tokenPerConsumer := totalToken / numConsumer

	lock := &sync.Mutex{}
	tokens := 0
	notifyProducerCV := NewConditionVariable(lock)
	notifyConsumerCV := NewConditionVariable(lock)

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(numProducer + numConsumer)

	produceFn := func() {
		defer waitGroup.Done()
		remainingToken := tokenPerProducer

		lock.Lock()
		defer lock.Unlock()

		for remainingToken > 0 {
			for tokens > 0 {
				randSignalBroadcast(notifyConsumerCV, signalRatio)
				notifyProducerCV.Wait(nil)
			}

			produce := rand.Intn(remainingToken + 1)
			tokens += produce
			remainingToken -= produce
		}
		randSignalBroadcast(notifyConsumerCV, signalRatio)
	}

	consumerFn := func() {
		defer waitGroup.Done()
		remainingToken := 0

		lock.Lock()
		defer lock.Unlock()

		for remainingToken < tokenPerConsumer {
			for tokens == 0 {
				randSignalBroadcast(notifyProducerCV, signalRatio)
				notifyConsumerCV.Wait(nil)
			}

			consume := min(tokens, tokenPerConsumer-remainingToken)
			tokens -= consume
			remainingToken += consume
		}
		randSignalBroadcast(notifyProducerCV, signalRatio)
		if tokens > 0 {
			randSignalBroadcast(notifyConsumerCV, signalRatio)
		}
	}

	for i := 0; i < numConsumer; i++ {
		go consumerFn()
	}
	for i := 0; i < numProducer; i++ {
		go produceFn()
	}

	waitGroup.Wait()
}

func randSignalBroadcast(
	cv ConditionVariable,
	signalRatio float64,
) {
	if rand.Float64() <= signalRatio {
		cv.Signal()
	} else {
		cv.Broadcast()
	}
}

func min(left int, right int) int {
	if left < right {
		return left
	} else if left > right {
		return right
	} else {
		return left
	}
}
