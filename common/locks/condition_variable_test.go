package locks

import (
	"math/rand"
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
		cv   *ConditionVariableImpl
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

func (s *conditionVariableSuite) TestChannelSize_New() {
	s.testChannelSize(s.cv.channel)
}

func (s *conditionVariableSuite) TestChannelSize_Broadcast() {
	s.cv.Broadcast()
	s.testChannelSize(s.cv.channel)
}

func (s *conditionVariableSuite) testChannelSize(
	channel chan struct{},
) {
	// assert channel size == 1
	select {
	case channel <- struct{}{}:
		// noop
	default:
		s.Fail("conditional variable size should be 1")
	}

	select {
	case channel <- struct{}{}:
		s.Fail("conditional variable size should be 1")
	default:
		// noop
	}
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
