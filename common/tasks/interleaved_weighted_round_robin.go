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

package tasks

import (
	"sort"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

var _ Scheduler = (*InterleavedWeightedRoundRobinScheduler)(nil)

type (
	// InterleavedWeightedRoundRobinSchedulerOptions is the config for
	// interleaved weighted round robin scheduler
	InterleavedWeightedRoundRobinSchedulerOptions struct {
		PriorityToWeight map[Priority]int
	}

	// InterleavedWeightedRoundRobinScheduler is a round robin scheduler implementation
	// ref: https://en.wikipedia.org/wiki/Weighted_round_robin#Interleaved_WRR
	InterleavedWeightedRoundRobinScheduler struct {
		status int32

		processor       Processor
		metricsProvider metrics.MetricsHandler
		logger          log.Logger

		notifyChan   chan struct{}
		shutdownChan chan struct{}

		numInflightTask int64
		sync.RWMutex
		priorityToWeight     map[Priority]int
		weightToTaskChannels map[int]*WeightedChannel
		// precalculated / flattened task chan according to weight
		// e.g. if
		// priorityToWeight := map[Priority]int{
		//		0: 5,
		//		1: 3,
		//		2: 2,
		//		3: 1,
		//	}
		// then iwrrChannels will contain chan [0, 0, 0, 1, 0, 1, 2, 0, 1, 2, 3] (ID-ed by priority)
		iwrrChannels atomic.Value // []*WeightedChannel
	}
)

func NewInterleavedWeightedRoundRobinScheduler(
	option InterleavedWeightedRoundRobinSchedulerOptions,
	processor Processor,
	metricsProvider metrics.MetricsHandler,
	logger log.Logger,
) *InterleavedWeightedRoundRobinScheduler {
	iwrrChannels := atomic.Value{}
	iwrrChannels.Store([]*WeightedChannel{})
	return &InterleavedWeightedRoundRobinScheduler{
		status: common.DaemonStatusInitialized,

		processor:       processor,
		metricsProvider: metricsProvider.WithTags(metrics.OperationTag(OperationTaskScheduler)),
		logger:          logger,

		notifyChan:   make(chan struct{}, 1),
		shutdownChan: make(chan struct{}),

		numInflightTask:      0,
		priorityToWeight:     option.PriorityToWeight,
		weightToTaskChannels: make(map[int]*WeightedChannel),
		iwrrChannels:         iwrrChannels,
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	s.processor.Start()

	go s.eventLoop()

	s.logger.Info("interleaved weighted round robin task scheduler started")
}

func (s *InterleavedWeightedRoundRobinScheduler) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(s.shutdownChan)

	s.processor.Stop()

	s.rescheduleTasks()

	s.logger.Info("interleaved weighted round robin task scheduler stopped")
}

func (s *InterleavedWeightedRoundRobinScheduler) Submit(
	task PriorityTask,
) {
	numTasks := atomic.AddInt64(&s.numInflightTask, 1)
	if numTasks == 1 {
		s.doDispatchTasksDirectly(task)
		return
	}

	// there are tasks pending dispatching, need to respect task priorities
	channel := s.getOrCreateTaskChannel(s.priorityToWeight[task.GetPriority()])
	channel.Chan() <- task
	s.notifyDispatcher()
}

func (s *InterleavedWeightedRoundRobinScheduler) TrySubmit(
	task PriorityTask,
) bool {
	numTasks := atomic.AddInt64(&s.numInflightTask, 1)
	if numTasks == 1 {
		s.doDispatchTasksDirectly(task)
		return true
	}

	// there are tasks pending dispatching, need to respect task priorities
	channel := s.getOrCreateTaskChannel(s.priorityToWeight[task.GetPriority()])
	select {
	case channel.Chan() <- task:
		s.notifyDispatcher()
		return true
	default:
		atomic.AddInt64(&s.numInflightTask, -1)
		return false
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) eventLoop() {
	for {
		select {
		case <-s.notifyChan:
			s.dispatchTasksWithWeight()
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) getOrCreateTaskChannel(
	weight int,
) *WeightedChannel {
	s.RLock()
	channel, ok := s.weightToTaskChannels[weight]
	if ok {
		s.RUnlock()
		return channel
	}
	s.RUnlock()

	s.Lock()
	defer s.Unlock()

	channel, ok = s.weightToTaskChannels[weight]
	if ok {
		return channel
	}

	channel = NewWeightedChannel(weight, WeightedChannelDefaultSize)
	s.weightToTaskChannels[weight] = channel

	weightedChannels := make(WeightedChannels, 0, len(s.weightToTaskChannels))
	for _, weightedChan := range s.weightToTaskChannels {
		weightedChannels = append(weightedChannels, weightedChan)
	}
	sort.Sort(weightedChannels)

	iwrrChannels := make([]*WeightedChannel, 0, len(weightedChannels))
	maxWeight := weightedChannels[len(weightedChannels)-1].Weight()
	for round := maxWeight - 1; round > -1; round-- {
		for index := len(weightedChannels) - 1; index > -1 && weightedChannels[index].Weight() > round; index-- {
			iwrrChannels = append(iwrrChannels, weightedChannels[index])
		}
	}
	s.iwrrChannels.Store(iwrrChannels)

	return channel
}

func (s *InterleavedWeightedRoundRobinScheduler) channels() []*WeightedChannel {
	return s.iwrrChannels.Load().([]*WeightedChannel)
}

func (s *InterleavedWeightedRoundRobinScheduler) notifyDispatcher() {
	if s.isStopped() {
		s.rescheduleTasks()
		return
	}

	select {
	case s.notifyChan <- struct{}{}:
	default:
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) dispatchTasksWithWeight() {
	for s.hasRemainingTasks() {
		weightedChannels := s.channels()
		s.doDispatchTasksWithWeight(weightedChannels)
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) doDispatchTasksWithWeight(
	channels []*WeightedChannel,
) {
	numTasks := int64(0)
LoopDispatch:
	for _, channel := range channels {
		select {
		case task := <-channel.Chan():
			s.processor.Submit(task)
			numTasks++
		default:
			continue LoopDispatch
		}
	}
	atomic.AddInt64(&s.numInflightTask, -numTasks)
}

func (s *InterleavedWeightedRoundRobinScheduler) doDispatchTasksDirectly(
	task PriorityTask,
) {
	s.processor.Submit(task)
	atomic.AddInt64(&s.numInflightTask, -1)
}

func (s *InterleavedWeightedRoundRobinScheduler) hasRemainingTasks() bool {
	numTasks := atomic.LoadInt64(&s.numInflightTask)
	return numTasks > 0
}

func (s *InterleavedWeightedRoundRobinScheduler) rescheduleTasks() {
	s.RLock()
	defer s.RUnlock()

	numTasks := int64(0)
DrainLoop:
	for _, channel := range s.weightToTaskChannels {
		for {
			select {
			case task := <-channel.Chan():
				task.Reschedule()
				numTasks++
			default:
				continue DrainLoop
			}
		}
	}
	atomic.AddInt64(&s.numInflightTask, -numTasks)
}

func (s *InterleavedWeightedRoundRobinScheduler) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}
