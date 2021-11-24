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

type (
	// InterleavedWeightedRoundRobinSchedulerOptions is the config for
	// interleaved weighted round robin scheduler
	InterleavedWeightedRoundRobinSchedulerOptions struct {
		QueueSize   int
		WorkerCount int
	}

	// InterleavedWeightedRoundRobinScheduler is a round robin scheduler implementation
	// ref: https://en.wikipedia.org/wiki/Weighted_round_robin#Interleaved_WRR
	InterleavedWeightedRoundRobinScheduler struct {
		status int32
		option InterleavedWeightedRoundRobinSchedulerOptions

		processor    Processor
		metricsScope metrics.Scope
		logger       log.Logger

		notifyChan   chan struct{}
		shutdownChan chan struct{}

		sync.RWMutex
		priorityToWeight     map[int]int
		weightToTaskChannels map[int]*WeightedChannel
		// precalculated / flattened task chan according to weight
		// e.g. if
		// priorityToWeight := map[int]int{
		//		0: 5,
		//		1: 3,
		//		2: 2,
		//		3: 1,
		//	}
		// then iwrrChannels will contain chan [0, 0, 0, 1, 0, 1, 2, 0, 1, 2, 3] (ID-ed by priority)
		iwrrChannels []*WeightedChannel
	}
)

func NewInterleavedWeightedRoundRobinScheduler(
	option InterleavedWeightedRoundRobinSchedulerOptions,
	priorityToWeight map[int]int,
	processor Processor,
	metricsClient metrics.Client,
	logger log.Logger,
) *InterleavedWeightedRoundRobinScheduler {
	return &InterleavedWeightedRoundRobinScheduler{
		status: common.DaemonStatusInitialized,
		option: option,

		processor:    processor,
		metricsScope: metricsClient.Scope(metrics.TaskSchedulerScope),
		logger:       logger,

		notifyChan:   make(chan struct{}, 1),
		shutdownChan: make(chan struct{}),

		priorityToWeight:     priorityToWeight,
		weightToTaskChannels: make(map[int]*WeightedChannel),
		iwrrChannels:         []*WeightedChannel{},
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
	channel := s.getOrCreateTaskChannel(s.priorityToWeight[task.GetPriority()])
	channel.Chan() <- task
	s.notifyDispatcher()
}

func (s *InterleavedWeightedRoundRobinScheduler) eventLoop() {
	for {
		select {
		case <-s.notifyChan:
			s.dispatchTasks()
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
	s.iwrrChannels = iwrrChannels

	return channel
}

func (s *InterleavedWeightedRoundRobinScheduler) dispatchTasks() {
	for s.hasRemainingTasks() {
		weightedChannels := s.channels()
		s.doDispatchTasks(weightedChannels)
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) channels() []*WeightedChannel {
	s.RLock()
	defer s.RUnlock()

	return s.iwrrChannels
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

func (s *InterleavedWeightedRoundRobinScheduler) doDispatchTasks(
	channels []*WeightedChannel,
) {

LoopDispatch:
	for _, channel := range channels {
		select {
		case task := <-channel.Chan():
			s.processor.Submit(task)

		case <-s.shutdownChan:
			return

		default:
			continue LoopDispatch
		}
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) hasRemainingTasks() bool {
	s.RLock()
	defer s.RUnlock()

	for _, weightedChan := range s.weightToTaskChannels {
		if weightedChan.Len() > 0 {
			return true
		}
	}
	return false
}

func (s *InterleavedWeightedRoundRobinScheduler) rescheduleTasks() {
	s.RLock()
	defer s.RUnlock()

DrainLoop:
	for _, channel := range s.weightToTaskChannels {
		for {
			select {
			case task := <-channel.Chan():
				task.Reschedule()

			default:
				continue DrainLoop
			}
		}
	}
}

func (s *InterleavedWeightedRoundRobinScheduler) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}
