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
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
)

const (
	checkRateLimiterEnabledInterval = 1 * time.Minute
)

var _ Scheduler[Task] = (*InterleavedWeightedRoundRobinScheduler[Task, struct{}])(nil)

type (
	// InterleavedWeightedRoundRobinSchedulerOptions is the config for
	// interleaved weighted round robin scheduler
	InterleavedWeightedRoundRobinSchedulerOptions[T Task, K comparable] struct {
		// Required for mapping a task to it's corresponding task channel
		TaskChannelKeyFn TaskChannelKeyFn[T, K]
		// Required for getting the weight for a task channel
		ChannelWeightFn ChannelWeightFn[K]
		// Optional, if specified, re-evaluate task channel weight when channel is not empty
		ChannelWeightUpdateCh chan struct{}
	}

	// TaskChannelKeyFn is the function for mapping a task to its task channel (key)
	TaskChannelKeyFn[T Task, K comparable] func(T) K

	// ChannelWeightFn is the function for mapping a task channel (key) to its weight
	ChannelWeightFn[K comparable] func(K) int

	// InterleavedWeightedRoundRobinScheduler is a round robin scheduler implementation
	// ref: https://en.wikipedia.org/wiki/Weighted_round_robin#Interleaved_WRR
	InterleavedWeightedRoundRobinScheduler[T Task, K comparable] struct {
		status int32

		fifoScheduler Scheduler[T]
		logger        log.Logger

		notifyChan   chan struct{}
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		options InterleavedWeightedRoundRobinSchedulerOptions[T, K]

		numInflightTask int64

		sync.RWMutex
		weightedChannels map[K]*WeightedChannel[T]

		// precalculated / flattened task chan according to weight
		// e.g. if
		// ChannelKeyToWeight has the following mapping
		//  0 -> 5
		//  1 -> 3
		//  2 -> 2
		//  3 -> 1
		// then iwrrChannels will contain chan [0, 0, 0, 1, 0, 1, 2, 0, 1, 2, 3] (ID-ed by channel key)
		iwrrChannels atomic.Value // []*WeightedChannel
	}
)

func NewInterleavedWeightedRoundRobinScheduler[T Task, K comparable](
	options InterleavedWeightedRoundRobinSchedulerOptions[T, K],
	fifoScheduler Scheduler[T],
	logger log.Logger,
) *InterleavedWeightedRoundRobinScheduler[T, K] {
	iwrrChannels := atomic.Value{}
	iwrrChannels.Store(WeightedChannels[T]{})

	return &InterleavedWeightedRoundRobinScheduler[T, K]{
		status: common.DaemonStatusInitialized,

		fifoScheduler: fifoScheduler,
		logger:        logger,

		options: options,

		notifyChan:   make(chan struct{}, 1),
		shutdownChan: make(chan struct{}),

		numInflightTask:  0,
		weightedChannels: make(map[K]*WeightedChannel[T]),
		iwrrChannels:     iwrrChannels,
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	s.fifoScheduler.Start()

	s.shutdownWG.Add(1)
	go s.eventLoop()

	s.logger.Info("interleaved weighted round robin task scheduler started")
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(s.shutdownChan)

	s.fifoScheduler.Stop()

	s.abortTasks()

	if success := common.AwaitWaitGroup(&s.shutdownWG, time.Minute); !success {
		s.logger.Warn("interleaved weighted round robin task scheduler timed out on shutdown.")
	}
	s.logger.Info("interleaved weighted round robin task scheduler stopped")
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) Submit(
	task T,
) {
	numTasks := atomic.AddInt64(&s.numInflightTask, 1)
	if !s.isStopped() && numTasks == 1 {
		s.doDispatchTaskDirectly(task)
		return
	}

	// there are tasks pending dispatching, need to respect round roubin weight
	// or currently unable to submit to fifo scheduler, either due to buffer is full
	// or exceeding rate limit
	channel := s.getOrCreateTaskChannel(s.options.TaskChannelKeyFn(task))
	channel.Chan() <- task
	s.notifyDispatcher()
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) TrySubmit(
	task T,
) bool {
	numTasks := atomic.AddInt64(&s.numInflightTask, 1)
	if !s.isStopped() && numTasks == 1 && s.tryDispatchTaskDirectly(task) {
		return true
	}

	// there are tasks pending dispatching, need to respect round roubin weight
	channel := s.getOrCreateTaskChannel(s.options.TaskChannelKeyFn(task))
	select {
	case channel.Chan() <- task:
		s.notifyDispatcher()
		return true
	default:
		atomic.AddInt64(&s.numInflightTask, -1)
		return false
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) eventLoop() {
	defer s.shutdownWG.Done()

	for {
		select {
		case <-s.notifyChan:
			s.dispatchTasksWithWeight()
		case <-s.shutdownChan:
			return
		}
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) getOrCreateTaskChannel(
	channelKey K,
) *WeightedChannel[T] {
	s.RLock()
	channel, ok := s.weightedChannels[channelKey]
	if ok {
		s.RUnlock()
		return channel
	}
	s.RUnlock()

	s.Lock()
	defer s.Unlock()

	channel, ok = s.weightedChannels[channelKey]
	if ok {
		return channel
	}

	weight := s.options.ChannelWeightFn(channelKey)
	channel = NewWeightedChannel[T](weight, WeightedChannelDefaultSize)
	s.weightedChannels[channelKey] = channel

	s.flattenWeightedChannelsLocked()
	return channel
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) flattenWeightedChannelsLocked() {
	weightedChannels := make(WeightedChannels[T], 0, len(s.weightedChannels))
	for _, weightedChan := range s.weightedChannels {
		weightedChannels = append(weightedChannels, weightedChan)
	}
	sort.Sort(weightedChannels)

	iwrrChannels := make(WeightedChannels[T], 0, len(weightedChannels))
	maxWeight := weightedChannels[len(weightedChannels)-1].Weight()
	for round := maxWeight - 1; round > -1; round-- {
		for index := len(weightedChannels) - 1; index > -1 && weightedChannels[index].Weight() > round; index-- {
			iwrrChannels = append(iwrrChannels, weightedChannels[index])
		}
	}
	s.iwrrChannels.Store(iwrrChannels)
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) channels() WeightedChannels[T] {
	return s.iwrrChannels.Load().(WeightedChannels[T])
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) notifyDispatcher() {
	if s.isStopped() {
		s.abortTasks()
		return
	}

	select {
	case s.notifyChan <- struct{}{}:
	default:
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) receiveWeightUpdateNotification() bool {
	if s.options.ChannelWeightUpdateCh == nil {
		return false
	}

	select {
	case <-s.options.ChannelWeightUpdateCh:
		// drain the channel as we don't know the channel size
		for {
			select {
			case <-s.options.ChannelWeightUpdateCh:
			default:
				return true
			}
		}
	default:
		return false
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) updateChannelWeightLocked() {
	for channelKey, weightedChannel := range s.weightedChannels {
		weightedChannel.SetWeight(s.options.ChannelWeightFn(channelKey))
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) dispatchTasksWithWeight() {
	for s.hasRemainingTasks() {
		if s.receiveWeightUpdateNotification() {
			s.Lock()
			s.updateChannelWeightLocked()
			s.flattenWeightedChannelsLocked()
			s.Unlock()
		}

		weightedChannels := s.channels()
		s.doDispatchTasksWithWeight(weightedChannels)
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) doDispatchTasksWithWeight(
	channels WeightedChannels[T],
) {
	numTasks := int64(0)
LoopDispatch:
	for _, channel := range channels {
		select {
		case task := <-channel.Chan():
			s.fifoScheduler.Submit(task)
			numTasks++
		default:
			continue LoopDispatch
		}
	}
	atomic.AddInt64(&s.numInflightTask, -numTasks)
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) doDispatchTaskDirectly(
	task T,
) {
	s.fifoScheduler.Submit(task)
	atomic.AddInt64(&s.numInflightTask, -1)
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) tryDispatchTaskDirectly(
	task T,
) bool {
	dispatched := s.fifoScheduler.TrySubmit(task)
	if dispatched {
		atomic.AddInt64(&s.numInflightTask, -1)
	}
	return dispatched
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) hasRemainingTasks() bool {
	numTasks := atomic.LoadInt64(&s.numInflightTask)
	return numTasks > 0
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) abortTasks() {
	s.RLock()
	defer s.RUnlock()

	numTasks := int64(0)
DrainLoop:
	for _, channel := range s.weightedChannels {
		for {
			select {
			case task := <-channel.Chan():
				task.Abort()
				numTasks++
			default:
				continue DrainLoop
			}
		}
	}
	atomic.AddInt64(&s.numInflightTask, -numTasks)
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}
