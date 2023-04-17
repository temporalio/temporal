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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
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
		// Required for converting task channel to rate limit request
		ChannelQuotaRequestFn ChannelQuotaRequestFn[K]
		// Required for getting metrics tags for task channels
		TaskChannelMetricTagsFn TaskChannelMetricTagsFn[K]
		// Required for determining if rate limiter should be enabled
		EnableRateLimiter dynamicconfig.BoolPropertyFn
		// Required for determining if task should still go through rate limiter and
		// emit metrics, but not actually block task dispatching.
		// only takes effect when rate limiter is not enabled
		EnableRateLimiterShadowMode dynamicconfig.BoolPropertyFn
		// Required for determining how long scheduler should be throttled
		// when exceeding allowed dispatch rate
		DispatchThrottleDuration dynamicconfig.DurationPropertyFn
	}

	// TaskChannelKeyFn is the function for mapping a task to its task channel (key)
	TaskChannelKeyFn[T Task, K comparable] func(T) K

	// ChannelWeightFn is the function for mapping a task channel (key) to its weight
	ChannelWeightFn[K comparable] func(K) int

	// ChannelQuotaRequestFn is the function for mapping a task channel (key) to its rate limit request
	ChannelQuotaRequestFn[K comparable] func(K) quotas.Request

	// TaskChannelMetricTagsFn is the function for mapping a task channel (key) to its metrics tags
	TaskChannelMetricTagsFn[K comparable] func(K) []metrics.Tag

	// InterleavedWeightedRoundRobinScheduler is a round robin scheduler implementation
	// ref: https://en.wikipedia.org/wiki/Weighted_round_robin#Interleaved_WRR
	InterleavedWeightedRoundRobinScheduler[T Task, K comparable] struct {
		status int32

		fifoScheduler  Scheduler[T]
		rateLimiter    quotas.RequestRateLimiter
		timeSource     clock.TimeSource
		logger         log.Logger
		metricsHandler metrics.Handler

		notifyChan   chan struct{}
		shutdownChan chan struct{}

		options InterleavedWeightedRoundRobinSchedulerOptions[T, K]

		numInflightTask int64

		sync.RWMutex
		weightedChannels map[K]*WeightedChannel[T]

		dispatchTimerLock sync.Mutex
		dispatchTimer     *time.Timer
		rateLimiterConfig atomic.Value // rateLimitConfig
		iwrrChannels      atomic.Value
	}

	channelWithStatus[T Task, K comparable] struct {
		*WeightedChannel[T]

		key              K
		rateLimitRequest quotas.Request
		metricsTags      []metrics.Tag

		throttled bool
	}

	channelsWithStatus[T Task, K comparable] []*channelWithStatus[T, K]

	rateLimiterConfig struct {
		enableThrottle   bool
		enableShadowMode bool
	}

	iwrrChannels[T Task, K comparable] struct {
		channels channelsWithStatus[T, K]

		// precalculated / flattened task chan according to weight
		// e.g. if
		// ChannelKeyToWeight has the following mapping
		//  0 -> 5
		//  1 -> 3
		//  2 -> 2
		//  3 -> 1
		// then iwrrChannels will contain chan [0, 0, 0, 1, 0, 1, 2, 0, 1, 2, 3] (ID-ed by channel key)
		flattenedChannels channelsWithStatus[T, K]
	}
)

func NewInterleavedWeightedRoundRobinScheduler[T Task, K comparable](
	options InterleavedWeightedRoundRobinSchedulerOptions[T, K],
	fifoScheduler Scheduler[T],
	rateLimiter quotas.RequestRateLimiter,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *InterleavedWeightedRoundRobinScheduler[T, K] {
	channels := atomic.Value{}
	channels.Store(iwrrChannels[T, K]{})

	rlConfig := atomic.Value{}
	rlConfig.Store(
		rateLimiterConfig{
			enableThrottle:   options.EnableRateLimiter(),
			enableShadowMode: options.EnableRateLimiterShadowMode(),
		},
	)

	return &InterleavedWeightedRoundRobinScheduler[T, K]{
		status: common.DaemonStatusInitialized,

		fifoScheduler:  fifoScheduler,
		rateLimiter:    rateLimiter,
		timeSource:     timeSource,
		logger:         logger,
		metricsHandler: metricsHandler,

		options: options,

		notifyChan:   make(chan struct{}, 1),
		shutdownChan: make(chan struct{}),

		numInflightTask:   0,
		weightedChannels:  make(map[K]*WeightedChannel[T]),
		rateLimiterConfig: rlConfig,
		iwrrChannels:      channels,
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

	s.logger.Info("interleaved weighted round robin task scheduler stopped")
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) Submit(
	task T,
) {
	numTasks := atomic.AddInt64(&s.numInflightTask, 1)
	channelKey := s.options.TaskChannelKeyFn(task)
	if numTasks == 1 && s.tryDispatchTaskDirectly(channelKey, task) {
		return
	}

	// there are tasks pending dispatching, need to respect round roubin weight
	// or currently unable to submit to fifo scheduler, either due to buffer is full
	// or exceeding rate limit
	channel := s.getOrCreateTaskChannel(channelKey)
	channel.Chan() <- task
	s.notifyDispatcher()
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) TrySubmit(
	task T,
) bool {
	numTasks := atomic.AddInt64(&s.numInflightTask, 1)
	channelKey := s.options.TaskChannelKeyFn(task)
	if numTasks == 1 && s.tryDispatchTaskDirectly(channelKey, task) {
		return true
	}

	// there are tasks pending dispatching, need to respect round roubin weight
	channel := s.getOrCreateTaskChannel(channelKey)
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
	checkRateLimiterEnabledTimer := time.NewTicker(checkRateLimiterEnabledInterval)
	defer checkRateLimiterEnabledTimer.Stop()

	for {
		select {
		case <-s.notifyChan:
			s.dispatchTasksWithWeight()
		case <-checkRateLimiterEnabledTimer.C:
			s.rateLimiterConfig.Store(rateLimiterConfig{
				enableThrottle:   s.options.EnableRateLimiter(),
				enableShadowMode: s.options.EnableRateLimiterShadowMode(),
			})
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
	weightedChannels := make(channelsWithStatus[T, K], 0, len(s.weightedChannels))
	for channelKey, weightedChan := range s.weightedChannels {
		weightedChannels = append(weightedChannels, &channelWithStatus[T, K]{
			WeightedChannel:  weightedChan,
			key:              channelKey,
			rateLimitRequest: s.options.ChannelQuotaRequestFn(channelKey),
			metricsTags:      s.options.TaskChannelMetricTagsFn(channelKey),
			throttled:        false,
		})
	}
	sort.Slice(weightedChannels, func(i, j int) bool {
		return weightedChannels[i].Weight() < weightedChannels[j].Weight()
	})

	flattenedChannels := make(channelsWithStatus[T, K], 0, len(weightedChannels))
	maxWeight := weightedChannels[len(weightedChannels)-1].Weight()
	for round := maxWeight - 1; round > -1; round-- {
		for index := len(weightedChannels) - 1; index > -1 && weightedChannels[index].Weight() > round; index-- {
			flattenedChannels = append(flattenedChannels, weightedChannels[index])
		}
	}
	s.iwrrChannels.Store(iwrrChannels[T, K]{
		channels:          weightedChannels,
		flattenedChannels: flattenedChannels,
	})
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) channels() iwrrChannels[T, K] {
	return s.iwrrChannels.Load().(iwrrChannels[T, K])
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) setupDispatchTimer() {
	s.dispatchTimerLock.Lock()
	defer s.dispatchTimerLock.Unlock()

	if s.dispatchTimer != nil {
		return
	}

	s.dispatchTimer = time.AfterFunc(s.options.DispatchThrottleDuration(), func() {
		s.dispatchTimerLock.Lock()
		defer s.dispatchTimerLock.Unlock()

		s.dispatchTimer = nil
		s.notifyDispatcher()
	})
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
LoopDispatch:
	for s.hasRemainingTasks() {
		if s.receiveWeightUpdateNotification() {
			s.Lock()
			s.updateChannelWeightLocked()
			s.flattenWeightedChannelsLocked()
			s.Unlock()
		}

		iwrrChannels := s.channels()
		rateLimiterConfig := s.getRateLimiterConfig()
		s.doDispatchTasksWithWeight(iwrrChannels, rateLimiterConfig)

		if !rateLimiterConfig.enableThrottle {
			continue LoopDispatch
		}

		// rate limiter throttled enabled
		// we only want to perform next round of dispatch if there are tasks in non-throttled channel.
		//
		// all channels = throttled channels + not throttled but has more task + not throttled and no more task
		// - If there's channel that's not throttled and has more task, need to trigger next round
		//   of dispatch immediately.
		// - Otherwise all channels = throttled channels + not throttled and no more task
		//   then as long as there's throttled channel, need to set a timer to try dispatch later

		numThrottled := 0
		for _, channel := range iwrrChannels.channels {
			if channel.throttled {
				numThrottled++
				continue
			}
			if len(channel.Chan()) > 0 {
				// there's channel that is not throttled and may have more tasks
				// start a new round of dispatch immediately
				continue LoopDispatch
			}
		}

		if numThrottled != 0 {
			s.setupDispatchTimer()
		}

		return
	}
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) doDispatchTasksWithWeight(
	iwrrChannels iwrrChannels[T, K],
	rateLimiterConfig rateLimiterConfig,
) {
	for _, channel := range iwrrChannels.channels {
		channel.throttled = false
	}

	rateLimiter := quotas.NoopRequestRateLimiter
	if rateLimiterConfig.enableThrottle || rateLimiterConfig.enableShadowMode {
		rateLimiter = s.rateLimiter
	}

	numFlattenedChannels := len(iwrrChannels.flattenedChannels)
	startIdx := rand.Intn(numFlattenedChannels)
	taskDispatched := int64(0)
	numThrottled := 0
LoopDispatch:
	for i := 0; i != numFlattenedChannels; i++ {
		channel := iwrrChannels.flattenedChannels[(startIdx+i)%numFlattenedChannels]

		if channel.throttled {
			continue LoopDispatch
		}

		if len(channel.Chan()) == 0 {
			continue LoopDispatch
		}

		if !rateLimiter.Allow(s.timeSource.Now(), channel.rateLimitRequest) {
			s.metricsHandler.Counter(metrics.TaskSchedulerThrottled.GetMetricName()).Record(1, channel.metricsTags...)

			if rateLimiterConfig.enableThrottle {
				channel.throttled = true
				numThrottled++
				if numThrottled == len(iwrrChannels.channels) {
					// all channels throttled
					break LoopDispatch
				}
				continue LoopDispatch
			}

			// throttled, but in shadow mode, do not actually throttle task dispatching
		}

		select {
		case task := <-channel.Chan():
			s.fifoScheduler.Submit(task)
			taskDispatched++
		default:
			continue LoopDispatch
		}
	}
	atomic.AddInt64(&s.numInflightTask, -taskDispatched)
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) tryDispatchTaskDirectly(
	channelKey K,
	task T,
) bool {
	rateLimiterConfig := s.getRateLimiterConfig()
	now := s.timeSource.Now()
	reservation := quotas.NoopReservation
	if rateLimiterConfig.enableThrottle || rateLimiterConfig.enableShadowMode {
		reservation = s.rateLimiter.Reserve(
			now,
			s.options.ChannelQuotaRequestFn(channelKey),
		)

		if reservation.DelayFrom(now) != 0 {
			reservation.CancelAt(now)
			s.metricsHandler.Counter(metrics.TaskSchedulerThrottled.GetMetricName()).Record(1, s.options.TaskChannelMetricTagsFn(channelKey)...)

			if rateLimiterConfig.enableThrottle {
				return false
			}

			// throttled, but in shadow mode, continue to dispatch
		}

		// not throttled, continue to dispatch
	}

	dispatched := s.fifoScheduler.TrySubmit(task)
	if dispatched {
		atomic.AddInt64(&s.numInflightTask, -1)
	} else {
		reservation.CancelAt(now)
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

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) getRateLimiterConfig() rateLimiterConfig {
	return s.rateLimiterConfig.Load().(rateLimiterConfig)
}

func (s *InterleavedWeightedRoundRobinScheduler[T, K]) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}
