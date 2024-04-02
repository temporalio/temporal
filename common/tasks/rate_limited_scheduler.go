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
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

type (
	QuotaRequestFn[T Task] func(T) quotas.Request
	MetricTagsFn[T Task]   func(T) []metrics.Tag

	RateLimitedSchedulerOptions struct {
		EnableShadowMode bool
	}

	RateLimitedScheduler[T Task] struct {
		scheduler Scheduler[T]

		rateLimiter    quotas.RequestRateLimiter
		timeSource     clock.TimeSource
		quotaRequestFn QuotaRequestFn[T]
		metricTagsFn   MetricTagsFn[T]
		options        RateLimitedSchedulerOptions

		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

func NewRateLimitedScheduler[T Task](
	scheduler Scheduler[T],
	rateLimiter quotas.RequestRateLimiter,
	timeSource clock.TimeSource,
	quotaRequestFn QuotaRequestFn[T],
	metricTagsFn MetricTagsFn[T],
	options RateLimitedSchedulerOptions,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *RateLimitedScheduler[T] {
	return &RateLimitedScheduler[T]{
		scheduler:      scheduler,
		rateLimiter:    rateLimiter,
		timeSource:     timeSource,
		quotaRequestFn: quotaRequestFn,
		metricTagsFn:   metricTagsFn,
		options:        options,
		logger:         logger,
		metricsHandler: metricsHandler,
	}
}

func (s *RateLimitedScheduler[T]) Submit(task T) {
	s.wait(task)
	s.scheduler.Submit(task)
}

func (s *RateLimitedScheduler[T]) TrySubmit(task T) bool {
	if !s.allow(task) {
		return false
	}

	// NOTE: if submission to the underlying scheduler fails, we are not cancelling the request token here.
	// Because when that happens, the underlying scheduler is already busy and overloaded.
	// There's no point in cancelling the token, which allows more tasks to be submitted the underlying scheduler.
	return s.scheduler.TrySubmit(task)
}

func (s *RateLimitedScheduler[T]) Start() {
	s.scheduler.Start()
}

func (s *RateLimitedScheduler[T]) Stop() {
	s.scheduler.Stop()
}

func (s *RateLimitedScheduler[T]) wait(task T) {
	now := s.timeSource.Now()
	reservation := s.rateLimiter.Reserve(
		s.timeSource.Now(),
		s.quotaRequestFn(task),
	)
	if !reservation.OK() {
		s.logger.Error("unable to make reservation in rateLimitedScheduler, skip rate limiting",
			tag.Key("quota-request"),
			tag.Value(s.quotaRequestFn(task)),
		)
		return
	}

	delay := reservation.DelayFrom(now)
	if delay <= 0 {
		return
	}

	metrics.TaskSchedulerThrottled.With(s.metricsHandler).Record(1, s.metricTagsFn(task)...)
	if s.options.EnableShadowMode {
		// in shadow mode, only emit metrics, but don't actually throttle
		return
	}

	<-time.NewTimer(delay).C
}

func (s *RateLimitedScheduler[T]) allow(task T) bool {
	if allow := s.rateLimiter.Allow(
		s.timeSource.Now(),
		s.quotaRequestFn(task),
	); allow {
		return true
	}

	metrics.TaskSchedulerThrottled.With(s.metricsHandler).Record(1, s.metricTagsFn(task)...)

	// in shadow mode, only emit metrics, but don't actually throttle
	return s.options.EnableShadowMode
}
