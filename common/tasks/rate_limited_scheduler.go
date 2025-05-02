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
