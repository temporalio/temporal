package log

import (
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"
)

const extraSkipForThrottleLogger = 3

type throttledLogger struct {
	limiter quotas.RateLimiter
	logger  Logger
}

var _ Logger = (*throttledLogger)(nil)

// NewThrottledLogger returns an implementation of logger that throttles the
// log messages being emitted. The underlying implementation uses a token bucket
// rate limiter and stops emitting logs once the bucket runs out of tokens
//
// Fatal/Panic logs are always emitted without any throttling
func NewThrottledLogger(logger Logger, rps quotas.RateFn) *throttledLogger {
	if sl, ok := logger.(SkipLogger); ok {
		logger = sl.Skip(extraSkipForThrottleLogger)
	}

	limiter := quotas.NewDefaultOutgoingRateLimiter(rps)
	tl := &throttledLogger{
		limiter: limiter,
		logger:  logger,
	}
	return tl
}

func (tl *throttledLogger) Debug(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.logger.Debug(msg, tags...)
	})
}

func (tl *throttledLogger) Info(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.logger.Info(msg, tags...)
	})
}

func (tl *throttledLogger) Warn(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.logger.Warn(msg, tags...)
	})
}

func (tl *throttledLogger) Error(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.logger.Error(msg, tags...)
	})
}

func (tl *throttledLogger) DPanic(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.logger.DPanic(msg, tags...)
	})
}

func (tl *throttledLogger) Panic(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.logger.Panic(msg, tags...)
	})
}

func (tl *throttledLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.logger.Fatal(msg, tags...)
	})
}

// Return a logger with the specified key-value pairs set, to be included in a subsequent normal logging call
func (tl *throttledLogger) With(tags ...tag.Tag) Logger {
	result := &throttledLogger{
		limiter: tl.limiter,
		logger:  With(tl.logger, tags...),
	}
	return result
}

func (tl *throttledLogger) rateLimit(f func()) {
	if ok := tl.limiter.Allow(); ok {
		f()
	}
}
