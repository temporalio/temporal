package log

import (
	"sync"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"
)

const (
	// extraSkipForThrottleLogger is the number of extra stack trace frames to skip when SkipLogger is used
	extraSkipForThrottleLogger = 3
	// defaultMaxThrottledMessages bounds the number of distinct messages that get their own rate limiter
	defaultMaxThrottledMessages = 1000
)

type (
	throttledLogger struct {
		limiters *messageLimiters
		logger   Logger
	}

	// messageLimiters keeps one rate limiter per distinct log message, so that a frequently repeated
	// message cannot exhaust the budget of rarely emitted ones. Once maxMessages distinct messages are
	// tracked, all further new messages share the overflow limiter to keep memory bounded.
	messageLimiters struct {
		rps         quotas.RateFn
		maxMessages int
		overflow    quotas.RateLimiter

		mu       sync.RWMutex
		limiters map[string]quotas.RateLimiter
	}
)

var _ Logger = (*throttledLogger)(nil)

// NewThrottledLogger returns an implementation of logger that throttles the
// log messages being emitted. Each distinct message is throttled independently
// by its own token bucket rate limiter, which stops emitting that message once
// its bucket runs out of tokens. This way rarely occurring messages (e.g. errors)
// are not dropped because some other message is logged frequently.
//
// Fatal/Panic/DPanic logs are always emitted without any throttling
func NewThrottledLogger(logger Logger, rps quotas.RateFn) *throttledLogger {
	return newThrottledLogger(logger, rps, defaultMaxThrottledMessages)
}

func newThrottledLogger(logger Logger, rps quotas.RateFn, maxMessages int) *throttledLogger {
	if sl, ok := logger.(SkipLogger); ok {
		logger = sl.Skip(extraSkipForThrottleLogger)
	}

	return &throttledLogger{
		limiters: &messageLimiters{
			rps:         rps,
			maxMessages: maxMessages,
			overflow:    quotas.NewDefaultOutgoingRateLimiter(rps),
			limiters:    make(map[string]quotas.RateLimiter),
		},
		logger: logger,
	}
}

func (tl *throttledLogger) Debug(msg string, tags ...tag.Tag) {
	tl.rateLimit(msg, func() {
		tl.logger.Debug(msg, tags...)
	})
}

func (tl *throttledLogger) Info(msg string, tags ...tag.Tag) {
	tl.rateLimit(msg, func() {
		tl.logger.Info(msg, tags...)
	})
}

func (tl *throttledLogger) Warn(msg string, tags ...tag.Tag) {
	tl.rateLimit(msg, func() {
		tl.logger.Warn(msg, tags...)
	})
}

func (tl *throttledLogger) Error(msg string, tags ...tag.Tag) {
	tl.rateLimit(msg, func() {
		tl.logger.Error(msg, tags...)
	})
}

func (tl *throttledLogger) DPanic(msg string, tags ...tag.Tag) {
	// DPanic logs are always emitted without any throttling. Call bypassRateLimit
	// to maintain the same number of stack trace frames (see extraSkipForThrottleLogger)
	tl.bypassRateLimit(func() {
		tl.logger.DPanic(msg, tags...)
	})
}

func (tl *throttledLogger) Panic(msg string, tags ...tag.Tag) {
	// Panic logs are always emitted without any throttling
	tl.bypassRateLimit(func() {
		tl.logger.Panic(msg, tags...)
	})
}

func (tl *throttledLogger) Fatal(msg string, tags ...tag.Tag) {
	// Fatal logs are always emitted without any throttling
	tl.bypassRateLimit(func() {
		tl.logger.Fatal(msg, tags...)
	})
}

// Return a logger with the specified key-value pairs set, to be included in a subsequent normal logging call
func (tl *throttledLogger) With(tags ...tag.Tag) Logger {
	result := &throttledLogger{
		limiters: tl.limiters,
		logger:   With(tl.logger, tags...),
	}
	return result
}

func (tl *throttledLogger) rateLimit(msg string, f func()) {
	if tl.limiters.limiter(msg).Allow() {
		f()
	}
}

func (ml *messageLimiters) limiter(msg string) quotas.RateLimiter {
	ml.mu.RLock()
	limiter, ok := ml.limiters[msg]
	ml.mu.RUnlock()
	if ok {
		return limiter
	}

	ml.mu.Lock()
	defer ml.mu.Unlock()
	if limiter, ok := ml.limiters[msg]; ok {
		return limiter
	}
	if len(ml.limiters) >= ml.maxMessages {
		return ml.overflow
	}
	limiter = quotas.NewDefaultOutgoingRateLimiter(ml.rps)
	ml.limiters[msg] = limiter
	return limiter
}

// bypassRateLimit bypasses the rate limit and calls the function directly. It exists to maintain the same
// number of stack trace frames as when rateLimit is called (see extraSkipForThrottleLogger)
func (tl *throttledLogger) bypassRateLimit(f func()) {
	f()
}
