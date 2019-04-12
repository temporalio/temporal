// Copyright (c) 2017 Uber Technologies, Inc.
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

package log

import (
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/tokenbucket"
)

type throttledLogger struct {
	rps int32
	tb  tokenbucket.TokenBucket
	log *loggerImpl
	cfg struct {
		rps dynamicconfig.IntPropertyFn
	}
}

var _ Logger = (*throttledLogger)(nil)

const skipForThrottleLogger = 6

// NewThrottledLogger returns an implementation of logger that throttles the
// log messages being emitted. The underlying implementation uses a token bucket
// ratelimiter and stops emitting logs once the bucket runs out of tokens
//
// Fatal/Panic logs are always emitted without any throttling
func NewThrottledLogger(zapLogger *zap.Logger, rps dynamicconfig.IntPropertyFn) Logger {
	rate := rps()
	log := &loggerImpl{
		zapLogger: zapLogger,
		skip:      skipForThrottleLogger,
	}
	tb := tokenbucket.New(rate, clock.NewRealTimeSource())
	tl := &throttledLogger{
		tb:  tb,
		rps: int32(rate),
		log: log,
	}
	tl.cfg.rps = rps
	return tl
}

func (tl *throttledLogger) Debug(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Debug(msg, tags...)
	})
}

func (tl *throttledLogger) Info(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Info(msg, tags...)
	})
}

func (tl *throttledLogger) Warn(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Warn(msg, tags...)
	})
}

func (tl *throttledLogger) Error(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Error(msg, tags...)
	})
}

func (tl *throttledLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.rateLimit(func() {
		tl.log.Fatal(msg, tags...)
	})
}

// Return a logger with the specified key-value pairs set, to be included in a subsequent normal logging call
func (tl *throttledLogger) WithTags(tags ...tag.Tag) Logger {
	fields := tl.log.buildFields(tags)
	zapLogger := tl.log.zapLogger.With(fields...)

	result := &throttledLogger{
		rps: atomic.LoadInt32(&tl.rps),
		tb:  tl.tb,
		log: &loggerImpl{
			zapLogger: zapLogger,
			skip:      skipForThrottleLogger,
		},
	}
	result.cfg.rps = tl.cfg.rps
	return result
}

func (tl *throttledLogger) rateLimit(f func()) {
	tl.resetRateIfChanged()
	ok, _ := tl.tb.TryConsume(1)
	if ok {
		f()
	}
}

// resetLimitIfChanged resets the underlying token bucket if the
// current rps quota is different from the actual rps quota obtained
// from dynamic config
func (tl *throttledLogger) resetRateIfChanged() {
	curr := atomic.LoadInt32(&tl.rps)
	actual := tl.cfg.rps()
	if int(curr) == actual {
		return
	}
	if atomic.CompareAndSwapInt32(&tl.rps, curr, int32(actual)) {
		tl.tb.Reset(actual)
	}
}
