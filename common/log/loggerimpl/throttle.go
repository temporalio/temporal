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

package loggerimpl

import (
	"sync/atomic"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type throttledLogger struct {
	rps     int32
	limiter quotas.Limiter
	log     log.Logger
}

var _ log.Logger = (*throttledLogger)(nil)

const skipForThrottleLogger = 6

// NewThrottledLogger returns an implementation of logger that throttles the
// log messages being emitted. The underlying implementation uses a token bucket
// ratelimiter and stops emitting logs once the bucket runs out of tokens
//
// Fatal/Panic logs are always emitted without any throttling
func NewThrottledLogger(logger log.Logger, rps dynamicconfig.IntPropertyFn) log.Logger {
	var log log.Logger
	lg, ok := logger.(*loggerImpl)
	if ok {
		log = &loggerImpl{
			zapLogger: lg.zapLogger,
			skip:      skipForThrottleLogger,
		}
	} else {
		logger.Warn("ReplayLogger may not emit callat tag correctly because the logger passed in is not loggerImpl")
		log = logger
	}

	rate := rps()
	limiter := quotas.NewDynamicRateLimiter(func() float64 {
		return float64(rps())
	})
	tl := &throttledLogger{
		limiter: limiter,
		rps:     int32(rate),
		log:     log,
	}
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
func (tl *throttledLogger) WithTags(tags ...tag.Tag) log.Logger {
	result := &throttledLogger{
		rps:     atomic.LoadInt32(&tl.rps),
		limiter: tl.limiter,
		log:     tl.log.WithTags(tags...),
	}
	return result
}

func (tl *throttledLogger) rateLimit(f func()) {
	if ok := tl.limiter.Allow(); ok {
		f()
	}
}
