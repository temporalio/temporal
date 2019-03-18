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

package logging

import (
	"sync/atomic"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/tokenbucket"
)

type throttledLogger struct {
	rps int32
	tb  tokenbucket.TokenBucket
	log bark.Logger
	cfg struct {
		rps dynamicconfig.IntPropertyFn
	}
}

var _ bark.Logger = (*throttledLogger)(nil)

// NewThrottledLogger returns an implementation of bark logger that throttles the
// log messages being emitted. The underlying implementation uses a token bucket
// ratelimiter and stops emitting logs once the bucket runs out of tokens
//
// Fatal/Panic logs are always emitted without any throttling
func NewThrottledLogger(log bark.Logger, rps dynamicconfig.IntPropertyFn) bark.Logger {
	rate := rps()
	tb := tokenbucket.New(rate, clock.NewRealTimeSource())
	tl := &throttledLogger{
		tb:  tb,
		rps: int32(rate),
		log: log,
	}
	tl.cfg.rps = rps
	return tl
}

func (tl *throttledLogger) Debug(args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Debug(args)
	})
}

// Log at debug level with fmt.Printf-like formatting
func (tl *throttledLogger) Debugf(format string, args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Debugf(format, args)
	})
}

// Log at info level
func (tl *throttledLogger) Info(args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Info(args)
	})
}

// Log at info level with fmt.Printf-like formatting
func (tl *throttledLogger) Infof(format string, args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Infof(format, args)
	})
}

// Log at warning level
func (tl *throttledLogger) Warn(args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Warn(args)
	})
}

// Log at warning level with fmt.Printf-like formatting
func (tl *throttledLogger) Warnf(format string, args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Warnf(format, args)
	})
}

// Log at error level
func (tl *throttledLogger) Error(args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Error(args)
	})
}

// Log at error level with fmt.Printf-like formatting
func (tl *throttledLogger) Errorf(format string, args ...interface{}) {
	tl.rateLimit(func() {
		tl.log.Error(format, args)
	})
}

// Log at fatal level, then terminate process (irrecoverable)
func (tl *throttledLogger) Fatal(args ...interface{}) {
	tl.log.Fatal(args)
}

// Log at fatal level with fmt.Printf-like formatting, then terminate process (irrecoverable)
func (tl *throttledLogger) Fatalf(format string, args ...interface{}) {
	tl.log.Fatalf(format, args)
}

// Log at panic level, then panic (recoverable)
func (tl *throttledLogger) Panic(args ...interface{}) {
	tl.log.Panic(args)
}

// Log at panic level with fmt.Printf-like formatting, then panic (recoverable)
func (tl *throttledLogger) Panicf(format string, args ...interface{}) {
	tl.log.Panicf(format, args)
}

// Return a logger with the specified key-value pair set, to be logged in a subsequent normal logging call
func (tl *throttledLogger) WithField(key string, value interface{}) bark.Logger {
	return tl.clone(tl.log.WithField(key, value))
}

// Return a logger with the specified key-value pairs set, to be included in a subsequent normal logging call
func (tl *throttledLogger) WithFields(keyValues bark.LogFields) bark.Logger {
	return tl.clone(tl.log.WithFields(keyValues))
}

// Return a logger with the specified error set, to be included in a subsequent normal logging call
func (tl *throttledLogger) WithError(err error) bark.Logger {
	return tl.clone(tl.log.WithError(err))
}

// Return map fields associated with this logger, if any (i.e. if this logger was returned from WithField[s])
// If no fields are set, returns nil
func (tl *throttledLogger) Fields() bark.Fields {
	return tl.log.Fields()
}

func (tl *throttledLogger) clone(log bark.Logger) bark.Logger {
	result := &throttledLogger{
		rps: atomic.LoadInt32(&tl.rps),
		tb:  tl.tb,
		log: log,
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
