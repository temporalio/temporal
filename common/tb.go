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

package common

import (
	"sync"
	"time"
)

type (
	// TokenBucketFactory is an interface mainly used for injecting mock implementation of TokenBucket for unit testing
	TokenBucketFactory interface {
		CreateTokenBucket(rps int, timeSource TimeSource) TokenBucket
	}

	// TokenBucket is the interface for
	// any implememtation of a token bucket
	// rate limiter
	TokenBucket interface {
		// TryConsume attempts to take count tokens from the
		// bucket. Returns true on success, false
		// otherwise along with the duration for the next refill
		TryConsume(count int) (bool, time.Duration)
		// Consume waits up to timeout duration to take count
		// tokens from the bucket. Returns true if count
		// tokens were acquired before timeout, false
		// otherwise
		Consume(count int, timeout time.Duration) bool
	}

	tokenBucketFactoryImpl struct{}

	tokenBucketImpl struct {
		sync.Mutex
		tokens       int
		fillRate     int   // amount of tokens to add every interval
		fillInterval int64 // time between refills
		// Because we divide the per-second quota equally
		// every 100 millis, there could be a remainder when
		// the desired rate is not a multiple 10 (1second/100Millis)
		// To overcome this, we keep track of left over remainder
		// and distribute this evenly during every fillInterval
		overflowRps            int
		overflowTokens         int
		nextRefillTime         int64
		nextOverflowRefillTime int64
		timeSource             TimeSource
	}
)

const (
	millisPerSecond = 1000
	backoffInterval = int64(10 * time.Millisecond)
)

// NewTokenBucket creates and returns a
// new token bucket rate limiter that
// repelenishes the bucket every 100
// milliseconds. Thread safe.
//
// @param rps
//    Desired rate per second
//
// Golang.org has an alternative implementation
// of the rate limiter. On benchmarking, golang's
// implementation was order of magnitude slower.
// In addition, it does a lot more than what we
// need. These are the benchmarks under different
// scenarios
//
// BenchmarkTokenBucketParallel	50000000	        40.7 ns/op
// BenchmarkGolangRateParallel 	10000000	       150 ns/op
// BenchmarkTokenBucketParallel-8	20000000	       124 ns/op
// BenchmarkGolangRateParallel-8 	10000000	       208 ns/op
// BenchmarkTokenBucketParallel	50000000	        37.8 ns/op
// BenchmarkGolangRateParallel 	10000000	       153 ns/op
// BenchmarkTokenBucketParallel-8	10000000	       129 ns/op
// BenchmarkGolangRateParallel-8 	10000000	       208 ns/op
//
func NewTokenBucket(rps int, timeSource TimeSource) TokenBucket {
	tb := new(tokenBucketImpl)
	tb.timeSource = timeSource
	tb.fillInterval = int64(time.Millisecond * 100)
	tb.fillRate = (rps * 100) / millisPerSecond
	tb.overflowRps = rps - (10 * tb.fillRate)
	tb.refill(time.Now().UnixNano())
	return tb
}

// NewTokenBucketFactory creates an instance of factory used for creating TokenBucket instances
func NewTokenBucketFactory() TokenBucketFactory {
	return &tokenBucketFactoryImpl{}
}

// CreateTokenBucket creates and returns a
// new token bucket rate limiter that
// repelenishes the bucket every 100
// milliseconds. Thread safe.
func (f *tokenBucketFactoryImpl) CreateTokenBucket(rps int, timeSource TimeSource) TokenBucket {
	return NewTokenBucket(rps, timeSource)
}

func (tb *tokenBucketImpl) TryConsume(count int) (bool, time.Duration) {
	now := tb.timeSource.Now().UnixNano()
	tb.Lock()
	tb.refill(now)
	nextRefillTime := time.Duration(tb.nextRefillTime - now)
	if tb.tokens < count {
		tb.Unlock()
		return false, nextRefillTime
	}
	tb.tokens -= count
	tb.Unlock()
	return true, nextRefillTime
}

func (tb *tokenBucketImpl) Consume(count int, timeout time.Duration) bool {

	var remTime = int64(timeout)
	var expiryTime = time.Now().UnixNano() + int64(timeout)

	for {

		if ok, _ := tb.TryConsume(count); ok {
			return true
		}

		if remTime < backoffInterval {
			time.Sleep(time.Duration(remTime))
		} else {
			time.Sleep(time.Duration(backoffInterval))
		}

		now := time.Now().UnixNano()
		if now >= expiryTime {
			return false
		}

		remTime = expiryTime - now
	}
}

func (tb *tokenBucketImpl) refill(now int64) {
	tb.refillOverFlow(now)
	if tb.isRefillDue(now) {
		tb.tokens = tb.fillRate
		if tb.overflowTokens > 0 {
			tb.tokens++
			tb.overflowTokens--
		}
		tb.nextRefillTime = now + tb.fillInterval
	}
}

func (tb *tokenBucketImpl) refillOverFlow(now int64) {
	if tb.overflowRps < 1 {
		return
	}
	if tb.isOverflowRefillDue(now) {
		tb.overflowTokens = tb.overflowRps
		tb.nextOverflowRefillTime = now + int64(time.Second)
	}
}

func (tb *tokenBucketImpl) isRefillDue(now int64) bool {
	return now >= tb.nextRefillTime
}

func (tb *tokenBucketImpl) isOverflowRefillDue(now int64) bool {
	return now >= tb.nextOverflowRefillTime
}
