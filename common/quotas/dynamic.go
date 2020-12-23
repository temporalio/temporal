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

package quotas

import (
	"go.uber.org/atomic"
)

type (
	// RateFn returns a float64 as the RPS
	RateFn func() float64

	// BurstFn returns a int as the RPS
	BurstFn func() int

	// RateKeyFn returns a float64 as the Rate for the given key
	RateKeyFn func(key string) float64

	// BurstKeyFn returns a int as the Burst for the given key
	BurstKeyFn func(key string) int

	// DynamicRateImpl stores the dynamic rate per second for rate limiter
	DynamicRateImpl struct {
		rate *atomic.Float64
	}

	// DynamicBurstImpl stores the dynamic burst for rate limiter
	DynamicBurstImpl struct {
		burst *atomic.Int64
	}

	DynamicRate interface {
		Load() float64
		Store(rate float64)
		RateFn() RateFn
	}

	DynamicBurst interface {
		Load() int
		Store(burst int)
		BurstFn() BurstFn
	}
)

func NewDynamicRate(rate float64) *DynamicRateImpl {

	return &DynamicRateImpl{
		rate: atomic.NewFloat64(rate),
	}
}

func NewDynamicBurst(burst int) *DynamicBurstImpl {

	return &DynamicBurstImpl{
		burst: atomic.NewInt64(int64(burst)),
	}
}

func (d *DynamicRateImpl) Load() float64 {
	return d.rate.Load()
}

func (d *DynamicRateImpl) Store(rate float64) {
	d.rate.Store(rate)
}

func (d *DynamicRateImpl) RateFn() RateFn {
	return d.Load
}

func (d *DynamicBurstImpl) Load() int {
	return int(d.burst.Load())
}

func (d *DynamicBurstImpl) Store(burst int) {
	d.burst.Store(int64(burst))
}

func (d *DynamicBurstImpl) BurstFn() BurstFn {
	return d.Load
}
