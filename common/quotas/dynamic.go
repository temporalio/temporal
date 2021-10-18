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

	// BurstFn returns an int as the burst / bucket size
	BurstFn func() int

	// RateBurstFn returns rate & burst for rate limiter
	RateBurstFn interface {
		Rate() float64
		Burst() int
	}

	RateBurstFnImpl struct {
		rateFn  RateFn
		burstFn BurstFn
	}

	// DynamicRateBurstImpl stores the dynamic rate & burst for rate limiter
	DynamicRateBurstImpl struct {
		rate  *atomic.Float64
		burst *atomic.Int64
	}

	DynamicRateBurst interface {
		LoadRate() float64
		StoreRate(rate float64)
		LoadBurst() int
		StoreBurst(burst int)
		RateBurstFn
	}
)

func NewRateBurstFn(
	rateFn RateFn,
	burstFn BurstFn,
) *RateBurstFnImpl {
	return &RateBurstFnImpl{
		rateFn:  rateFn,
		burstFn: burstFn,
	}
}

func NewDefaultIncomingRateBurstFn(
	rateFn RateFn,
) *RateBurstFnImpl {
	return newDefaultRateBurstFn(rateFn, defaultIncomingRateBurstRatio)
}

func NewDefaultOutgoingRateBurstFn(
	rateFn RateFn,
) *RateBurstFnImpl {
	return newDefaultRateBurstFn(rateFn, defaultOutgoingRateBurstRatio)
}

func newDefaultRateBurstFn(
	rateFn RateFn,
	rateToBurstRatio float64,
) *RateBurstFnImpl {
	burstFn := func() int {
		rate := rateFn()
		if rate < 0 {
			rate = 0
		}
		if rateToBurstRatio < 0 {
			rateToBurstRatio = 0
		}
		burst := int(rate * rateToBurstRatio)
		if burst == 0 && rate > 0 && rateToBurstRatio > 0 {
			burst = 1
		}
		return burst
	}
	return NewRateBurstFn(rateFn, burstFn)
}

func (d *RateBurstFnImpl) Rate() float64 {
	return d.rateFn()
}

func (d *RateBurstFnImpl) Burst() int {
	return d.burstFn()
}

func NewDynamicRateBurst(
	rate float64,
	burst int,
) *DynamicRateBurstImpl {
	return &DynamicRateBurstImpl{
		rate:  atomic.NewFloat64(rate),
		burst: atomic.NewInt64(int64(burst)),
	}
}

func (d *DynamicRateBurstImpl) LoadRate() float64 {
	return d.rate.Load()
}

func (d *DynamicRateBurstImpl) StoreRate(rate float64) {
	d.rate.Store(rate)
}

func (d *DynamicRateBurstImpl) LoadBurst() int {
	return int(d.burst.Load())
}

func (d *DynamicRateBurstImpl) StoreBurst(burst int) {
	d.burst.Store(int64(burst))
}

func (d *DynamicRateBurstImpl) Rate() float64 {
	return d.LoadRate()
}

func (d *DynamicRateBurstImpl) Burst() int {
	return d.LoadBurst()
}
