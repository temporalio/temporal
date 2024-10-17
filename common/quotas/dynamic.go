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
	"math"
	"sync/atomic"
)

type (
	// RateFn returns a float64 as the RPS
	RateFn func() float64

	// NamespaceRateFn returns a float64 as the RPS for the given namespace
	NamespaceRateFn func(namespace string) float64

	// BurstFn returns an int as the burst / bucket size
	BurstFn func() int

	// BurstRatioFn returns a float as the ratio of burst to rate
	BurstRatioFn func() float64

	// NamespaceBurstFn returns an int as the burst / bucket size for the given namespace
	NamespaceBurstFn func(namespace string) float64

	// RateBurst returns rate & burst for rate limiter
	RateBurst interface {
		Rate() float64
		Burst() int
	}

	RateBurstImpl struct {
		rateFn  RateFn
		burstFn BurstFn
	}

	// MutableRateBurstImpl stores the dynamic rate & burst for rate limiter
	MutableRateBurstImpl struct {
		rate  atomic.Uint64
		burst atomic.Int64
	}

	MutableRateBurst interface {
		SetRPS(rps float64)
		SetBurst(burst int)
		RateBurst
	}
)

func NewRateBurst(
	rateFn RateFn,
	burstFn BurstFn,
) *RateBurstImpl {
	return &RateBurstImpl{
		rateFn:  rateFn,
		burstFn: burstFn,
	}
}

func NewDefaultIncomingRateBurst(
	rateFn RateFn,
) *RateBurstImpl {
	return NewDefaultRateBurst(rateFn, func() float64 {
		return defaultIncomingRateBurstRatio
	})
}

func NewDefaultOutgoingRateBurst(
	rateFn RateFn,
) *RateBurstImpl {
	return NewDefaultRateBurst(rateFn, func() float64 {
		return defaultOutgoingRateBurstRatio
	})
}

func NewDefaultRateBurst(
	rateFn RateFn,
	rateToBurstRatio BurstRatioFn,
) *RateBurstImpl {
	burstFn := func() int {
		rate := rateFn()
		if rate < 0 {
			rate = 0
		}

		ratio := rateToBurstRatio()
		if ratio < 0 {
			ratio = 0
		}
		burst := int(rate * ratio)
		if burst == 0 && rate > 0 && ratio > 0 {
			burst = 1
		}
		return burst
	}
	return NewRateBurst(rateFn, burstFn)
}

func (d *RateBurstImpl) Rate() float64 {
	return d.rateFn()
}

func (d *RateBurstImpl) Burst() int {
	return d.burstFn()
}

func NewMutableRateBurst(
	rate float64,
	burst int,
) *MutableRateBurstImpl {
	d := &MutableRateBurstImpl{}
	d.SetRPS(rate)
	d.SetBurst(burst)

	return d
}

func (d *MutableRateBurstImpl) SetRPS(rate float64) {
	d.rate.Store(math.Float64bits(rate))
}

func (d *MutableRateBurstImpl) SetBurst(burst int) {
	d.burst.Store(int64(burst))
}

func (d *MutableRateBurstImpl) Rate() float64 {
	return math.Float64frombits(d.rate.Load())
}

func (d *MutableRateBurstImpl) Burst() int {
	return int(d.burst.Load())
}
