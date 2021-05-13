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
	"context"
	"fmt"
	"sort"
	"time"
)

type (
	// PriorityRateLimiterImpl is a wrapper around the golang rate limiter
	PriorityRateLimiterImpl struct {
		apiToPriority          map[string]int
		priorityToRateLimiters map[int]RateLimiter

		// priority value 0 means highest priority
		// sorted rate limiter from low priority value to high priority value
		priorityToIndex map[int]int
		rateLimiters    []RateLimiter
	}
)

var _ RequestRateLimiter = (*PriorityRateLimiterImpl)(nil)

// NewPriorityRateLimiter returns a new rate limiter that can handle dynamic
// configuration updates
func NewPriorityRateLimiter(
	apiToPriority map[string]int,
	priorityToRateLimiters map[int]RateLimiter,
) *PriorityRateLimiterImpl {
	priorities := make([]int, 0, len(priorityToRateLimiters))
	for priority := range priorityToRateLimiters {
		priorities = append(priorities, priority)
	}
	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i] < priorities[j]
	})
	priorityToIndex := make(map[int]int, len(priorityToRateLimiters))
	rateLimiters := make([]RateLimiter, 0, len(priorityToRateLimiters))
	for index, priority := range priorities {
		priorityToIndex[priority] = index
		rateLimiters = append(rateLimiters, priorityToRateLimiters[priority])
	}

	// sanity check priority within apiToPriority appears in priorityToRateLimiters
	for _, priority := range apiToPriority {
		if _, ok := priorityToRateLimiters[priority]; !ok {
			panic("API to priority & priority to rate limiter does not match")
		}
	}

	return &PriorityRateLimiterImpl{
		apiToPriority:          apiToPriority,
		priorityToRateLimiters: priorityToRateLimiters,

		priorityToIndex: priorityToIndex,
		rateLimiters:    rateLimiters,
	}
}

func (p *PriorityRateLimiterImpl) Allow(
	now time.Time,
	request Request,
) bool {
	decidingRateLimiter, consumeRateLimiters := p.getRateLimiters(request)

	allow := decidingRateLimiter.AllowN(now, request.Token)
	if !allow {
		return false
	}

	for _, limiter := range consumeRateLimiters {
		_ = limiter.ReserveN(now, request.Token)
	}
	return allow
}

func (p *PriorityRateLimiterImpl) Reserve(
	now time.Time,
	request Request,
) Reservation {
	decidingRateLimiter, consumeRateLimiters := p.getRateLimiters(request)

	decidingReservation := decidingRateLimiter.ReserveN(now, request.Token)
	if !decidingReservation.OK() {
		return decidingReservation
	}

	otherReservations := make([]Reservation, len(consumeRateLimiters))
	for index, limiter := range consumeRateLimiters {
		otherReservations[index] = limiter.ReserveN(now, request.Token)
	}
	return NewPriorityReservation(decidingReservation, otherReservations)
}

func (p *PriorityRateLimiterImpl) Wait(
	ctx context.Context,
	request Request,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := time.Now().UTC()
	reservation := p.Reserve(now, request)
	if !reservation.OK() {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", request.Token)
	}

	delay := reservation.DelayFrom(now)
	if delay == 0 {
		return nil
	}
	waitLimit := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(now)
	}
	if waitLimit < delay {
		reservation.CancelAt(now)
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", request.Token)
	}

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return nil

	case <-ctx.Done():
		reservation.CancelAt(time.Now())
		return ctx.Err()
	}
}

func (p *PriorityRateLimiterImpl) getRateLimiters(
	request Request,
) (RateLimiter, []RateLimiter) {
	priority, ok := p.apiToPriority[request.API]
	if !ok {
		// if API not assigned a priority use the lowest priority
		return p.rateLimiters[len(p.rateLimiters)-1], nil
	}

	rateLimiterIndex := p.priorityToIndex[priority]
	return p.rateLimiters[rateLimiterIndex], p.rateLimiters[rateLimiterIndex+1:]
}
