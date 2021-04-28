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
	"time"
)

type (
	PriorityReservationImpl struct {
		decidingReservation Reservation
		otherReservations   []Reservation
	}
)

var _ Reservation = (*PriorityReservationImpl)(nil)

func NewPriorityReservation(
	decidingReservation Reservation,
	otherReservations []Reservation,
) *PriorityReservationImpl {
	return &PriorityReservationImpl{
		decidingReservation: decidingReservation,
		otherReservations:   otherReservations,
	}
}

// OK returns whether the limiter can provide the requested number of tokens
func (r *PriorityReservationImpl) OK() bool {
	return r.decidingReservation.OK()
}

// Cancel indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *PriorityReservationImpl) Cancel() {
	r.CancelAt(time.Now())
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *PriorityReservationImpl) CancelAt(now time.Time) {
	r.decidingReservation.CancelAt(now)
	for _, reservation := range r.otherReservations {
		reservation.CancelAt(now)
	}
}

// Delay returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *PriorityReservationImpl) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *PriorityReservationImpl) DelayFrom(now time.Time) time.Duration {
	return r.decidingReservation.DelayFrom(now)
}
