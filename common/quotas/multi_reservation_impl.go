package quotas

import (
	"time"
)

type (
	MultiReservationImpl struct {
		ok           bool
		reservations []Reservation
	}
)

var _ Reservation = (*MultiReservationImpl)(nil)

func NewMultiReservation(
	ok bool,
	reservations []Reservation,
) *MultiReservationImpl {
	if ok && len(reservations) == 0 {
		panic("expect at least one reservation")
	}
	return &MultiReservationImpl{
		ok:           ok,
		reservations: reservations,
	}
}

// OK returns whether the limiter can provide the requested number of tokens
func (r *MultiReservationImpl) OK() bool {
	return r.ok
}

// Cancel indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *MultiReservationImpl) Cancel() {
	r.CancelAt(time.Now())
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *MultiReservationImpl) CancelAt(now time.Time) {
	if !r.ok {
		return
	}

	for _, reservation := range r.reservations {
		reservation.CancelAt(now)
	}
}

// Delay returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *MultiReservationImpl) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
// MultiReservation DelayFrom returns the maximum delay of all its sub-reservations.
func (r *MultiReservationImpl) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}

	result := r.reservations[0].DelayFrom(now)
	for _, reservation := range r.reservations {
		duration := reservation.DelayFrom(now)
		if result < duration {
			result = duration
		}
	}
	return result
}
