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
