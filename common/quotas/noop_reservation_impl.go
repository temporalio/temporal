package quotas

import (
	"time"
)

type (
	NoopReservationImpl struct{}
)

var _ Reservation = (*NoopReservationImpl)(nil)

var NoopReservation Reservation = &NoopReservationImpl{}

// OK returns whether the limiter can provide the requested number of tokens
func (r *NoopReservationImpl) OK() bool {
	return true
}

// Cancel indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *NoopReservationImpl) Cancel() {
	// noop
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *NoopReservationImpl) CancelAt(_ time.Time) {
	// noop
}

// Delay returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *NoopReservationImpl) Delay() time.Duration {
	return time.Duration(0) // no delay
}

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *NoopReservationImpl) DelayFrom(_ time.Time) time.Duration {
	return time.Duration(0) // no delay
}
