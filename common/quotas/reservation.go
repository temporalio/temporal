package quotas

import (
	"time"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination reservation_mock.go

type (
	// Reservation holds information about events that are permitted by a Limiter to happen after a delay
	Reservation interface {
		// OK returns whether the limiter can provide the requested number of tokens
		OK() bool

		// Cancel indicates that the reservation holder will not perform the reserved action
		// and reverses the effects of this Reservation on the rate limit as much as possible
		Cancel()

		// CancelAt indicates that the reservation holder will not perform the reserved action
		// and reverses the effects of this Reservation on the rate limit as much as possible
		CancelAt(now time.Time)

		// Delay returns the duration for which the reservation holder must wait
		// before taking the reserved action.  Zero duration means act immediately.
		Delay() time.Duration

		// DelayFrom returns the duration for which the reservation holder must wait
		// before taking the reserved action.  Zero duration means act immediately.
		DelayFrom(now time.Time) time.Duration
	}
)
