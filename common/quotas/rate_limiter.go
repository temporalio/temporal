package quotas

import (
	"context"
	"time"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination rate_limiter_mock.go

type (
	// RateLimiter corresponds to basic rate limiting functionality.
	RateLimiter interface {
		// Allow attempts to allow a request to go through. The method returns
		// immediately with a true or false indicating if the request can make
		// progress
		Allow() bool

		// AllowN attempts to allow a request to go through. The method returns
		// immediately with a true or false indicating if the request can make
		// progress
		AllowN(now time.Time, numToken int) bool

		// Reserve returns a Reservation that indicates how long the caller
		// must wait before event happen.
		Reserve() Reservation

		// ReserveN returns a Reservation that indicates how long the caller
		// must wait before event happen.
		ReserveN(now time.Time, numToken int) Reservation

		// Wait waits till the deadline for a rate limit token to allow the request
		// to go through.
		Wait(ctx context.Context) error

		// WaitN waits till the deadline for n rate limit token to allow the request
		// to go through.
		WaitN(ctx context.Context, numToken int) error

		// Rate returns the rate per second for this rate limiter
		Rate() float64

		// Burst returns the burst for this rate limiter
		Burst() int

		// TokensAt returns the number of tokens that will be available at time t
		TokensAt(t time.Time) int

		// RecycleToken immediately unblocks another process that is waiting for a token, if
		// a waiter exists. A token should be recycled when the action being rate limited was
		// not completed for some reason (i.e. a task is not dispatched because it was invalid).
		RecycleToken()
	}
)
