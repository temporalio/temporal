package quotas

import (
	"context"
	"time"
)

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination request_rate_limiter_mock.go

type (
	// RequestRateLimiterFn returns generate a namespace specific rate limiter
	RequestRateLimiterFn func(req Request) RequestRateLimiter

	// RequestPriorityFn returns a priority for the given Request
	RequestPriorityFn func(req Request) int

	// RequestRateLimiter corresponds to basic rate limiting functionality.
	RequestRateLimiter interface {
		// Allow attempts to allow a request to go through. The method returns
		// immediately with a true or false indicating if the request can make
		// progress
		Allow(now time.Time, request Request) bool

		// Reserve returns a Reservation that indicates how long the caller
		// must wait before event happen.
		Reserve(now time.Time, request Request) Reservation

		// Wait waits till the deadline for a rate limit token to allow the request
		// to go through.
		Wait(ctx context.Context, request Request) error
	}
)
