package quotas

import "context"

// RPSFunc returns a float64 as the RPS
type RPSFunc func() float64

// RPSKeyFunc returns a float64 as the RPS for the given key
type RPSKeyFunc func(key string) float64

// Info corresponds to information required to determine rate limits
type Info struct {
	Namespace string
}

// Limiter corresponds to basic rate limiting functionality.
type Limiter interface {
	// Allow attempts to allow a request to go through. The method returns
	// immediately with a true or false indicating if the request can make
	// progress
	Allow() bool

	// Wait waits till the deadline for a rate limit token to allow the request
	// to go through.
	Wait(ctx context.Context) error
}

// Policy corresponds to a quota policy. A policy allows implementing layered
// and more complex rate limiting functionality.
type Policy interface {
	// Allow attempts to allow a request to go through. The method returns
	// immediately with a true or false indicating if the request can make
	// progress
	Allow(info Info) bool
}
