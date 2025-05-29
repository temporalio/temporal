package quotas

import (
	"context"
	"fmt"
	"math"
	"time"

	"golang.org/x/time/rate"
)

const (
	InfDuration = rate.InfDuration
)

type (
	// MultiRateLimiterImpl is a wrapper around the limiter interface
	MultiRateLimiterImpl struct {
		rateLimiters []RateLimiter
		recycleCh    chan struct{}
	}
)

var _ RateLimiter = (*MultiRateLimiterImpl)(nil)

// NewMultiRateLimiter returns a new rate limiter that have multiple stage
func NewMultiRateLimiter(
	rateLimiters []RateLimiter,
) *MultiRateLimiterImpl {
	if len(rateLimiters) == 0 {
		panic("expect at least one rate limiter")
	}
	return &MultiRateLimiterImpl{
		rateLimiters: rateLimiters,
		recycleCh:    make(chan struct{}),
	}
}

// Allow immediately returns with true or false indicating if a rate limit
// token is available or not
func (rl *MultiRateLimiterImpl) Allow() bool {
	return rl.AllowN(time.Now(), 1)
}

// AllowN immediately returns with true or false indicating if n rate limit
// token is available or not
func (rl *MultiRateLimiterImpl) AllowN(now time.Time, numToken int) bool {
	length := len(rl.rateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, rateLimiter := range rl.rateLimiters {
		reservation := rateLimiter.ReserveN(now, numToken)
		if !reservation.OK() || reservation.DelayFrom(now) > 0 {
			if reservation.OK() {
				reservation.CancelAt(now)
			}

			// cancel all existing reservation
			for _, reservation := range reservations {
				reservation.CancelAt(now)
			}
			return false
		}
		reservations = append(reservations, reservation)
	}

	return true
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (rl *MultiRateLimiterImpl) Reserve() Reservation {
	return rl.ReserveN(time.Now(), 1)
}

// ReserveN calls ReserveN on its list of rate limiters and returns a MultiReservation that is a list of the
// individual reservation objects indicating how long the caller must wait before the event can happen.
func (rl *MultiRateLimiterImpl) ReserveN(now time.Time, numToken int) Reservation {
	length := len(rl.rateLimiters)
	reservations := make([]Reservation, 0, length)

	for _, rateLimiter := range rl.rateLimiters {
		reservation := rateLimiter.ReserveN(now, numToken)
		if !reservation.OK() {
			// cancel all existing reservation
			for _, reservation := range reservations {
				reservation.CancelAt(now)
			}
			return NewMultiReservation(false, nil)
		}
		reservations = append(reservations, reservation)
	}

	return NewMultiReservation(true, reservations)
}

// Wait waits up till maximum deadline for a rate limit token
func (rl *MultiRateLimiterImpl) Wait(ctx context.Context) error {
	return rl.WaitN(ctx, 1)
}

// WaitN waits up till maximum deadline for n rate limit tokens
func (rl *MultiRateLimiterImpl) WaitN(ctx context.Context, numToken int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := time.Now().UTC()
	reservation := rl.ReserveN(now, numToken)
	if !reservation.OK() {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", numToken)
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
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", numToken)
	}

	t := time.NewTimer(delay)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			return nil
		case <-ctx.Done():
			reservation.CancelAt(time.Now())
			return ctx.Err()
		case <-rl.recycleCh:
			if numToken > 1 {
				break // recycling 1 token to a process requesting >1 tokens is a no-op, because we only know that at least one token was not used
			}

			// Cancel() reverses the effects of this Reservation on the rate limit as much as possible,
			// considering that other reservations may have already been made. Normally, Cancel() indicates
			// that the reservation holder will not perform the reserved action, so it would make the most
			// sense to cancel the reservation whose token was just recycled. However, we don't have access
			// to the recycled reservation anymore, and even if we did, Cancel on a reservation that
			// has fully waited is a no-op, so instead we cancel the current reservation as a proxy.
			//
			// Since Cancel() just restores tokens to the rate limiter, cancelling the current 1-token
			// reservation should have approximately the same effect on the actual rate as cancelling the
			// recycled reservation.
			//
			// If the recycled reservation was for >1 token, cancelling the current 1-token reservation will
			// lead to a slower actual rate than cancelling the original, so the approximation is conservative.
			reservation.Cancel()
			return nil
		}
	}
}

// Rate returns the minimum rate per second for this rate limiter
func (rl *MultiRateLimiterImpl) Rate() float64 {
	result := rl.rateLimiters[0].Rate()
	for _, rateLimiter := range rl.rateLimiters {
		newRate := rateLimiter.Rate()
		if result > newRate {
			result = newRate
		}
	}
	return result
}

// Burst returns the minimum burst for this rate limiter
func (rl *MultiRateLimiterImpl) Burst() int {
	result := rl.rateLimiters[0].Burst()
	for _, rateLimiter := range rl.rateLimiters {
		newBurst := rateLimiter.Burst()
		if result > newBurst {
			result = newBurst
		}
	}
	return result
}

func (rl *MultiRateLimiterImpl) TokensAt(t time.Time) int {
	tokens := math.MaxInt
	for _, rateLimiter := range rl.rateLimiters {
		tokens = min(tokens, rateLimiter.TokensAt(t))
	}
	return tokens
}

// RecycleToken should be called when the action being rate limited was not completed
// for some reason (i.e. a task is not dispatched because it was invalid).
// In this case, we want to immediately unblock another process that is waiting for one token
// so that the actual rate of completed actions is as close to the intended rate limit as possible.
// If no process is waiting for a token when RecycleToken is called, this is a no-op.
//
// For most rate limiters, recycle token is implemented in the base level ClockedRateLimiter,
// but because MultiRateLimiter implements WaitN by calling ReserveN on all its sub-rate-limiters,
// instead of WaitN, there is no one waiting on ClockedRateLimiter's recycle token channel.
// So MultiRateLimiter needs its own recycle method and channel.
//
// Since we don't know how many tokens were reserved by the process calling recycle, we will only unblock
// new reservations that are for one token (otherwise we could recycle a 1-token-reservation and unblock
// a 100-token-reservation). If all waiting processes are waiting for >1 tokens, this is a no-op.
//
// Because recycleCh is an unbuffered channel, the token will be reused for the next waiter only if
// there exists a waiter at the time RecycleToken is called. Usually the attempted rate is consistently
// above or below the limit for a period of time, so if rate limiting is in effect and recycling matters,
// most likely there will be a waiter. If the actual rate is erratically bouncing to either side of the
// rate limit AND we perform many recycles, this will drop some recycled tokens.
// If that situation turns out to be common, we may want to make it a buffered channel instead.
//
// Our goal is to ensure that each token in our bucket is used every second, meaning the time between
// taking and successfully using a token must be <= 1s. For this to be true, we must have:
//
//	time_to_recycle * number_of_recycles_per_second <= 1s
//	time_to_recycle * probability_of_recycle * number_of_attempts_per_second <= 1s
//
// Therefore, it is also possible for this strategy to be inaccurate if the delay between taking and
// successfully using a token is greater than one second.
//
// Currently, RecycleToken is called when we take a token to attempt a matching task dispatch and
// then later find out (usually via RPC to History) that the task should not be dispatched.
// If history rpc takes 10ms --> 100 opportunities for the token to be used that second --> 99% recycle probability is ok.
// If recycle probability is 50% --> need at least 2 opportunities for token to be used --> 500ms history rpc time is ok.
// Note that the task forwarder rate limit impacts the rate of recycle, which can add inaccuracy.
func (rl *MultiRateLimiterImpl) RecycleToken() {
	select {
	case rl.recycleCh <- struct{}{}:
	default:
	}
}
