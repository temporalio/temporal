package quotas

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/clock"
	"golang.org/x/time/rate"
)

// ClockedRateLimiter wraps a rate.Limiter with a clock.TimeSource. It is used to ensure that the rate limiter respects
// the time determined by the timeSource.
type ClockedRateLimiter struct {
	rateLimiter *rate.Limiter
	timeSource  clock.TimeSource
	recycleCh   chan struct{}
}

var (
	ErrRateLimiterWaitInterrupted                       = errors.New("rate limiter wait interrupted")
	ErrRateLimiterReservationCannotBeMade               = errors.New("rate limiter reservation cannot be made due to insufficient quota")
	ErrRateLimiterReservationWouldExceedContextDeadline = errors.New("rate limiter reservation would exceed context deadline")
)

func NewClockedRateLimiter(rateLimiter *rate.Limiter, timeSource clock.TimeSource) ClockedRateLimiter {
	return ClockedRateLimiter{
		rateLimiter: rateLimiter,
		timeSource:  timeSource,
		recycleCh:   make(chan struct{}),
	}
}

func (l ClockedRateLimiter) Allow() bool {
	return l.AllowN(l.timeSource.Now(), 1)
}

func (l ClockedRateLimiter) AllowN(now time.Time, token int) bool {
	return l.rateLimiter.AllowN(now, token)
}

// ClockedReservation wraps a rate.Reservation with a clockwork.Clock. It is used to ensure that the reservation
// respects the time determined by the timeSource.
type ClockedReservation struct {
	reservation *rate.Reservation
	timeSource  clock.TimeSource
}

func (r ClockedReservation) OK() bool {
	return r.reservation.OK()
}

func (r ClockedReservation) Delay() time.Duration {
	return r.DelayFrom(r.timeSource.Now())
}

func (r ClockedReservation) DelayFrom(t time.Time) time.Duration {
	return r.reservation.DelayFrom(t)
}

func (r ClockedReservation) Cancel() {
	r.CancelAt(r.timeSource.Now())
}

func (r ClockedReservation) CancelAt(t time.Time) {
	r.reservation.CancelAt(t)
}

func (l ClockedRateLimiter) Reserve() ClockedReservation {
	return l.ReserveN(l.timeSource.Now(), 1)
}

func (l ClockedRateLimiter) ReserveN(now time.Time, token int) ClockedReservation {
	reservation := l.rateLimiter.ReserveN(now, token)
	return ClockedReservation{reservation, l.timeSource}
}

func (l ClockedRateLimiter) Wait(ctx context.Context) error {
	return l.WaitN(ctx, 1)
}

// WaitN is the only method that is different from rate.Limiter. We need to fully reimplement this method because
// the original method uses time.Now(), and does not allow us to pass in a time.Time. Fortunately, it can be built on
// top of ReserveN. However, there are some optimizations that we can make.
func (l ClockedRateLimiter) WaitN(ctx context.Context, token int) error {
	reservation := ClockedReservation{l.rateLimiter.ReserveN(l.timeSource.Now(), token), l.timeSource}
	if !reservation.OK() {
		return fmt.Errorf("%w: WaitN(n=%d)", ErrRateLimiterReservationCannotBeMade, token)
	}

	waitDuration := reservation.Delay()

	// Optimization: if the waitDuration is 0, we don't need to start a timer.
	if waitDuration <= 0 {
		return nil
	}

	// Optimization: if the waitDuration is longer than the context deadline, we can immediately return an error.
	if deadline, ok := ctx.Deadline(); ok {
		if l.timeSource.Now().Add(waitDuration).After(deadline) {
			reservation.Cancel()
			return fmt.Errorf("%w: WaitN(n=%d)", ErrRateLimiterReservationWouldExceedContextDeadline, token)
		}
	}

	waitExpired := make(chan struct{})
	timer := l.timeSource.AfterFunc(waitDuration, func() {
		close(waitExpired)
	})
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			reservation.Cancel()
			return fmt.Errorf("%w: %v", ErrRateLimiterWaitInterrupted, ctx.Err())
		case <-waitExpired:
			return nil
		case <-l.recycleCh:
			if token > 1 {
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

func (l ClockedRateLimiter) SetLimitAt(t time.Time, newLimit rate.Limit) {
	l.rateLimiter.SetLimitAt(t, newLimit)
}

func (l ClockedRateLimiter) SetBurstAt(t time.Time, newBurst int) {
	l.rateLimiter.SetBurstAt(t, newBurst)
}

func (l ClockedRateLimiter) TokensAt(t time.Time) int {
	return int(l.rateLimiter.TokensAt(t))
}

// RecycleToken should be called when the action being rate limited was not completed
// for some reason (i.e. a task is not dispatched because it was invalid).
// In this case, we want to immediately unblock another process that is waiting for one token
// so that the actual rate of completed actions is as close to the intended rate limit as possible.
// If no process is waiting for a token when RecycleToken is called, this is a no-op.
func (l ClockedRateLimiter) RecycleToken() {
	select {
	case l.recycleCh <- struct{}{}:
	default:
	}
}
