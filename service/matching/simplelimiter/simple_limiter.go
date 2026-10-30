package simplelimiter

import (
	"math"
	"time"
)

// Ready and Params implement a "GCRA" limiter.
// A Ready is "ready" if its value is <= now (as unix nanos).
type Ready int64 // ready time as unix nanos

type Params struct {
	Interval time.Duration // ideal task spacing interval, or 0 for no limit (infinite), or -1 for zero limit
	Burst    time.Duration // burst duration
}

// MaxBurst is the maximum supported burst duration. MakeParams clips the burst duration to this.
const MaxBurst = time.Minute

// never is a value of Ready (timestamp in unix nanos) that will never happen, but that won't
// overflow when we do math with it (unlike math.MaxInt64).
const never = Ready(7 << 60) // this is in the year 2225

// NoLimit returns Params that correspond to an unlimited rate limiter.
func NoLimit() Params {
	return Params{}
}

// MakeParams returns Params for the given rate and burst duration.
func MakeParams(rate float64, burstDuration time.Duration) Params {
	// 1e-9 would make interval overflow int64
	if rate <= 1e-9 {
		return Params{
			Interval: time.Duration(-1),
		}
	}
	return Params{
		Interval: time.Duration(float64(time.Second) / rate),
		Burst:    min(burstDuration, MaxBurst),
	}
}

// Never returns true if the params will never allow a token.
func (p Params) Never() bool { return p.Interval < 0 }

// Limited returns true if there is any limit at all.
func (p Params) Limited() bool { return p.Interval > 0 }

// DivideInterval returns new Params that limit the rate to a fraction of the original rate.
func (p Params) DivideInterval(by float32) Params {
	return Params{
		Interval: time.Duration(float32(p.Interval) / by),
		Burst:    p.Burst,
	}
}

// Delay returns the time until the limiter is ready.
// If the return value is <= 0 then the limiter can go now.
func (ready Ready) Delay(now int64) time.Duration {
	return time.Duration(int64(ready) - now)
}

// Consume updates ready based on the current time and number of new tokens consumed.
// Note the result has to be assigned back to the state.
func (ready Ready) Consume(p Params, now int64, tokens int64) Ready {
	// This is a slight variation of the normal GCRA: instead of tracking the end of the
	// allowed interval (the theoretical arrival time), ready tracks the beginning of it, and
	// the end is ready + burst. To find the next ready time:
	// - Add ready+burst to find the next theoretical arrival time.
	// - If that's in the past, clip it at the current time.
	// - Subtract burst to turn it back into a ready time.
	// - Finally add the tokens we used.
	//
	// For intuition, consider that if if now is > ready by only a tiny amount, i.e. we're
	// bursting, then the max takes ready+burst and we push up the ready time by the full
	// interval. We can do this burst/interval times before it catches up and we're no longer
	// ready.
	//
	// Alternatively, if now is > ready by more than burst, then we end up subtracting the full
	// burst from now and adding one interval.
	if p.Never() {
		return never
	}
	clippedReady := max(now, int64(ready)+p.Burst.Nanoseconds()) - p.Burst.Nanoseconds()
	return Ready(clippedReady + tokens*p.Interval.Nanoseconds())
}

// Clip updates ready to an allowable range based on the given parameters.
// Note the result has to be assigned back to the state.
func (ready Ready) Clip(p Params, now int64, maxTokens int64) Ready {
	if p.Never() {
		return never
	}
	// If ready was set very far in the future (e.g. because the rate was zero), then we can
	// clip it back to now + maxTokens*interval + burst.
	maxDelay := maxTokens*p.Interval.Nanoseconds() + p.Burst.Nanoseconds()
	return min(ready, Ready(now+maxDelay))
}

// Available returns the number of tokens that are ready to be consumed now.
func (ready Ready) Available(params Params, now int64) int32 {
	if params.Never() || ready.Delay(now) > 0 {
		return 0
	} else if !params.Limited() {
		return math.MaxInt32
	}
	clippedReady := max(now, int64(ready)+params.Burst.Nanoseconds()) - params.Burst.Nanoseconds()
	return int32(min((now-clippedReady)/params.Interval.Nanoseconds()+1, math.MaxInt32))
}
