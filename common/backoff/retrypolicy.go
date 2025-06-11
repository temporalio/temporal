package backoff

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"
)

const (
	// NoInterval represents Maximim interval
	NoInterval                      = 0
	done              time.Duration = -1
	noMaximumAttempts               = 0

	defaultBackoffCoefficient = 2.0
	defaultMaximumInterval    = 10 * time.Second
	defaultExpirationInterval = time.Minute
	defaultMaximumAttempts    = noMaximumAttempts
	defaultJitterPct          = 0
)

var (
	// DisabledRetryPolicy is a retry policy that never retries
	DisabledRetryPolicy RetryPolicy = &disabledRetryPolicyImpl{}

	// common 'globalToFile' rand instance, used in adding jitter to next interval in retry policy
	jitterRand atomic.Pointer[rand.Rand]
)

type (
	// RetryPolicy is the API which needs to be implemented by various retry policy implementations
	RetryPolicy interface {
		ComputeNextDelay(elapsedTime time.Duration, numAttempts int, err error) time.Duration
	}

	// Retrier manages the state of retry operation
	Retrier interface {
		NextBackOff(err error) time.Duration
		Reset()
	}

	// ExponentialRetryPolicy provides the implementation for retry policy using a coefficient to compute the next delay.
	// Formula used to compute the next delay is:
	// 	min(initialInterval * pow(backoffCoefficient, currentAttempt), maximumInterval)
	ExponentialRetryPolicy struct {
		initialInterval    time.Duration
		backoffCoefficient float64
		maximumInterval    time.Duration
		expirationInterval time.Duration
		maximumAttempts    int
	}

	// ErrorDependentRetryPolicy is a policy that computes the next delay time based on the error returned by the
	// operation. The delay time to use for a particular error is determined by the delayForError function.
	ErrorDependentRetryPolicy struct {
		maximumAttempts int
		jitterPct       float64
		delayForError   func(err error) time.Duration
	}

	ConstantDelayRetryPolicy struct {
		maximumAttempts int
		jitterPct       float64
		delay           time.Duration
	}

	disabledRetryPolicyImpl struct{}

	retrierImpl struct {
		policy         RetryPolicy
		timeSource     clock.TimeSource
		currentAttempt int
		startTime      time.Time
	}
)

// NewExponentialRetryPolicy returns an instance of ExponentialRetryPolicy using the provided initialInterval
func NewExponentialRetryPolicy(initialInterval time.Duration) *ExponentialRetryPolicy {
	p := &ExponentialRetryPolicy{
		initialInterval:    initialInterval,
		backoffCoefficient: defaultBackoffCoefficient,
		maximumInterval:    defaultMaximumInterval,
		expirationInterval: defaultExpirationInterval,
		maximumAttempts:    defaultMaximumAttempts,
	}

	return p
}

// NewRetrier is used for creating a new instance of Retrier
func NewRetrier(policy RetryPolicy, timeSource clock.TimeSource) Retrier {
	return &retrierImpl{
		policy:         policy,
		timeSource:     timeSource,
		startTime:      timeSource.Now(),
		currentAttempt: 1,
	}
}

// WithInitialInterval sets the initial interval used by ExponentialRetryPolicy for the very first retry
// All later retries are computed using the following formula:
// initialInterval * math.Pow(backoffCoefficient, currentAttempt)
func (p *ExponentialRetryPolicy) WithInitialInterval(initialInterval time.Duration) *ExponentialRetryPolicy {
	p.initialInterval = initialInterval
	return p
}

// WithBackoffCoefficient sets the coefficient used by ExponentialRetryPolicy to compute next delay for each retry
// All retries are computed using the following formula:
// initialInterval * math.Pow(backoffCoefficient, currentAttempt)
func (p *ExponentialRetryPolicy) WithBackoffCoefficient(backoffCoefficient float64) *ExponentialRetryPolicy {
	p.backoffCoefficient = backoffCoefficient
	return p
}

// WithMaximumInterval sets the maximum interval for each retry.
// This does *not* cause the policy to stop retrying when the interval between retries reaches the supplied duration.
// That is what WithExpirationInterval does. Instead, this prevents the interval from exceeding maximumInterval.
func (p *ExponentialRetryPolicy) WithMaximumInterval(maximumInterval time.Duration) *ExponentialRetryPolicy {
	p.maximumInterval = maximumInterval
	return p
}

// WithExpirationInterval sets the absolute expiration interval for all retries
func (p *ExponentialRetryPolicy) WithExpirationInterval(expirationInterval time.Duration) *ExponentialRetryPolicy {
	p.expirationInterval = expirationInterval
	return p
}

// WithMaximumAttempts sets the maximum number of retry attempts
func (p *ExponentialRetryPolicy) WithMaximumAttempts(maximumAttempts int) *ExponentialRetryPolicy {
	p.maximumAttempts = maximumAttempts
	return p
}

// ComputeNextDelay returns the next delay interval.  This is used by Retrier to delay calling the operation again
func (p *ExponentialRetryPolicy) ComputeNextDelay(elapsedTime time.Duration, numAttempts int, _ error) time.Duration {
	// Check to see if we ran out of maximum number of attempts
	// NOTE: if maxAttempts is X, return done when numAttempts == X, otherwise there will be attempt X+1
	if p.maximumAttempts != noMaximumAttempts && numAttempts >= p.maximumAttempts {
		return done
	}

	// Stop retrying after expiration interval is elapsed
	if p.expirationInterval != NoInterval && elapsedTime > p.expirationInterval {
		return done
	}

	nextInterval := float64(p.initialInterval) * math.Pow(p.backoffCoefficient, float64(numAttempts-1))
	// Disallow retries if initialInterval is negative or nextInterval overflows
	if nextInterval <= 0 {
		return done
	}
	if p.maximumInterval != NoInterval {
		nextInterval = math.Min(nextInterval, float64(p.maximumInterval))
	}

	if p.expirationInterval != NoInterval {
		remainingTime := float64(math.Max(0, float64(p.expirationInterval-elapsedTime)))
		nextInterval = math.Min(remainingTime, nextInterval)
	}

	// Bail out if the next interval is smaller than initial retry interval
	nextDuration := time.Duration(nextInterval)
	if nextDuration < p.initialInterval {
		return done
	}

	nextInterval = p.addJitter(nextInterval)

	return time.Duration(nextInterval)
}

func (p *ExponentialRetryPolicy) addJitter(nextInterval float64) float64 {
	// add jitter to avoid global synchronization
	jitterPortion := int(0.2 * nextInterval)
	// Prevent overflow
	if jitterPortion < 1 {
		jitterPortion = 1
	}
	nextInterval = nextInterval*0.8 + float64(getJitterRand().Intn(jitterPortion))
	return nextInterval
}

func (r *disabledRetryPolicyImpl) ComputeNextDelay(_ time.Duration, _ int, _ error) time.Duration {
	return done
}

// Reset will set the Retrier into initial state
func (r *retrierImpl) Reset() {
	r.startTime = r.timeSource.Now()
	r.currentAttempt = 1
}

// NextBackOff returns the next delay interval.  This is used by Retry to delay calling the operation again
func (r *retrierImpl) NextBackOff(err error) time.Duration {
	nextInterval := r.policy.ComputeNextDelay(r.getElapsedTime(), r.currentAttempt, err)

	// Now increment the current attempt
	r.currentAttempt++
	return nextInterval
}

func (r *retrierImpl) getElapsedTime() time.Duration {
	return r.timeSource.Now().Sub(r.startTime)
}

var _ RetryPolicy = (*ErrorDependentRetryPolicy)(nil)

func NewErrorDependentRetryPolicy(delayForError func(err error) time.Duration) *ErrorDependentRetryPolicy {
	return &ErrorDependentRetryPolicy{
		maximumAttempts: defaultMaximumAttempts,
		delayForError:   delayForError,
		jitterPct:       defaultJitterPct,
	}
}

func (p *ErrorDependentRetryPolicy) WithMaximumAttempts(maximumAttempts int) *ErrorDependentRetryPolicy {
	p.maximumAttempts = maximumAttempts
	return p
}

func (p *ErrorDependentRetryPolicy) WithJitter(jitterPct float64) *ErrorDependentRetryPolicy {
	p.jitterPct = jitterPct
	return p
}

func (p *ErrorDependentRetryPolicy) ComputeNextDelay(_ time.Duration, attempt int, err error) time.Duration {
	if p.maximumAttempts != noMaximumAttempts && attempt >= p.maximumAttempts {
		return done
	}

	return addJitter(p.delayForError(err), p.jitterPct)
}

var _ RetryPolicy = (*ConstantDelayRetryPolicy)(nil)

func NewConstantDelayRetryPolicy(delay time.Duration) *ConstantDelayRetryPolicy {
	return &ConstantDelayRetryPolicy{
		maximumAttempts: defaultMaximumAttempts,
		jitterPct:       defaultJitterPct,
		delay:           delay,
	}
}

func (p *ConstantDelayRetryPolicy) WithMaximumAttempts(maximumAttempts int) *ConstantDelayRetryPolicy {
	p.maximumAttempts = maximumAttempts
	return p
}

func (p *ConstantDelayRetryPolicy) WithJitter(jitterPct float64) *ConstantDelayRetryPolicy {
	p.jitterPct = jitterPct
	return p
}

func (p *ConstantDelayRetryPolicy) ComputeNextDelay(_ time.Duration, attempt int, _ error) time.Duration {
	if p.maximumAttempts != noMaximumAttempts && attempt >= p.maximumAttempts {
		return done
	}

	return addJitter(p.delay, p.jitterPct)
}

func addJitter(duration time.Duration, jitterPct float64) time.Duration {
	return duration * time.Duration(1+jitterPct*rand.Float64())
}

func getJitterRand() *rand.Rand {
	if r := jitterRand.Load(); r != nil {
		return r
	}
	r := rand.New(NewRetryLockedSource())

	if !jitterRand.CompareAndSwap(nil, r) {
		// Two different goroutines called some top-level
		// function at the same time. While the results in
		// that case are unpredictable, if we just use r here,
		// and we are using a seed, we will most likely return
		// the same value for both calls. That doesn't seem ideal.
		// Just use the first one to get in.
		return jitterRand.Load()
	}

	return r
}

// We want to wrap our rng source with mutex, because the one in math/rand is used by other clients,
// so all of them are contending for the same mutex.
// Proper solution will be to use standard thread safe Rng source, but until Go 2 it seems it will not happen.
// See the following discussions for details
// https://github.com/golang/go/issues/24121 <- main
// https://github.com/stripe/veneur/pull/466 -< make rng source faster
// https://github.com/golang/go/issues/25057
// https://github.com/golang/go/issues/21393

type RetryLockedSource struct {
	lk sync.Mutex
	s  rand.Source
}

func (r *RetryLockedSource) Int63() int64 {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.s.Int63()
}

func (r *RetryLockedSource) Seed(seed int64) {
	panic("internal error: call to RetryLockedSource.Seed")
}

func NewRetryLockedSource() *RetryLockedSource {
	return &RetryLockedSource{
		lk: sync.Mutex{},
		s:  rand.NewSource(time.Now().UnixNano()),
	}
}
