// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package backoff

import (
	"math"
	"math/rand"
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

	defaultFirstPhaseMaximumAttempts = 3
)

var (
	// DisabledRetryPolicy is a retry policy that never retries
	DisabledRetryPolicy RetryPolicy = &disabledRetryPolicyImpl{}
)

type (
	// RetryPolicy is the API which needs to be implemented by various retry policy implementations
	RetryPolicy interface {
		ComputeNextDelay(elapsedTime time.Duration, numAttempts int) time.Duration
	}

	// Retrier manages the state of retry operation
	Retrier interface {
		NextBackOff() time.Duration
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
func (p *ExponentialRetryPolicy) ComputeNextDelay(elapsedTime time.Duration, numAttempts int) time.Duration {
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

	// add jitter to avoid global synchronization
	jitterPortion := int(0.2 * nextInterval)
	// Prevent overflow
	if jitterPortion < 1 {
		jitterPortion = 1
	}
	nextInterval = nextInterval*0.8 + float64(rand.Intn(jitterPortion))

	return time.Duration(nextInterval)
}

func (r *disabledRetryPolicyImpl) ComputeNextDelay(_ time.Duration, _ int) time.Duration {
	return done
}

// Reset will set the Retrier into initial state
func (r *retrierImpl) Reset() {
	r.startTime = r.timeSource.Now()
	r.currentAttempt = 1
}

// NextBackOff returns the next delay interval.  This is used by Retry to delay calling the operation again
func (r *retrierImpl) NextBackOff() time.Duration {
	nextInterval := r.policy.ComputeNextDelay(r.getElapsedTime(), r.currentAttempt)

	// Now increment the current attempt
	r.currentAttempt++
	return nextInterval
}

func (r *retrierImpl) getElapsedTime() time.Duration {
	return r.timeSource.Now().Sub(r.startTime)
}
