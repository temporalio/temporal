package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/persistence"
)

func Test_NextRetry(t *testing.T) {
	a := assert.New(t)
	now, _ := time.Parse(time.RFC3339, "2018-04-13T16:08:08+00:00")
	reason := "good-reason"
	identity := "some-worker-identity"

	// no retry without retry policy
	ai := &persistence.ActivityInfo{
		ScheduleToStartTimeout: 5,
		ScheduleToCloseTimeout: 30,
		StartToCloseTimeout:    25,
		HasRetryPolicy:         false,
		NonRetriableErrors:     []string{"bad-reason", "ugly-reason"},
		StartedIdentity:        identity,
	}
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))

	// no retry if cancel requested
	ai.HasRetryPolicy = true
	ai.CancelRequested = true
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))

	// no retry if both MaximumAttempts and ExpirationTime are not set
	ai.CancelRequested = false
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))

	// no retry if MaximumAttempts is 1 (for initial attempt)
	ai.InitialInterval = 1
	ai.MaximumAttempts = 1
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))

	// backoff retry, intervals: 1s, 2s, 4s, 8s.
	ai.MaximumAttempts = 5
	ai.BackoffCoefficient = 2
	a.Equal(time.Second, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))
	ai.Attempt++

	a.Equal(time.Second*2, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))
	ai.Attempt++

	a.Equal(time.Second*4, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))
	ai.Attempt++

	// test non-retriable error
	reason = "bad-reason"
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))

	reason = "good-reason"

	a.Equal(time.Second*8, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))
	ai.Attempt++

	// no retry as max attempt reached
	a.Equal(ai.MaximumAttempts-1, ai.Attempt)
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))

	// increase max attempts, with max interval cap at 10s
	ai.MaximumAttempts = 6
	ai.MaximumInterval = 10
	a.Equal(time.Second*10, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))
	ai.Attempt++

	// no retry because expiration time before next interval
	ai.MaximumAttempts = 8
	ai.ExpirationTime = now.Add(time.Second * 5)
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))

	// extend expiration, next interval should be 10s
	ai.ExpirationTime = now.Add(time.Minute)
	a.Equal(time.Second*10, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))
	ai.Attempt++

	// with big max retry, math.Pow() could overflow, verify that it uses the MaxInterval
	ai.Attempt = 64
	ai.MaximumAttempts = 100
	a.Equal(time.Second*10, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetriableErrors,
	))
	ai.Attempt++
}
