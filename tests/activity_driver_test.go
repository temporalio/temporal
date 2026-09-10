package tests

// Self-tests for the activity drivers.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
)

// TestDriversRecognizeTimeoutObservedBeforeWait reproduces a race in awaitTimeout: a retryable timeout
// may fire after Poll returns but before awaitTimeout takes its first observation. The timeout has
// already rescheduled attempt 2 by then, and the driver must recognize it rather than wait for a
// subsequent change that will never come.
func (s *activityParityTestSuite) TestDriversRecognizeTimeoutObservedBeforeWait() {
	const waitForDriver = 2 * activityDriverPollInterval
	cfg := activityConfig{
		MaxAttempts:   2,
		RetryInterval: activityLongDuration,
		StartToClose:  activityShortTimeout,
	}

	waitUntilTimeoutVisible := func(t *testing.T, timeoutInfo func(require.TestingT) activityTimeoutInfo) {
		var got activityTimeoutInfo
		await.Require(t.Context(), t, func(t *await.T) {
			got = timeoutInfo(t)
			t.Require().Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, got.timeout)
			t.Require().Equal(int32(2), got.attempt)
		}, cfg.StartToClose+activityDriverTimerMargin, activityDriverPollInterval)
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newWFADriver(t, newActivityParityEnv(t), cfg).start(t, cfg)
		a.driveEvent(t, model.Poll)
		waitUntilTimeoutVisible(t, a.timeoutInfo)
		a.awaitTimeout(t, model.StartToCloseElapses, time.Now().Add(waitForDriver))
	})

	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newSAADriver(t, newActivityParityEnv(t), cfg).start(t, cfg)
		a.driveEvent(t, model.Poll)
		waitUntilTimeoutVisible(t, a.timeoutInfo)
		a.awaitTimeout(t, model.StartToCloseElapses, time.Now().Add(waitForDriver))
	})
}

// contextualDriver is the slice of a driver's API this test exercises.
type contextualDriver interface{ testContext() context.Context }

// TestDriverContextReflectsExtension proves the drivers fetch their context fresh on every call
// instead of caching the one observed at construction, so a timeout extension made after the driver
// starts is visible to later RPCs. env is nil: testContext() never touches it.
//
// Runs inside a synctest bubble so it doesn't need to wait out real minutes of test-context timeout,
// and so `go test -timeout` (a real-clock deadline, meaningless in a fake-clock bubble) can't cap the
// context and mask the very extension this test is checking for.
func TestDriverContextReflectsExtension(t *testing.T) {
	t.Parallel()

	for name, newDriver := range map[string]func(*testing.T) contextualDriver{
		"SAA": func(t *testing.T) contextualDriver { return newSAADriver(t, nil, activityConfig{}) },
		"WFA": func(t *testing.T) contextualDriver { return newWFADriver(t, nil, activityConfig{}) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				d := newDriver(t)
				before, ok := d.testContext().Deadline()
				require.True(t, ok)

				extended := testcontext.EnsureRemaining(testcontext.For(t), t, testcontext.DefaultTimeout()+time.Minute)
				extendedDeadline, ok := extended.Deadline()
				require.True(t, ok)
				require.True(t, extendedDeadline.After(before), "test setup: EnsureRemaining should have extended the deadline")

				after, ok := d.testContext().Deadline()
				require.True(t, ok)
				require.Equal(t, extendedDeadline, after,
					"driver must observe the extended deadline, not the one captured at construction")
			})
		})
	}
}
