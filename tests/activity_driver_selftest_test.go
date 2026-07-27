package tests

// Self-tests for the activity drivers.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
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

	waitUntilTimeoutVisible := func(t *testing.T, timeoutInfo func() activityTimeoutInfo) {
		var got activityTimeoutInfo
		require.Truef(t, activityDriverPollUntil(
			time.Now().Add(cfg.StartToClose+activityDriverTimerMargin),
			func() bool {
				got = timeoutInfo()
				return got.timeout == enumspb.TIMEOUT_TYPE_START_TO_CLOSE && got.attempt == 2
			},
		), "attempt 1 did not time out and reschedule attempt 2; last observed: %+v", got)
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		a := newWFADriver(t, newActivityParityEnv(t), cfg).start(t, cfg)
		a.driveEvent(t, model.Poll)
		waitUntilTimeoutVisible(t, func() activityTimeoutInfo { return a.timeoutInfo(t) })
		a.awaitTimeout(t, model.StartToCloseElapses, time.Now().Add(waitForDriver))
	})

	s.T().Run("StandaloneActivity", func(t *testing.T) {
		a := newSAADriver(t, newActivityParityEnv(t), cfg).start(t, cfg)
		a.driveEvent(t, model.Poll)
		waitUntilTimeoutVisible(t, func() activityTimeoutInfo { return a.timeoutInfo(t) })
		a.awaitTimeout(t, model.StartToCloseElapses, time.Now().Add(waitForDriver))
	})
}
