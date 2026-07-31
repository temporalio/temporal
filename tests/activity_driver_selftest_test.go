package tests

// Self-tests for the activity drivers.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/await"
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
