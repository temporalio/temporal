package tests

// Self-tests for the activity drivers.

import (
	"fmt"
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

// The model refuses two sorts of trace event: one whose clock is not running, and a Poll that
// finds no task. Driving either waits for something that never happens.
func TestModelRefusesUnrealizableEvents(t *testing.T) {
	t.Parallel()

	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	for name, tc := range map[string]struct {
		accepted []model.Event
		refused  model.Event
		failure  string
	}{
		"PollDuringRetryBackoff": {
			accepted: []model.Event{model.Poll, model.FailRetryably},
			refused:  model.Poll,
			failure:  "Poll that finds no task",
		},
		"PollWhilePaused": {
			accepted: []model.Event{model.Pause},
			refused:  model.Poll,
			failure:  "Poll that finds no task",
		},
		"PollAfterClose": {
			accepted: []model.Event{model.Terminate},
			refused:  model.Poll,
			failure:  "Poll that finds no task",
		},
		"HeartbeatTimeoutThatIsNotConfigured": {
			accepted: []model.Event{model.Poll},
			refused:  model.HeartbeatElapses,
			failure:  "event that cannot occur",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := newActivityModel(cfg)
			for _, e := range tc.accepted {
				require.Emptyf(t, tryAdvance(m, e), "the model must accept %s", e)
			}
			require.Contains(t, tryAdvance(m, tc.refused), tc.failure)
		})
	}
}

// tryAdvance advances m past e, returning the refusal the model reported, or empty if it accepted e
// and advanced.
func tryAdvance(m *activityModel, e model.Event) string {
	recorder := &failureRecorder{}
	m.advance(recorder, e)
	return recorder.failure
}

// failureRecorder is a require.TestingT that records a refusal rather than failing the test, so that
// a test can assert the model refuses an event. FailNow does nothing, which is safe because
// activityModel.advance returns as soon as it has reported a refusal.
type failureRecorder struct{ failure string }

func (r *failureRecorder) Errorf(format string, args ...any) {
	r.failure = fmt.Sprintf(format, args...)
}
func (r *failureRecorder) FailNow() {}
