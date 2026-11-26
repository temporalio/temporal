package tests

// Self-tests for the activity drivers.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/parallelsuite"
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

	waitUntilTimeoutVisible := func(s *activityParityTestSuite, timeoutInfo func(require.TestingT) activityTimeoutInfo) {
		var got activityTimeoutInfo
		s.Await(func(s *activityParityTestSuite) {
			got = timeoutInfo(s.AssertionT())
			s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, got.timeout)
			s.Equal(int32(2), got.attempt)
		}, cfg.StartToClose+activityDriverTimerMargin, activityDriverPollInterval)
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newWFADriver(t, newActivityParityEnv(t), cfg).start(t, cfg)
		a.driveEvent(t, model.Poll)
		waitUntilTimeoutVisible(s, a.timeoutInfo)
		a.awaitTimeout(t, model.StartToCloseElapses, time.Now().Add(waitForDriver))
	})

	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newSAADriver(t, newActivityParityEnv(t), cfg).start(t, cfg)
		a.driveEvent(t, model.Poll)
		waitUntilTimeoutVisible(s, a.timeoutInfo)
		a.awaitTimeout(t, model.StartToCloseElapses, time.Now().Add(waitForDriver))
	})
}

type ActivityDriverSuite struct {
	parallelsuite.Suite[*ActivityDriverSuite]
}

func TestActivityDriverSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityDriverSuite{})
}

// The model refuses two sorts of trace event: one whose clock is not running, and a Poll that
// finds no task. Driving either waits for something that never happens.
func (s *ActivityDriverSuite) TestModelRefusesUnrealizableEvents() {
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
		s.Run(name, func(s *ActivityDriverSuite) {

			m := newActivityModel(cfg)
			for _, e := range tc.accepted {
				s.Emptyf(tryAdvance(m, e), "the model must accept %s", e)
			}
			s.Contains(tryAdvance(m, tc.refused), tc.failure)
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

// contextualDriver is the slice of a driver's API this test exercises.
type contextualDriver interface{ testContext() context.Context }

// TestDriverContextReflectsExtension proves the drivers fetch their context fresh on every call
// instead of caching the one observed at construction, so a timeout extension made after the driver
// starts is visible to later RPCs. env is nil: testContext() never touches it.
//
// Runs inside a synctest bubble so it doesn't need to wait out real minutes of test-context timeout,
// and so `go test -timeout` (a real-clock deadline, meaningless in a fake-clock bubble) can't cap the
// context and mask the very extension this test is checking for.
func (s *ActivityDriverSuite) TestDriverContextReflectsExtension() {
	for name, newDriver := range map[string]func(*testing.T) contextualDriver{
		"SAA": func(t *testing.T) contextualDriver { return newSAADriver(t, nil, activityConfig{}) },
		"WFA": func(t *testing.T) contextualDriver { return newWFADriver(t, nil, activityConfig{}) },
	} {
		s.Run(name, func(s *ActivityDriverSuite) {
			s.Synctest(func(s *ActivityDriverSuite) {
				d := newDriver(s.T())
				before, ok := d.testContext().Deadline()
				s.True(ok)

				extended := testcontext.EnsureRemaining(s.Context(), s.T(), testcontext.DefaultTimeout()+time.Minute)
				extendedDeadline, ok := extended.Deadline()
				s.True(ok)
				s.True(extendedDeadline.After(before), "test setup: EnsureRemaining should have extended the deadline")

				after, ok := d.testContext().Deadline()
				s.True(ok)
				s.Equal(extendedDeadline, after,
					"driver must observe the extended deadline, not the one captured at construction")
			})
		})
	}
}
