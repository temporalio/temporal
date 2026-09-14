package tests

// Self-tests for the activity drivers: when a scripted event's effect never arrives, the driver must
// say so, rather than leaving the test to assert against whatever state it did reach.
//
// Each case injects a mismatch between what the driver believes it configured and what the server
// actually got, via customizeStart, and is paired with an uninjected control.
//
// These tests hand the driver a recorder and assert on what it recorded, rather than failing
// themselves.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"google.golang.org/protobuf/types/known/durationpb"
)

// recordingT collects what the driver reports instead of failing the enclosing test.
type recordingT struct {
	testing.TB
	failures []string
}

var errRecordedFailNow = errors.New("recordingT.FailNow")

func (r *recordingT) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingT) Fatalf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
	panic(errRecordedFailNow)
}

func (r *recordingT) FailNow() { panic(errRecordedFailNow) }

// recordDriverReports runs drive against a recorder and returns everything the driver reported.
func recordDriverReports(t testing.TB, drive func(testing.TB)) (failures []string) {
	rt := &recordingT{TB: t}
	defer func() {
		if p := recover(); p != nil && p != errRecordedFailNow {
			panic(p)
		}
		failures = rt.failures
	}()
	drive(rt)
	return rt.failures
}

// TestSAADriverReportsUnrealizedWallClockEvents drives a trace whose wall-clock event provably cannot
// take effect inside the window the driver waits, and requires the driver to report it.
func (s *activityParityTestSuite) TestSAADriverReportsUnrealizedWallClockEvents() {
	t := s.T()
	// realStartToClose is far longer than the window the driver derives below, so the timeout cannot
	// possibly have fired when the driver moves on.
	const realStartToClose = 30 * time.Second

	// There is no BackoffElapses case: that wait takes its deadline from the server's
	// NextAttemptScheduleTime, so a configured interval shorter than the real one cannot make the driver
	// move on early. Only a dispatch the server never makes fails it, which no start-time config injects.

	t.Run("StartToCloseElapses", func(t *testing.T) {
		drive := func(customize func(*workflowservice.StartActivityExecutionRequest)) []string {
			return recordDriverReports(t, func(rt testing.TB) {
				d := newSAADriver(t, newActivityParityEnv(t), activityConfig{
					MaxAttempts:  1,
					StartToClose: activityShortTimeout, // the window the driver will wait out
				})
				d.customizeStart = customize
				d.driveTrace(rt, []model.Event{model.Poll, model.StartToCloseElapses})
			})
		}

		require.Empty(t, drive(nil), "control: an uninjected trace must drive cleanly")

		injected := drive(func(req *workflowservice.StartActivityExecutionRequest) {
			req.StartToCloseTimeout = durationpb.New(realStartToClose)
		})
		require.NotEmpty(t, injected,
			"the attempt was still running when the driver moved past StartToCloseElapses; the driver must report that")
	})
}

// TestSAADriverBlamesItselfWhenItOutrunsTheDispatchWindow requires the negative poll to blame the
// driver, not the product, when the dispatch window it meant to check has already closed. A task found
// then was dispatched legitimately, and reporting it as a product divergence costs an investigation.
//
// Injected by shortening the real start delay to nothing while the driver still believes it is an hour,
// which puts the poll in the position a slow machine would.
func (s *activityParityTestSuite) TestSAADriverBlamesItselfWhenItOutrunsTheDispatchWindow() {
	t := s.T()
	// negativePoll drives the one Poll of a start-delayed activity through the model-checking path, and
	// returns everything the driver reported.
	negativePoll := func(t *testing.T, customize func(*workflowservice.StartActivityExecutionRequest)) []string {
		return recordDriverReports(t, func(rt testing.TB) {
			d := newSAADriver(t, newActivityParityEnv(t), activityConfig{MaxAttempts: 1, StartDelay: activityLongDuration})
			d.customizeStart = customize
			a := d.start(rt, d.cfg)
			_, err := a.observed() // seed the stamp baseline, as the model-checking driver does after Start
			require.NoError(rt, err)
			cur, poll := model.Initial(d.cfg.modelConfig()), model.Poll
			applyActivityEvent(rt, a, poll, cur, model.Transition(d.cfg.modelConfig(), cur, poll), true)
		})
	}

	t.Run("windowStillOpen", func(t *testing.T) {
		require.Empty(t, negativePoll(t, nil),
			"control: an activity genuinely inside its start delay dispatches nothing, and the poll must say so")
	})

	t.Run("windowAlreadyClosed", func(t *testing.T) {
		reports := strings.Join(negativePoll(t, func(req *workflowservice.StartActivityExecutionRequest) {
			req.StartDelay = nil
		}), "\n")
		require.NotContains(t, reports, "a task WAS dispatched",
			"the dispatch was legitimate — the driver ran its check after the window closed — so this must "+
				"not be reported as the product dispatching early")
		require.Contains(t, reports, outranDispatchWindow,
			"the driver must report that it could no longer make this check")
	})
}

// TestAdjudicateDispatch pins how a negative poll tells a product defect from its own window closing: a
// task found while the window was still open is the product dispatching early; one found after the
// window closed is excused.
func TestAdjudicateDispatch(t *testing.T) {
	dispatchTime := time.Date(2020, 1, 1, 0, 0, 10, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		polledUntil time.Time
		expected    negativePollResult
	}{
		{"well inside the window", dispatchTime.Add(-5 * time.Second), dispatchedEarly},
		{"just inside the window", dispatchTime.Add(-time.Nanosecond), dispatchedEarly},
		{"exactly at the dispatch time", dispatchTime, windowOutrun},
		{"after the window closed", dispatchTime.Add(5 * time.Second), windowOutrun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, adjudicateDispatch(tc.polledUntil, dispatchTime))
		})
	}
}

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
