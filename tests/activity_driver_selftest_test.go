package tests

// Self-tests for the activity drivers: when a scripted event's effect never arrives, the driver must
// say so, rather than leaving the test to assert against whatever state it did reach.
//
// Each case injects a mismatch between what the driver believes it configured and what the server
// actually got, via customizeStart, and is paired with an uninjected control.
//
// The driver reports through the require.TestingT it is handed, so these tests hand it a recorder and
// assert on what it recorded, rather than failing themselves.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"google.golang.org/protobuf/types/known/durationpb"
)

// recordingT collects what the driver reports instead of failing the enclosing test.
type recordingT struct{ failures []string }

var errRecordedFailNow = fmt.Errorf("recordingT.FailNow")

func (r *recordingT) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingT) FailNow() { panic(errRecordedFailNow) }

// recordDriverReports runs drive against a recorder and returns everything the driver reported.
func recordDriverReports(drive func(require.TestingT)) (failures []string) {
	rt := &recordingT{}
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
	// realStartToClose is far longer than the window the driver derives below, so the timeout cannot
	// possibly have fired when the driver moves on.
	const realStartToClose = 30 * time.Second

	// There is no BackoffElapses case: that wait takes its deadline from the server's
	// NextAttemptScheduleTime, so a configured interval shorter than the real one cannot make the driver
	// move on early. Only a dispatch the server never makes fails it, which no start-time config injects.

	s.T().Run("StartToCloseElapses", func(t *testing.T) {
		drive := func(customize func(*workflowservice.StartActivityExecutionRequest)) []string {
			return recordDriverReports(func(rt require.TestingT) {
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
	// negativePoll drives the one Poll of a start-delayed activity through the model-checking path, and
	// returns everything the driver reported.
	negativePoll := func(t *testing.T, customize func(*workflowservice.StartActivityExecutionRequest)) []string {
		return recordDriverReports(func(rt require.TestingT) {
			d := newSAADriver(t, newActivityParityEnv(t), activityConfig{MaxAttempts: 1, StartDelay: activityLongDuration})
			d.customizeStart = customize
			a := d.start(rt, d.cfg)
			_, err := a.observed() // seed the stamp baseline, as the model-checking driver does after Start
			require.NoError(rt, err)
			cur, poll := model.Initial(d.cfg.modelConfig()), model.Poll
			a.apply(rt, poll, cur, model.Transition(d.cfg.modelConfig(), cur, poll), true)
		})
	}

	s.T().Run("windowStillOpen", func(t *testing.T) {
		require.Empty(t, negativePoll(t, nil),
			"control: an activity genuinely inside its start delay dispatches nothing, and the poll must say so")
	})

	s.T().Run("windowAlreadyClosed", func(t *testing.T) {
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
