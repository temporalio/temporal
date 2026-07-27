package tests

// Config shared by the two activity drivers, activity_standalone_driver.go and
// activity_workflow_driver.go.

import (
	"cmp"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"google.golang.org/protobuf/types/known/durationpb"
)

// activityConfig is the activity a driver starts.
//
// activityConfig.forTrace takes a trace and computes defaults for the config, so you will often be
// able to supply a trace and not worry about the config. Timeouts are usually left unset:
// activityConfig.forTrace gives a short window to each one the trace fires, so that adding e.g.
// model.HeartbeatElapses to a trace is all you need to do to specify that the activity has a
// heartbeat timeout. Set a timeout explicitly in the config only to say something the trace cannot
// — that it exists without firing, or that its exact duration is what the test is about.
//
// The server rejects an activity with neither start-to-close nor schedule-to-close set. The drivers
// always send start-to-close, defaulted long enough not to fire. The other timeouts are simply
// absent when unset.
type activityConfig struct {
	MaxAttempts            int32         // RetryPolicy MaximumAttempts; 0 = unlimited
	RetryInterval          time.Duration // RetryPolicy InitialInterval; 0 => activityShortRetryInterval
	BackoffCoefficient     float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	MaxRetryInterval       time.Duration // RetryPolicy MaximumInterval; 0 => RetryInterval
	NextRetryDelay         time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	NonRetryableErrorTypes []string      // RetryPolicy NonRetryableErrorTypes

	StartToClose     time.Duration // 0 => activityLongDuration, so it does not fire
	ScheduleToClose  time.Duration // 0 = unset
	ScheduleToStart  time.Duration // 0 = unset
	HeartbeatTimeout time.Duration // 0 = unset
	StartDelay       time.Duration // SAA only: WFA has no per-activity start delay
}

// activityInput is what both SAA and WFA send, so a worker sees the same input either way.
const activityInput = "Input"

// timerProcessorMaxShift is the floor the timer queue puts on a task's fire time: it will not fire one
// earlier than now + this.
var timerProcessorMaxShift = dynamicconfig.TimerProcessorMaxTimeShift.Get(
	dynamicconfig.NewCollection(dynamicconfig.StaticClient(nil), log.NewNoopLogger()))()

// activityLongDuration is a timeout, retry interval or start delay long enough not to elapse during a
// test.
const activityLongDuration = 24 * time.Hour

// activityShortTimeout is a timeout short enough to wait for while driving a trace
var activityShortTimeout = 2 * timerProcessorMaxShift

// activityShortDispatchDelay is a retry interval or start delay short enough to wait for while
// driving a trace. Note that the queue will not fire the dispatch timer any earlier than
// timerProcessorMaxShift.
var activityShortDispatchDelay = timerProcessorMaxShift

func (c activityConfig) retryInterval() time.Duration {
	return cmp.Or(c.RetryInterval, activityShortDispatchDelay)
}
func (c activityConfig) startToClose() time.Duration {
	return cmp.Or(c.StartToClose, activityLongDuration)
}

// forTrace replaces missing values in the config with appropriate values for the given trace.
func (c activityConfig) forTrace(trace []model.Event) activityConfig {
	for _, e := range trace {
		switch e.Type {
		case model.ScheduleToStartElapsesType:
			c.ScheduleToStart = cmp.Or(c.ScheduleToStart, activityShortTimeout)
		case model.ScheduleToCloseElapsesType:
			c.ScheduleToClose = cmp.Or(c.ScheduleToClose, activityShortTimeout)
		case model.StartToCloseElapsesType:
			c.StartToClose = cmp.Or(c.StartToClose, activityShortTimeout)
		case model.HeartbeatElapsesType:
			c.HeartbeatTimeout = cmp.Or(c.HeartbeatTimeout, activityShortTimeout)
		default: // a non-timeout event
		}
	}
	return c
}

// timerDuration is how long the timer behind a timer event takes to elapse.
func (c activityConfig) timerDuration(e model.Event) time.Duration {
	switch e.Type {
	case model.StartDelayElapsesType:
		return c.StartDelay
	case model.BackoffElapsesType:
		// The first backoff only: a later one is longer under a non-constant policy. Waiting for a
		// dispatch uses the server's schedule time instead; see awaitDispatchTimePassed.
		return cmp.Or(c.NextRetryDelay, c.retryInterval())
	case model.StartToCloseElapsesType:
		return c.startToClose()
	case model.ScheduleToCloseElapsesType:
		return c.ScheduleToClose
	case model.ScheduleToStartElapsesType:
		return c.ScheduleToStart
	case model.HeartbeatElapsesType:
		return c.HeartbeatTimeout
	default:
		panic("unknown event type: " + e.Type.String())
	}
}

// activityInfo is user-visible activity state projected out of SAA's ActivityExecutionInfo and
// WFA's PendingActivityInfo.
//
// CurrentRetryInterval is rounded to the second, because WFA derives it by subtracting two stored
// timestamps while SAA stores it exactly. NextAttemptScheduleTime is reduced to whether it is set
// to facilitate test assertions.
type activityInfo struct {
	RunState                   enumspb.PendingActivityState
	Attempt                    int32
	CurrentRetryInterval       time.Duration
	NextAttemptScheduleTimeSet bool
}

// activityDriverTimeout bounds a wait for something the server should do promptly: dispatch a task to
// poll for, schedule the activity a workflow owns, close an activity the trace has finished with. A
// wait for a configured window is bounded by that window plus activityDriverTimerMargin instead.
const activityDriverTimeout = 10 * time.Second

// activityDriverTimerMargin is margin added to a timer event's duration when polling for its effect.
var activityDriverTimerMargin = 2 * timerProcessorMaxShift

// activityDriverPollInterval is the gap between reads when polling for a timer event's effect.
const activityDriverPollInterval = 100 * time.Millisecond

// timeoutType is the TimeoutType a timeout-elapse event reports when it fires,
// TIMEOUT_TYPE_UNSPECIFIED for any other event. The model names no API types, so the correspondence
// lives here.
func timeoutType(e model.Event) enumspb.TimeoutType {
	switch e.Type {
	case model.ScheduleToStartElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
	case model.ScheduleToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	case model.StartToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_START_TO_CLOSE
	case model.HeartbeatElapsesType:
		return enumspb.TIMEOUT_TYPE_HEARTBEAT
	default:
		return enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}
}

// validateTrace rejects a trace the drivers cannot realize. An attempt's timeouts run concurrently,
// from deadlines the server anchors at schedule or attempt-start time, while the driver waits each one
// out from the moment its event is driven — so an attempt can be ended by at most one. Once the first
// fires, the others are no longer running and the driver would wait for something that never happens.
//
// A Poll starts a new attempt, which arms a fresh set, so the same timeout may appear again after one.
// Dispatch delays are exempt entirely: each backoff is its own window, and awaitDispatchTimePassed
// takes its deadline from the server rather than from the trace.
//
// A rule of thumb, not a decision procedure. The model decides this per event and per state, and
// replaces this once it lands here.
func validateTrace(t require.TestingT, trace []model.Event) {
	var timeouts []model.Event
	for _, e := range trace {
		switch {
		case e.Type == model.PollType:
			timeouts = nil // a new attempt arms its timeouts afresh
		case isTimerEvent(e.Type) && !isDispatchDelayEvent(e.Type):
			timeouts = append(timeouts, e)
		default: // an event that neither starts an attempt nor ends one by timeout
		}
		if len(timeouts) > 1 {
			require.Failf(t, "a trace cannot name two timeouts on one attempt",
				"they run concurrently, so once the first fires the rest cannot occur. This attempt names %v. "+
					"Poll again first if the second belongs to a later attempt.", timeouts)
			return
		}
	}
}

// isTimerEvent reports whether an event represents a timer elapsing, as opposed to an RPC.
func isTimerEvent(et model.EventType) bool {
	switch et {
	case model.ScheduleToStartElapsesType, model.ScheduleToCloseElapsesType, model.StartToCloseElapsesType,
		model.HeartbeatElapsesType, model.StartDelayElapsesType, model.BackoffElapsesType:
		return true
	default:
		return false
	}
}

// isDispatchDelayEvent reports whether an event is a dispatch-delay window elapsing rather than a timeout.
// A dispatch delay advances no transition-history version; its effect is the pending dispatch time
// passing.
func isDispatchDelayEvent(et model.EventType) bool {
	return et == model.StartDelayElapsesType || et == model.BackoffElapsesType
}

func activityFailure(retryable bool, nextRetryDelay time.Duration) *failurepb.Failure {
	info := &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: !retryable}
	if nextRetryDelay > 0 {
		info.NextRetryDelay = durationpb.New(nextRetryDelay)
	}
	return &failurepb.Failure{
		Message:     "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
	}
}

// activityTimeoutMark is what a driver compares to decide that the timeout an event names is this
// event's, rather than one left over from an earlier attempt.
type activityTimeoutMark struct {
	timeout enumspb.TimeoutType
	attempt int32
	closed  bool
}

// activityDriverPollUntil reports whether cond held before the deadline, reading every activityDriverPollInterval.
//
// common/testing/await is the usual way to write this, but await.Require and await.RequireTrue take a
// testing.TB, which has an unexported method and so admits only *testing.T. The drivers take a
// require.TestingT instead, which is what lets their self-tests hand them a recorder and assert on
// what they reported.
func activityDriverPollUntil(deadline time.Time, cond func() bool) bool {
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(activityDriverPollInterval) //nolint:forbidigo
	}
}
