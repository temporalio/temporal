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
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

// activityConfig is the activity a driver starts.
//
// activityConfig.forTrace takes a trace and computes defaults for the confi, so you will often be
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
	RetryInterval          time.Duration // RetryPolicy InitialInterval; 0 => activityDefaultRetryInterval
	BackoffCoefficient     float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	MaxRetryInterval       time.Duration // RetryPolicy MaximumInterval; 0 => RetryInterval
	NextRetryDelay         time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	NonRetryableErrorTypes []string      // RetryPolicy NonRetryableErrorTypes

	StartToClose    time.Duration // 0 => activityLongTimeout, so it does not fire
	ScheduleToClose time.Duration // 0 = unset
	ScheduleToStart time.Duration // 0 = unset
	Heartbeat       time.Duration // 0 = unset
	StartDelay      time.Duration // SAA only: WFA has no per-activity start delay
}

// activityParityDefaultInput is the payload the drivers start activities with.
var activityParityDefaultInput = payloads.EncodeString(activityParityInput)

// activityParityInput is what both surfaces send, so a worker sees the same input either way.
const activityParityInput = "Input"

// activityLongTimeout is a timeout long enough not to fire during a test.
const activityLongTimeout = 24 * time.Hour

// activityShortTimeout is a timeout short enough for a trace to wait out during a test.
const activityShortTimeout = 2 * time.Second

// activityLongRetryInterval is a retry interval long enough to observe an activity while it is still
// backing off.
const activityLongRetryInterval = 30 * time.Second

// activityShortRetryInterval is a retry interval short enough for a trace to wait the backoff out. Not
// much shorter is useful: a timer task's fire time is floored at now + TimerProcessorMaxTimeShift (~1s).
const activityShortRetryInterval = 1 * time.Second

// activityLongStartDelay is a start delay long enough to keep the first attempt pending for a whole test.
const activityLongStartDelay = time.Hour

func (c activityConfig) retryInterval() time.Duration {
	return cmp.Or(c.RetryInterval, activityDefaultRetryInterval)
}
func (c activityConfig) startToClose() time.Duration {
	return cmp.Or(c.StartToClose, activityLongTimeout)
}

// forTrace is the config with a short window for each timeout the trace fires, so that it can. A
// timeout the author set is left alone: only they can say how long a timeout that the trace does not
// fire should be, or that a fired one has a duration the test depends on.
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
			c.Heartbeat = cmp.Or(c.Heartbeat, activityShortTimeout)
		}
	}
	return c
}

// window is how long the clock behind a wall-clock event takes to elapse, from the option that event
// fires on. Zero for an event whose option is not configured, which no trace should drive.
func (c activityConfig) window(e model.Event) time.Duration {
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
		return c.Heartbeat
	default:
		return 0
	}
}

// --- what both surfaces report ----------------------------------------------------------------

// activityInfo is user-visible activity state projected out of the two different messages that
// carry it: SAA's ActivityExecutionInfo and WFA's PendingActivityInfo.
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

// --- driving a trace --------------------------------------------------------------------------

// activityDefaultRetryInterval is the RetryPolicy InitialInterval when a driver sets none.
const activityDefaultRetryInterval = 200 * time.Millisecond

// activityDriverPositivePollTimeout bounds a poll that must find a task.
const activityDriverPositivePollTimeout = 10 * time.Second

// activityDriverWallClockSettle is slack added to a wall-clock event's window when waiting for its effect.
const activityDriverWallClockSettle = 2 * time.Second

// activityDriverPollInterval is the gap between reads when polling for a wall-clock event's effect.
const activityDriverPollInterval = 100 * time.Millisecond

// activityDriverScheduleTimeout bounds the wait for a workflow to schedule the activity it owns.
const activityDriverScheduleTimeout = 10 * time.Second

// activityDriverTerminalTimeout bounds the wait for an activity the trace has driven to a terminal status.
const activityDriverTerminalTimeout = 10 * time.Second

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
		case isWallClockEvent(e.Type) && !isDispatchDelayEvent(e.Type):
			timeouts = append(timeouts, e)
		}
		if len(timeouts) > 1 {
			require.Failf(t, "a trace cannot name two timeouts on one attempt",
				"they run concurrently, so once the first fires the rest cannot occur. This attempt names %v. "+
					"Poll again first if the second belongs to a later attempt.", timeouts)
			return
		}
	}
}

// isWallClockEvent reports whether an event fires on wall-clock time rather than synchronously.
func isWallClockEvent(k model.EventType) bool {
	switch k {
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
func isDispatchDelayEvent(k model.EventType) bool {
	return k == model.StartDelayElapsesType || k == model.BackoffElapsesType
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
		time.Sleep(activityDriverPollInterval)
	}
}
