package tests

// Config shared by the two activity drivers, activity_standalone_driver.go and
// activity_workflow_driver.go.

import (
	"cmp"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
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
	RetryInterval          time.Duration // RetryPolicy InitialInterval; 0 => activityShortDispatchDelay
	BackoffCoefficient     float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	MaxRetryInterval       time.Duration // RetryPolicy MaximumInterval; 0 => RetryInterval
	NextRetryDelay         time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	NonRetryableErrorTypes []string      // RetryPolicy NonRetryableErrorTypes

	StartToClose     time.Duration // 0 => activityLongDuration, so it does not fire
	ScheduleToClose  time.Duration
	ScheduleToStart  time.Duration
	HeartbeatTimeout time.Duration
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
		case model.StartDelayElapsesType:
			c.StartDelay = cmp.Or(c.StartDelay, activityShortDispatchDelay)
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
		// dispatch uses the server's schedule time instead; see awaitDispatchDelay.
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

// modelConfig is the model's view of the activity: which options are configured at all. Deriving it
// means the two cannot disagree.
func (c activityConfig) modelConfig() model.Config {
	return model.Config{
		MaxAttempts:        c.MaxAttempts,
		HasStartDelay:      c.StartDelay > 0,
		HasScheduleToClose: c.ScheduleToClose > 0,
		HasScheduleToStart: c.ScheduleToStart > 0,
		HasHeartbeat:       c.HeartbeatTimeout > 0,
	}
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

// activityModelCursor is the model state a driver has reached, so that driveEvent can check each
// event against the state it is driven from.
type activityModelCursor struct {
	cfg   model.Config
	state model.AbstractState
}

func newActivityModelCursor(cfg activityConfig) *activityModelCursor {
	mc := cfg.modelConfig()
	return &activityModelCursor{cfg: mc, state: model.Initial(mc)}
}

// check fails if e cannot occur in the state reached so far, then advances past it.
func (c *activityModelCursor) check(t require.TestingT, e model.Event) {
	if !model.Possible(c.cfg, c.state, e.Type) {
		require.Failf(t, "the trace drives an event that cannot occur",
			"%s cannot occur in %v/%v: its clock is not running there. Remove it, or drive the events "+
				"that start its clock first.", e, c.state.Status, c.state.Dispatchability)
		return
	}
	c.state = model.Transition(c.cfg, c.state, e).Next
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
	attemptFailure enumspb.TimeoutType // ended the last attempt
	outcome        enumspb.TimeoutType // closed the activity
	cause          enumspb.TimeoutType // chained by outcome as what led to it
	attempt        int32
	closed         bool
}

// reports says whether the activity reports tt as having occurred.
func (m activityTimeoutMark) reports(tt enumspb.TimeoutType) bool {
	return tt != enumspb.TIMEOUT_TYPE_UNSPECIFIED &&
		(m.attemptFailure == tt || m.outcome == tt || m.cause == tt)
}

func timeoutTypeOf(f *failurepb.Failure) enumspb.TimeoutType {
	return f.GetTimeoutFailureInfo().GetTimeoutType()
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

// activityHeartbeatDetails is the checkpoint payload both drivers send with a Heartbeat event.
var activityHeartbeatDetails = &commonpb.Payloads{Payloads: []*commonpb.Payload{{
	Metadata: map[string][]byte{"encoding": []byte("json/plain")},
	Data:     []byte(`"hb"`),
}}}

func firstPayloadData(p *commonpb.Payloads) []byte {
	if ps := p.GetPayloads(); len(ps) > 0 {
		return ps[0].GetData()
	}
	return nil
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

// activityTerminalProjection is the terminal status plus the failure discriminant a user sees: the
// application failure Type for FAILED, the TimeoutType string for TIMED_OUT, empty otherwise.
type activityTerminalProjection struct {
	Status      enumspb.ActivityExecutionStatus
	FailureType string
}

// failureCause is the Type and Message of the failure a terminal outcome chains as its Cause.
type failureCause struct {
	Type    string
	Message string
}
