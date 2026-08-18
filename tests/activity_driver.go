package tests

// Config shared by activity_standalone_driver.go and activity_workflow_driver.go.

import (
	"cmp"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// activityHeartbeatDetails is the checkpoint payload a driver attaches to RespondActivityTaskFailed when
// the event sets HasHeartbeatDetails; the server stores it as the activity's last heartbeat progress. It
// differs from the model.Heartbeat payload so assertions can tell which source was persisted.
var activityHeartbeatDetails = payloads.EncodeString("failure checkpoint details")

// activityRecordedHeartbeatDetails is the checkpoint payload a driver sends for a model.Heartbeat event.
var activityRecordedHeartbeatDetails = payloads.EncodeString("heartbeat details")

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
		default: // an event that arms no window of its own
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
	LastHeartbeatDetails       []byte
}

// activityTerminalOutcome is user-visible terminal activity state projected from SAA's
// ActivityExecutionOutcome and WFA's workflow result.
type activityTerminalOutcome struct {
	status     enumspb.ActivityExecutionStatus
	retryState enumspb.RetryState
}

// activityDriverTimeout bounds a wait for something the server should do promptly: dispatch a task to
// poll for, schedule the activity a workflow owns, close an activity the trace has finished with. A
// wait for a configured window is bounded by that window plus activityDriverTimerMargin instead.
const activityDriverTimeout = 10 * time.Second

// activityDriverTimerMargin is margin added to a timer event's duration when polling for its effect.
var activityDriverTimerMargin = activityDriverTimeout

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

// activityFailureSizeLimit is used to truncate larger retryable failure message.
var activityFailureSizeLimit = dynamicconfig.MutableStateActivityFailureSizeLimitError.Get(
	dynamicconfig.NewCollection(dynamicconfig.StaticClient(nil), log.NewNoopLogger()))("")

// activityLargeFailureMessage is an example large message which may get truncated.
var activityLargeFailureMessage = strings.Repeat("x", 2*activityFailureSizeLimit)

// respondFailedFailure is the Failure a RespondFailed event carries, or nil when the event omits it
// (modeling a worker that calls RespondActivityTaskFailed without a Failure).
func respondFailedFailure(e model.Event, nextRetryDelay time.Duration) *failurepb.Failure {
	if e.Failure == nil {
		return nil
	}
	switch e.Failure.Type {
	case model.ApplicationFailureType:
		info := &failurepb.ApplicationFailureInfo{Type: "TestFailure", NonRetryable: !e.Failure.Retryable}
		if nextRetryDelay > 0 {
			info.NextRetryDelay = durationpb.New(nextRetryDelay)
		}
		message := "test failure"
		if e.Failure.LargeMessage {
			message = activityLargeFailureMessage
		}
		return &failurepb.Failure{
			Message:     message,
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
		}
	case model.ServerFailureType:
		return &failurepb.Failure{
			Message:     "test server failure",
			FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: !e.Failure.Retryable}},
		}
	case model.StartToCloseTimeoutFailureType:
		return syntheticTimeoutFailure(enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	case model.HeartbeatTimeoutFailureType:
		return syntheticTimeoutFailure(enumspb.TIMEOUT_TYPE_HEARTBEAT)
	case model.ScheduleToStartTimeoutFailureType:
		return syntheticTimeoutFailure(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)
	case model.ScheduleToCloseTimeoutFailureType:
		return syntheticTimeoutFailure(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	case model.UnknownFailureType:
		return &failurepb.Failure{Message: "test unknown failure"}
	default:
		panic(fmt.Sprintf("unknown failure type: %d", e.Failure.Type))
	}
}

// syntheticTimeoutFailure creates a worker-reported timeout for the by-ID failure RPC,
// exercising failure classification rather than the server's timeout-task path.
func syntheticTimeoutFailure(timeoutType enumspb.TimeoutType) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "test synthetic timeout failure",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: timeoutType,
		}},
	}
}

// activityTimeoutInfo is the information a driver uses to identify a timeout.
type activityTimeoutInfo struct {
	timeout  enumspb.TimeoutType // Timeout type currently reported; unspecified when none is reported.
	attempt  int32               // Current attempt; advances when a retryable per-attempt timeout fires.
	terminal bool                // Whether the activity is terminal, as every non-retrying timeout makes it.
}

// activityDriverState is the state shared by the two drivers.
type activityDriverState struct {
	ctx            context.Context
	cfg            activityConfig
	token          []byte
	startedAttempt int32 // attempt number returned by the last successful Poll
}

// driverState lets an embedded activityDriverState supply its state to drivenActivity.
func (a *activityDriverState) driverState() *activityDriverState {
	return a
}

// drivenActivity is what the shared event driver needs from either implementation.
type drivenActivity interface {
	driverState() *activityDriverState
	pollForTask(require.TestingT, time.Duration) *workflowservice.PollActivityTaskQueueResponse
	awaitDispatchDelay(testing.TB, model.Event)
	timeoutInfo(require.TestingT) activityTimeoutInfo
	rpc(testing.TB, model.Event) error
}

// driveActivityEvent advances an activity by one event.
func driveActivityEvent(t testing.TB, a drivenActivity, e model.Event) {
	state := a.driverState()
	switch {
	case e.Type == model.PollType:
		resp := a.pollForTask(t, activityDriverTimeout)
		require.NotNilf(t, resp, "%s: no task was dispatched within %s", e, activityDriverTimeout)
		state.token = resp.GetTaskToken()
		state.startedAttempt = resp.GetAttempt()
	case isDispatchDelayEvent(e.Type):
		a.awaitDispatchDelay(t, e)
	case isTimerEvent(e.Type):
		awaitActivityTimeout(t, a, e, time.Now().Add(state.cfg.timerDuration(e)+activityDriverTimerMargin))
	default:
		require.NoError(t, a.rpc(t, e))
	}
}

// awaitActivityTimeout blocks until the activity reports the timeout the event names, and fails if it
// does not within (window + margin).
func awaitActivityTimeout(t testing.TB, a drivenActivity, e model.Event, deadline time.Time) {
	state := a.driverState()
	want := timeoutType(e)
	var got activityTimeoutInfo
	await.Require(state.ctx, t, func(t *await.T) {
		got = a.timeoutInfo(t)
		fired := got.timeout == want && (got.terminal || got.attempt > state.startedAttempt)
		t.Require().Truef(fired,
			"%s: activity reports timeout %s at attempt %d (terminal=%v), want %s after attempt %d",
			e, got.timeout, got.attempt, got.terminal, want, state.startedAttempt)
	}, max(0, time.Until(deadline)), activityDriverPollInterval)
}

// awaitActivityDispatchDelay waits until the server no longer reports a future dispatch deadline.
// NextAttemptScheduleTime disappearing establishes only that the dispatch is due, not that its task
// reached Matching; a subsequent Poll proves that. Started is also success because it proves a racing
// poller consumed the dispatch. Any other state hides or removes the deadline, so cannot establish
// this trace event.
func awaitActivityDispatchDelay(
	ctx context.Context,
	t testing.TB,
	e model.Event,
	observe func(require.TestingT) (
		activityInProgress bool,
		runState enumspb.PendingActivityState,
		nextAttemptScheduleTime *timestamppb.Timestamp,
		details any,
	),
) {
	activityInProgress, runState, nextAttemptScheduleTime, details := observe(t)
	switch {
	case runState == enumspb.PENDING_ACTIVITY_STATE_STARTED:
		return
	case !activityInProgress || runState != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED:
		t.Errorf("%s: no delayed dispatch can elapse; last observed: %+v", e, details)
		return
	case nextAttemptScheduleTime == nil:
		return
	}

	deadline := nextAttemptScheduleTime.AsTime().Add(activityDriverTimerMargin)
	await.Require(ctx, t, func(t *await.T) {
		activityInProgress, runState, nextAttemptScheduleTime, details = observe(t)
		settled := !activityInProgress ||
			runState != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED ||
			nextAttemptScheduleTime == nil
		t.Require().Truef(settled, "%s: dispatch deadline is still pending; last observed: %+v", e, details)
	}, max(0, time.Until(deadline)), activityDriverPollInterval)
	if !activityInProgress ||
		(runState != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED &&
			runState != enumspb.PENDING_ACTIVITY_STATE_STARTED) {
		t.Errorf("%s: the activity stopped being in progress before the driver observed its delayed dispatch becoming due; last observed: %+v",
			e, details)
	}
}

func activityMarshalPayloads(p *commonpb.Payloads) []byte {
	if p == nil {
		return nil
	}
	b, err := p.Marshal()
	if err != nil {
		panic("marshaling payloads failed: " + err.Error())
	}
	return b
}
