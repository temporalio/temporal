package tests

// Config shared by activity_standalone_driver.go and activity_workflow_driver.go.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/retrypolicy"
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
// Start-to-close defaults long enough not to fire unless DisableStartToClose is set.
// The other timeouts are absent when unset.
type activityConfig struct {
	EagerStart             bool
	MaxAttempts            int32         // RetryPolicy MaximumAttempts; 0 = unlimited
	RetryInterval          time.Duration // RetryPolicy InitialInterval; 0 => activityShortRetryInterval
	BackoffCoefficient     float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	MaxRetryInterval       time.Duration // RetryPolicy MaximumInterval; 0 => RetryInterval
	NextRetryDelay         time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	NonRetryableErrorTypes []string      // RetryPolicy NonRetryableErrorTypes
	HandoffOptions         *activitypb.HandoffOptions

	StartToClose     time.Duration // 0 => activityLongDuration, so it does not fire
	ScheduleToClose  time.Duration // 0 = unset
	ScheduleToStart  time.Duration // 0 = unset
	HeartbeatTimeout time.Duration // 0 = unset
	StartDelay       time.Duration // SAA only: WFA has no per-activity start delay

	// Leaves start-to-close unset, allowing tests to rely on the handoff phase timeouts alone.
	DisableStartToClose bool
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
	if c.DisableStartToClose {
		return 0
	}

	return cmp.Or(c.StartToClose, activityLongDuration)
}

// forTrace replaces missing values in the config with appropriate values for the given trace.
func (c activityConfig) forTrace(trace []model.Event) activityConfig {
	c.HandoffOptions = common.CloneProto(c.HandoffOptions)
	awaitingCompletion := false
	for _, e := range trace {
		switch e.Type {
		case model.ScheduleToStartElapsesType:
			c.ScheduleToStart = cmp.Or(c.ScheduleToStart, activityShortTimeout)
		case model.ScheduleToCloseElapsesType:
			c.ScheduleToClose = cmp.Or(c.ScheduleToClose, activityShortTimeout)
		case model.StartToCloseElapsesType:
			c.StartToClose = cmp.Or(c.StartToClose, activityShortTimeout)
		case model.HeartbeatElapsesType:
			usePreHandoffHeartbeat := c.HandoffOptions != nil && !awaitingCompletion
			if usePreHandoffHeartbeat {
				if c.HandoffOptions.GetHeartbeatTimeout().AsDuration() == 0 {
					c.HandoffOptions.HeartbeatTimeout = durationpb.New(activityShortTimeout)
				}
			} else {
				c.HeartbeatTimeout = cmp.Or(c.HeartbeatTimeout, activityShortTimeout)
			}
		case model.StartToHandoffElapsesType:
			if c.HandoffOptions != nil && c.HandoffOptions.GetStartToHandoffTimeout().AsDuration() == 0 {
				c.HandoffOptions.StartToHandoffTimeout = durationpb.New(activityShortTimeout)
			}
		case model.HandoffToCloseElapsesType:
			if c.HandoffOptions != nil && c.HandoffOptions.GetHandoffToCloseTimeout().AsDuration() == 0 {
				c.HandoffOptions.HandoffToCloseTimeout = durationpb.New(activityShortTimeout)
			}
		case model.HandoffType:
			awaitingCompletion = true
		case model.PollType:
			awaitingCompletion = false
		case model.StartDelayElapsesType:
			c.StartDelay = cmp.Or(c.StartDelay, activityShortDispatchDelay)
		default: // an event that arms no window of its own
		}
	}
	return c
}

// timerDuration is how long the timer behind a timer event takes to elapse.
func (c activityConfig) timerDuration(e model.Event, awaitingCompletion bool) time.Duration {
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
		usePreHandoffHeartbeat := c.HandoffOptions != nil && !awaitingCompletion
		if usePreHandoffHeartbeat {
			return c.HandoffOptions.GetHeartbeatTimeout().AsDuration()
		}
		return c.HeartbeatTimeout
	case model.StartToHandoffElapsesType:
		return c.HandoffOptions.GetStartToHandoffTimeout().AsDuration()
	case model.HandoffToCloseElapsesType:
		return c.HandoffOptions.GetHandoffToCloseTimeout().AsDuration()
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

// activityHandoffInfo is handoff metadata projected from an activity's Describe response.
type activityHandoffInfo struct {
	handoffTime            *timestamppb.Timestamp
	options                *activitypb.HandoffOptions
	awaitingCompletion     bool
	startToCloseTimeout    time.Duration
	scheduleToCloseTimeout time.Duration
}

// activityFailureInfo projects retained failure metadata and terminal outcomes after closure.
type activityFailureInfo struct {
	attempt         int32
	handoffAttempt  int32
	lastFailure     *failurepb.Failure
	recordedOutcome *activitypb.ActivityExecutionOutcome
	resultOutcome   *activitypb.ActivityExecutionOutcome
}

// activityTerminalOutcome is user-visible terminal activity state projected from SAA's
// ActivityExecutionOutcome and WFA's workflow result.
type activityTerminalOutcome struct {
	status     enumspb.ActivityExecutionStatus
	retryState enumspb.RetryState
}

// modelConfig is the model's view of the activity: which options are configured at all, plus the two
// ways the options alone settle that no retry can follow. Deriving it means the two cannot disagree.
func (c activityConfig) modelConfig() model.Config {
	handoffRetryInterval := cmp.Or(c.HandoffOptions.GetRetryPolicy().GetInitialInterval().AsDuration(),
		retrypolicy.DefaultDefaultRetrySettings.InitialInterval)

	return model.Config{
		InitialAttemptStarted:       c.EagerStart,
		MaxAttempts:                 c.MaxAttempts,
		HasStartDelay:               c.StartDelay > 0,
		HasScheduleToClose:          c.ScheduleToClose > 0,
		HasScheduleToStart:          c.ScheduleToStart > 0,
		HasHeartbeat:                c.HeartbeatTimeout > 0,
		NonRetryableTimeouts:        nonRetryableTimeouts(c.NonRetryableErrorTypes),
		HandoffOptions:              c.HandoffOptions != nil,
		HasStartToClose:             c.startToClose() > 0,
		HasStartToHandoff:           c.HandoffOptions.GetStartToHandoffTimeout().AsDuration() > 0,
		HasHandoffToClose:           c.HandoffOptions.GetHandoffToCloseTimeout().AsDuration() > 0,
		HasHandoffHeartbeat:         c.HandoffOptions.GetHeartbeatTimeout().AsDuration() > 0,
		HandoffMaxAttempts:          c.HandoffOptions.GetRetryPolicy().GetMaximumAttempts(),
		HandoffNonRetryableTimeouts: nonRetryableTimeouts(c.HandoffOptions.GetRetryPolicy().GetNonRetryableErrorTypes()),

		// In reality, the server decides this based on how long the attempt took, and any
		// NextRetryDelay sent by the worker. The model cannot express that. Instead it supports a
		// fixed retry delay and the calculation here pretends that the attempt duration is zero.
		// This will be wrong if an attempt runs long enough that the delay no longer fits.
		RetryOutlivesScheduleToClose: c.ScheduleToClose > 0 &&
			cmp.Or(c.NextRetryDelay, c.retryInterval()) >= c.ScheduleToClose,
		HandoffRetryOutlivesScheduleToClose: c.HandoffOptions != nil && c.ScheduleToClose > 0 &&
			cmp.Or(c.NextRetryDelay, handoffRetryInterval) >= c.ScheduleToClose,
	}
}

// nonRetryableTimeouts returns the internal timeout event types that should not be retried
// according to the retry policy's NonRetryableErrorTypes. (NonRetryableErrorTypes can contain
// arbitrary strings, matched against ApplicationFailureInfo.Type, and the names of internal timeout
// events, prefixed by "TemporalTimeout:").
func nonRetryableTimeouts(errorTypes []string) []model.EventType {
	var nonRetryable []model.EventType
	for _, e := range []model.Event{
		// ScheduleToClose and ScheduleToStart are always non-retryable, so it makes no difference
		// if the user includes them in this list.
		model.ScheduleToStartElapses, model.ScheduleToCloseElapses, model.StartToCloseElapses, model.HeartbeatElapses,
		model.StartToHandoffElapses, model.HandoffToCloseElapses,
	} {
		timeout := timeoutType(e)
		shortName := retrypolicy.TimeoutFailureTypePrefix + timeout.String()
		protoName := retrypolicy.TimeoutFailureTypePrefix + enumspb.TimeoutType_name[int32(timeout)]

		if slices.Contains(errorTypes, shortName) || slices.Contains(errorTypes, protoName) {
			nonRetryable = append(nonRetryable, e.Type)
		}
	}
	return nonRetryable
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
// TIMEOUT_TYPE_UNSPECIFIED for any other event. The model does not use API types, so the mapping
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
	case model.StartToHandoffElapsesType:
		return enumspb.TIMEOUT_TYPE_START_TO_HANDOFF
	case model.HandoffToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE
	default:
		return enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}
}

// activityModel is the model state a driver has reached, so that driveEvent can check each event
// against the state it is driven from. It replaces a rule of thumb about which traces are realizable
// with the model's decision, per event and per state: a timeout event whose clock is not running
// cannot occur, so a driver waiting for it would wait for something that never happens.
type activityModel struct {
	cfg   model.Config
	state model.AbstractState
}

func newActivityModel(cfg activityConfig) *activityModel {
	mc := cfg.modelConfig()
	return &activityModel{cfg: mc, state: model.Initial(mc)}
}

// advance moves the model past e, failing if e cannot occur in the state reached so far. It returns
// the error kind the model requires the server to answer e with.
func (m *activityModel) advance(t require.TestingT, e model.Event) model.ErrorKind {
	out := model.Transition(m.cfg, m.state, e)
	if out.ImpossibleTimeWindowElapse {
		require.Failf(t, "the trace drives an event that cannot occur",
			"%s cannot occur in %v/%v: its clock is not running there. Remove it, or drive the events "+
				"that start its clock first.", e, m.state.Status, m.state.Dispatchability)
		return model.NoError
	}
	// A Poll in a trace is there to start an attempt; one that finds no task asserts nothing.
	if e.Type == model.PollType && !model.FindsTask(m.state) {
		require.Failf(t, "the trace drives a Poll that finds no task",
			"nothing is dispatchable in %v/%v. Remove the Poll, or drive the events that make an "+
				"attempt dispatchable first.", m.state.Status, m.state.Dispatchability)
		return model.NoError
	}
	m.state = out.Next
	return out.Reject
}

// isTimerEvent reports whether an event represents a timer elapsing, as opposed to an RPC.
func isTimerEvent(et model.EventType) bool {
	switch et {
	case model.ScheduleToStartElapsesType, model.ScheduleToCloseElapsesType, model.StartToCloseElapsesType,
		model.HeartbeatElapsesType, model.StartDelayElapsesType, model.BackoffElapsesType,
		model.StartToHandoffElapsesType, model.HandoffToCloseElapsesType:
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
		info := &failurepb.ApplicationFailureInfo{Type: "TestFailure", NonRetryable: e.Failure.NonRetryable}
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
			FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: e.Failure.NonRetryable}},
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
	timeout        enumspb.TimeoutType // Timeout type currently reported; unspecified when none is reported.
	outcomeTimeout enumspb.TimeoutType // Terminal outcome's timeout type, independent of LastFailure.
	attempt        int32               // Current attempt; advances on activity retries.
	terminal       bool                // Whether the activity is terminal, as every non-retrying timeout makes it.
	handoffAttempt int32               // Advances on handoff retries, which preserve the activity attempt.
}

// activityDriverState is the state shared by the two drivers.
type activityDriverState struct {
	cfg                   activityConfig
	token                 []byte
	lastTask              *workflowservice.PollActivityTaskQueueResponse
	startedAttempt        int32 // attempt number returned by the last successful Poll
	startedHandoffAttempt int32
}

// driverState lets an embedded activityDriverState supply its state to drivenActivity.
func (a *activityDriverState) driverState() *activityDriverState {
	return a
}

func (a *activityDriverState) taskResponse() *workflowservice.PollActivityTaskQueueResponse {
	return a.lastTask
}

// drivenActivity is what the shared event driver needs from either implementation.
type drivenActivity interface {
	driverState() *activityDriverState
	// testContext returns the driver's current test context, fetched fresh per
	// call rather than cached, so a later timeout extension is visible.
	testContext() context.Context
	pollForTask(require.TestingT, time.Duration) *workflowservice.PollActivityTaskQueueResponse
	awaitDispatchDelay(testing.TB, model.Event)
	timeoutInfo(require.TestingT) activityTimeoutInfo
	observedState(require.TestingT) activityState
	rpc(testing.TB, model.Event) error
}

// driveActivityEvent advances an activity by one event, holding the server to the model on both
// halves of the contract: the answer it gives the call, and the state it is left in.
func driveActivityEvent(t testing.TB, a drivenActivity, e model.Event, m *activityModel) {
	from := m.state.Status
	awaitingCompletion := m.state.AwaitingCompletion
	wantReject := m.advance(t, e)
	state := a.driverState()
	switch {
	case e.Type == model.PollType:
		resp := a.pollForTask(t, activityDriverTimeout)
		require.NotNilf(t, resp, "%s: no task was dispatched within %s", e, activityDriverTimeout)
		require.Equal(t, m.state.AttemptCount, resp.GetAttempt())
		require.Equal(t, m.state.HandoffAttempt, resp.GetHandoffAttempt())

		state.token = resp.GetTaskToken()
		state.lastTask = resp
		state.startedAttempt = resp.GetAttempt()
		state.startedHandoffAttempt = resp.GetHandoffAttempt()
	case isDispatchDelayEvent(e.Type):
		a.awaitDispatchDelay(t, e)
	case isTimerEvent(e.Type):
		awaitActivityTimeout(t, a, e, time.Now().Add(state.cfg.timerDuration(e, awaitingCompletion)+activityDriverTimerMargin), from == model.ResetRequested)
	default:
		requireErrorMatches(t, e, from, wantReject, a.rpc(t, e))
	}
	requireStateMatches(t, a, e, from, m.state)
}

// activityState is the state a driver observes, reduced to what both implementations report: the run
// state, attempt counters and handoff phase while the activity is open, and closedness once it is
// not. A terminal status itself is reported differently by each, so terminalOutcome checks that.
type activityState struct {
	closed             bool
	runState           enumspb.PendingActivityState
	attempt            int32
	handoffAttempt     int32
	awaitingCompletion bool
}

// requireStateMatches compares the state the server reports with the state model.Transition says the
// event leaves the activity in. The read is retried, because an event's effect is not always visible
// by the time the call driving it returns.
func requireStateMatches(t testing.TB, a drivenActivity, e model.Event, from model.Status, s model.AbstractState) {
	t.Helper()
	want := activityState{closed: s.Status.Terminal()}
	if !want.closed {
		want.runState, want.attempt = expectedRunState(s), s.AttemptCount
		want.handoffAttempt, want.awaitingCompletion = s.HandoffAttempt, s.AwaitingCompletion
	}
	await.Require(a.testContext(), t, func(t *await.T) {
		t.Require().Equal(want, a.observedState(t),
			"after %s from %v, the state the server reports disagrees with the model", e, from)
	}, activityDriverTimeout, activityDriverPollInterval)
}

// expectedRunState is the PendingActivityState an open activity is reported in. The model does not
// use API types, so the mapping lives here.
func expectedRunState(s model.AbstractState) enumspb.PendingActivityState {
	switch s.Status {
	case model.Scheduled:
		return enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	case model.Started:
		if s.AwaitingCompletion {
			return enumspb.PENDING_ACTIVITY_STATE_HANDED_OFF
		}
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	case model.CancelRequested:
		return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	case model.PauseRequested:
		return enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
	case model.Paused:
		return enumspb.PENDING_ACTIVITY_STATE_PAUSED
	case model.ResetRequested:
		// A reset the worker has yet to yield to is reported as the pause it carries, and otherwise
		// as the current running phase.
		if s.ResetKeepPaused {
			return enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
		}
		if s.AwaitingCompletion {
			return enumspb.PENDING_ACTIVITY_STATE_HANDED_OFF
		}
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	default:
		panic("no run state is reported for status " + s.Status.String())
	}
}

// requireErrorMatches compares the error an RPC returned, or its absence, with the one
// model.Transition requires. The two implementations word a refusal differently, so the error kind is
// what they have to agree on.
func requireErrorMatches(t require.TestingT, e model.Event, from model.Status, want model.ErrorKind, err error) {
	got := activityRejectKind(err)
	if got == want {
		return
	}
	require.Failf(t, "the server's answer to an RPC disagrees with the model",
		"%s from %v: the model requires %s, the server gave %s (%v)", e, from, want, got, err)
}

// activityRejectKind classifies an RPC error as the model's ErrorKind. The FrontendClient returns
// serviceerror types, so this matches on type rather than on gRPC status code.
func activityRejectKind(err error) model.ErrorKind {
	if err == nil {
		return model.NoError
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return model.NotFound
	}
	if _, ok := errors.AsType[*serviceerror.FailedPrecondition](err); ok {
		return model.FailedPrecondition
	}
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return model.InvalidArgument
	}
	return model.ErrorKind(-1) // unrecognized, so it matches no predicted kind
}

// awaitActivityTimeout blocks until the activity reports the timeout for e, and fails if it does not
// within (window + margin).
func awaitActivityTimeout(t testing.TB, a drivenActivity, e model.Event, deadline time.Time, resetPending bool) {
	state := a.driverState()
	want := timeoutType(e)
	var got activityTimeoutInfo
	await.Require(a.testContext(), t, func(t *await.T) {
		got = a.timeoutInfo(t)
		retried := got.attempt > state.startedAttempt || got.handoffAttempt > state.startedHandoffAttempt
		resetApplied := false
		if resetPending && got.timeout == want {
			// A timeout can apply a reset without advancing either counter.
			runState := a.observedState(t).runState
			resetApplied = runState == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED || runState == enumspb.PENDING_ACTIVITY_STATE_PAUSED
		}

		fired := got.timeout == want && (got.terminal || retried || resetApplied)
		t.Require().Truef(fired,
			"%s: activity reports timeout %s at attempt %d/handoff %d (terminal=%v), want %s after attempt %d/handoff %d",
			e, got.timeout, got.attempt, got.handoffAttempt, got.terminal, want, state.startedAttempt, state.startedHandoffAttempt)
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
