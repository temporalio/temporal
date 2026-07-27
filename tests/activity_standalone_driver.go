package tests

// Driver for standalone-activity (SAA) tests: starts an activity and drives it through a sequence
// of events (a trace). Each event is either a frontend RPC, a poll, or a wall-clock wait. The event
// vocabulary is in chasm/lib/activity/model.

import (
	"cmp"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// activityConfig is the activity a driver starts. One value configures either surface, so a parity
// test describes a single activity rather than two that might differ.
//
// Every field is the value it names. A zero duration leaves that option unset, which for a timeout
// means it never fires; the exceptions are noted.
//
// Timeouts are usually left unset: forTrace gives a short window to each one the trace fires, so
// writing model.HeartbeatElapses is itself the statement that this activity has a heartbeat timeout.
// Set one explicitly only to say something the trace cannot — that it exists without firing, or that
// its exact duration is what the test is about.
type activityConfig struct {
	MaxAttempts            int32         // RetryPolicy MaximumAttempts; 0 = unlimited
	RetryInterval          time.Duration // RetryPolicy InitialInterval; 0 => activityDefaultRetryInterval
	BackoffCoefficient     float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	MaxRetryInterval       time.Duration // RetryPolicy MaximumInterval; 0 => RetryInterval
	NextRetryDelay         time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	NonRetryableErrorTypes []string      // RetryPolicy NonRetryableErrorTypes

	StartToClose    time.Duration // 0 => activityLongTimeout, so it does not fire
	ScheduleToClose time.Duration
	ScheduleToStart time.Duration
	Heartbeat       time.Duration
	StartDelay      time.Duration // SAA only: WFA has no per-activity start delay
}

// activityParityDefaultInput is the payload the drivers start activities with. Its content is never asserted on.
var activityParityDefaultInput = payloads.EncodeString("Input")

// activityLongTimeout is a timeout long enough not to fire during a test.
const activityLongTimeout = time.Hour

// activityShortTimeout is a timeout short enough for a trace to wait out.
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

// --- driver --------------------------------------------------------------------------------

type saaDriver struct {
	env        *testcore.TestEnv
	ctx        context.Context
	cfg        activityConfig
	numStarted int
	idBase     string // activity-id prefix
}

// newSAADriver builds a driver with the test-scoped context and its own activity-id prefix.
func newSAADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *saaDriver {
	return &saaDriver{
		env:    env,
		ctx:    testcontext.For(t),
		cfg:    cfg,
		idBase: t.Name(),
	}
}

// activityDefaultRetryInterval is the RetryPolicy InitialInterval when a driver sets none.
const activityDefaultRetryInterval = 200 * time.Millisecond

// activityDriverPositivePollTimeout bounds a poll that must find a task.
const activityDriverPositivePollTimeout = 10 * time.Second

// activityDriverWallClockSettle is slack added to a wall-clock event's window when waiting for its effect.
const activityDriverWallClockSettle = 2 * time.Second

// activityDriverPollInterval is the gap between reads when polling for a wall-clock event's effect.
const activityDriverPollInterval = 100 * time.Millisecond

// activityDriverTerminalTimeout bounds the wait for an activity the trace has driven to a terminal status.
const activityDriverTerminalTimeout = 10 * time.Second

// saaHandle is a handle to one activity instance: the ids that address it, plus the token last
// dispatched to it.
type saaHandle struct {
	d          *saaDriver
	activityID string
	taskQueue  string
	runID      string
	token      []byte
}

// driveTrace runs a trace on a fresh activity and returns a handle at the reached state. Model-free:
// each RPC must succeed.
func (d *saaDriver) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
	validateTrace(t, trace)
	d.cfg = d.cfg.forTrace(trace)
	a := d.start(t)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *saaHandle) driveEvent(t require.TestingT, e model.Event) {
	switch {
	case e.Type == model.PollType:
		// A poll captures the dispatched task token. Every Poll a trace drives is a positive poll — the
		// activity is meant to be dispatchable — so finding no task is a failure, not a step to skip.
		resp := a.pollForTask(t, activityDriverPositivePollTimeout)
		require.NotNilf(t, resp, "%s: no task was dispatched within %s", e, activityDriverPositivePollTimeout)
		a.token = resp.GetTaskToken()
	case isWallClockEvent(e.Type):
		// A wall-clock event is realized by waiting out its configured window.
		a.awaitWallClock(t, e)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// awaitWallClock blocks until a wall-clock event's effect is visible, and fails if it is not within
// (window + settle). A timeout advances the transition-history version, so it is waited for with a long
// poll; a dispatch-delay elapse advances no version, so it is detected by NextAttemptScheduleTime
// clearing.
func (a *saaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	if isDispatchDelayEvent(e.Type) {
		a.awaitDispatchTimePassed(t, e)
		return
	}
	a.awaitStateTransition(t, e, time.Now().Add(a.d.cfg.window(e)+activityDriverWallClockSettle))
}

// awaitStateTransition long-polls DescribeActivityExecution until the transition-history version
// advances past the token's, and fails if none does by the deadline. An empty response means the
// server's long-poll window expired, so resubmit. Each poll is bounded by the deadline.
func (a *saaHandle) awaitStateTransition(t require.TestingT, e model.Event, deadline time.Time) {
	token := a.describe(t).GetLongPollToken()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(a.d.ctx, deadline)
		resp, err := a.d.env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     a.d.env.Namespace().String(),
			ActivityId:    a.activityID,
			RunId:         a.runID,
			LongPollToken: token,
		})
		cancel()
		if err != nil {
			if time.Now().Before(deadline) {
				require.NoError(t, err)
			}
			break // the deadline cancelled the long poll
		}
		if resp.GetInfo() != nil {
			return // non-empty: the state advanced
		}
	}
	t.Errorf("%s: the activity did not transition within %s of driving the event, so the event did not take "+
		"effect. Last observed: %+v", e, a.d.cfg.window(e)+activityDriverWallClockSettle, a.activityInfo(t))
}

// awaitDispatchTimePassed polls the public projection until the pending dispatch time has passed, and
// fails if it has not. The deadline is the server's own NextAttemptScheduleTime, not the configured
// window: under a non-constant backoff the two differ, and only the server knows which attempt is
// waiting.
func (a *saaHandle) awaitDispatchTimePassed(t require.TestingT, e model.Event) {
	info := a.describe(t).GetInfo()
	next := info.GetNextAttemptScheduleTime()
	if next == nil {
		return // the dispatch time has already passed
	}
	deadline := next.AsTime().Add(activityDriverWallClockSettle)
	p := saaActivityInfo(info)
	if activityDriverPollUntil(deadline, func() bool { p = a.activityInfo(t); return !p.NextAttemptScheduleTimeSet }) {
		return
	}
	t.Errorf("%s: a dispatch is still pending %s after the time the server scheduled it for, so the "+
		"window did not elapse. Last observed: %+v", e, activityDriverWallClockSettle, p)
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

func (d *saaDriver) start(t require.TestingT) *saaHandle {
	d.numStarted++
	id := fmt.Sprintf("%s-%d", d.idBase, d.numStarted)
	resp, err := d.env.FrontendClient().StartActivityExecution(d.ctx, d.startRequest(id, id))
	require.NoError(t, err)
	return &saaHandle{d: d, activityID: id, taskQueue: id, runID: resp.RunId}
}

func (d *saaDriver) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	c := d.cfg
	opt := func(v time.Duration) *durationpb.Duration {
		if v == 0 {
			return nil
		}
		return durationpb.New(v)
	}
	return &workflowservice.StartActivityExecutionRequest{
		Namespace:              d.env.Namespace().String(),
		ActivityId:             activityID,
		ActivityType:           d.env.Tv().ActivityType(),
		Identity:               d.env.Tv().ClientIdentity(),
		Input:                  activityParityDefaultInput,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout:    durationpb.New(c.startToClose()),
		ScheduleToCloseTimeout: opt(c.ScheduleToClose),
		ScheduleToStartTimeout: opt(c.ScheduleToStart),
		HeartbeatTimeout:       opt(c.Heartbeat),
		StartDelay:             opt(c.StartDelay),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(c.retryInterval()),
			BackoffCoefficient:     cmp.Or(c.BackoffCoefficient, 1.0),
			MaximumInterval:        durationpb.New(cmp.Or(c.MaxRetryInterval, c.retryInterval())),
			MaximumAttempts:        c.MaxAttempts,
			NonRetryableErrorTypes: c.NonRetryableErrorTypes,
		},
		RequestId: uuid.NewString(),
	}
}

// describe returns the DescribeActivityExecution response, including the outcome, the last failure, and
// the heartbeat details.
func (a *saaHandle) describe(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := a.d.env.FrontendClient().DescribeActivityExecution(a.d.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:               a.d.env.Namespace().String(),
		ActivityId:              a.activityID,
		RunId:                   a.runID,
		IncludeOutcome:          true,
		IncludeLastFailure:      true,
		IncludeHeartbeatDetails: true,
	})
	require.NoError(t, err)
	return resp
}

// activityInfo is the activity's ActivityExecutionInfo, projected.
func (a *saaHandle) activityInfo(t require.TestingT) activityInfo {
	return saaActivityInfo(a.describe(t).GetInfo())
}

// terminalStatus waits for the activity to reach a terminal status and reports it.
// PollActivityExecution is SAA's counterpart to waiting on the workflow result: it resolves once the
// activity is no longer running. An empty response means the server's long-poll window expired, so
// resubmit. Each poll is bounded by the deadline.
func (a *saaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	deadline := time.Now().Add(activityDriverTerminalTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(a.d.ctx, deadline)
		resp, err := a.d.env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  a.d.env.Namespace().String(),
			ActivityId: a.activityID,
			RunId:      a.runID,
		})
		cancel()
		if err != nil {
			if time.Now().Before(deadline) {
				require.NoError(t, err)
			}
			break // the deadline cancelled the long poll
		}
		if resp.GetRunId() != "" {
			return a.describe(t).GetInfo().GetStatus()
		}
	}
	t.Errorf("the activity did not reach a terminal status within %s of the trace finishing. Last observed: %+v",
		activityDriverTerminalTimeout, a.activityInfo(t))
	return enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
}

func saaActivityInfo(i *apiactivitypb.ActivityExecutionInfo) activityInfo {
	return activityInfo{
		RunState:                   i.GetRunState(),
		Attempt:                    i.GetAttempt(),
		CurrentRetryInterval:       i.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: i.GetNextAttemptScheduleTime() != nil,
	}
}

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.RespondFailedType:
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(), Failure: activityFailure(e.Retryable, a.d.cfg.NextRetryDelay),
		})
		return err
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	default:
		return fmt.Errorf("saaDriver: unhandled event type %v", e.Type)
	}
}

func (a *saaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.d.ctx, timeout)
	defer cancel()
	resp, err := a.d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue},
		Identity:  a.d.env.Tv().WorkerIdentity(),
	})
	// Matching signals "waited, found nothing" with an empty response and a nil error, so any error
	// means the poll did not complete cleanly.
	if err != nil {
		if a.d.ctx.Err() != nil {
			return nil // teardown
		}
		if deadline, ok := a.d.ctx.Deadline(); ok && time.Until(deadline) < common.MinLongPollTimeout {
			t.Errorf("saaDriver: test context budget exhausted before the poll could run (%.1fs left, need >= %s). "+
				"Raise TEMPORAL_TEST_TIMEOUT and `go test -timeout`.\n  %v",
				time.Until(deadline).Seconds(), common.MinLongPollTimeout, err)
			return nil
		}
		t.Errorf("saaDriver bug: PollActivityTaskQueue did not complete cleanly (poll timeout must be >= "+
			"MinLongPollTimeout; only an empty response with a nil error means \"no task\"): %v", err)
		return nil
	}
	if resp.GetActivityId() == "" {
		return nil // no task available
	}
	return resp
}

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

// validateTrace rejects a trace the drivers cannot realize. Timeouts run concurrently, from deadlines
// the server anchors at schedule or attempt-start time, while the driver waits each one out from the
// moment its event is driven — so a trace can name at most one. Once the first fires, the others are no
// longer running and the driver would wait for something that never happens.
//
// Dispatch delays are exempt: each backoff is a fresh window, and awaitDispatchTimePassed takes its
// deadline from the server rather than from the trace.
//
// A rule of thumb, not a decision procedure. The model decides this per event and per state, and
// replaces this once it lands here.
func validateTrace(t require.TestingT, trace []model.Event) {
	var timeouts []model.Event
	for _, e := range trace {
		if isWallClockEvent(e.Type) && !isDispatchDelayEvent(e.Type) {
			timeouts = append(timeouts, e)
		}
	}
	require.LessOrEqualf(t, len(timeouts), 1,
		"a trace can name at most one timeout: they run concurrently, so once the first fires the rest "+
			"cannot occur. This one names %v", timeouts)
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
