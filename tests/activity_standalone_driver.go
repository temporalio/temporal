package tests

// Driver for standalone-activity (SAA) tests: it starts an activity and drives it through a
// sequence of events (a 'trace'). Each event is either a frontend RPC, a poll, or a timer
// wait. The event vocabulary is in chasm/lib/activity/model.

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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// --- the activity under test -----------------------------------------------------------------

// --- driver --------------------------------------------------------------------------------

type saaDriver struct {
	env              *testcore.TestEnv
	ctx              context.Context
	chasmCtx         context.Context // memoized by chasmContext
	cfg              activityConfig
	cfgIdx           int // labels this driver's config in the conformance explorer's logs
	numStarted       int
	activityIDPrefix string // activity-id prefix

	positivePollTimeout time.Duration // bounds a "must dispatch" poll; 0 => activityDriverTimeout

	// customizeStart mutates the StartActivityExecutionRequest before it is sent.
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

// newSAADriver builds a driver.
func newSAADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *saaDriver {
	return &saaDriver{
		env:              env,
		ctx:              testcontext.For(t),
		cfg:              cfg,
		activityIDPrefix: t.Name(),
	}
}

// saaPollTimeout is a poll timeout above common.MinLongPollTimeout, the floor below which the frontend
// rejects the poll rather than reaching matching.
const saaPollTimeout = common.MinLongPollTimeout + time.Second

// saaHandle is a handle to an activity instance.
type saaHandle struct {
	cursor        *activityModelCursor // the model state reached, so driveEvent can check each event
	cfg           activityConfig       // d.cfg with the windows this trace needs; see activityConfig.forTrace
	d             *saaDriver
	activityID    string
	runID         string
	taskQueue     string
	token         []byte
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
	// establishedReqID[eventType] is the request id that established the current state for an operator
	// command; a SameRequestID event reuses it. lastReqID is the most recent operator RPC's id, promoted
	// into establishedReqID by apply when that RPC changes state.
	establishedReqID map[model.EventType]string
	lastReqID        string
	path             []model.Event // events driven to reach the edge under test, for failure reports

	// Raw stamps, shifted cur->prev by each observed() read; see checkTaskInvalidation.
	prevStamp, curStamp       int32
	prevSTCStamp, curSTCStamp int32
}

// driveTrace schedules an activity, and then advances that activity through a sequence of events (a
// 'trace'). Returns a handle to the activity at the reached state.
func (d *saaDriver) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
	cfg := d.cfg.forTrace(trace)
	a := d.start(t, cfg)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *saaHandle) driveEvent(t require.TestingT, e model.Event) {
	a.cursor.check(t, e)
	d := a.d
	switch {
	case e.Type == model.PollType:
		timeout := cmp.Or(d.positivePollTimeout, activityDriverTimeout)
		resp := a.pollForTask(t, timeout)
		require.NotNilf(t, resp, "%s: no task was dispatched within %s", e, timeout)
		a.token = resp.GetTaskToken()
	case isDispatchDelayEvent(e.Type):
		a.awaitDispatchDelay(t, e)
	case isTimerEvent(e.Type):
		a.awaitTimeout(t, e, time.Now().Add(a.cfg.timerDuration(e)+activityDriverTimerMargin))
	default:
		// An RPC
		require.NoError(t, a.rpc(e))
	}
}

// awaitTimeout blocks until the activity reports the timeout the event names, and fails if it does
// not within (window + margin). Waiting for the activity to change instead accepts a different
// timeout firing, and misses one that fired before the wait began.
//
// A timeout ends an attempt, so a reported type only belongs to this event if the activity has
// moved on since the wait began, or has closed and so can report nothing further. Otherwise the
// same type left over from an earlier attempt would satisfy the wait at once.
func (a *saaHandle) awaitTimeout(t require.TestingT, e model.Event, deadline time.Time) {
	want := timeoutType(e)
	before := a.timeoutMark(t)
	var got activityTimeoutMark
	fired := func() bool {
		got = a.timeoutMark(t)
		return got.reports(want) && (got.closed || got != before)
	}
	if activityDriverPollUntil(deadline, fired) {
		return
	}
	t.Errorf("%s: the activity did not report a %s timeout within %s of driving the event; it reports %+v. "+
		"Check that the config makes this the timeout that fires.",
		e, want, a.cfg.timerDuration(e)+activityDriverTimerMargin, got)
}

// timeoutMark is the most recent timeout the activity reports, with the attempt and closed-ness that
// place it in the activity's history.
func (a *saaHandle) timeoutMark(t require.TestingT) activityTimeoutMark {
	r := a.describe(t)
	outcome := r.GetOutcome().GetFailure()
	return activityTimeoutMark{
		attemptFailure: timeoutTypeOf(r.GetInfo().GetLastFailure()),
		outcome:        timeoutTypeOf(outcome),
		cause:          timeoutTypeOf(outcome.GetCause()),
		attempt:        r.GetInfo().GetAttempt(),
		closed:         r.GetInfo().GetStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
	}
}

// awaitDispatchTimePassed polls the activity until the delayed dispatch is no longer pending, and
// fails if it is still pending, or if the activity ended first and so never dispatched at all.
func (a *saaHandle) awaitDispatchDelay(t require.TestingT, e model.Event) {
	info := a.describe(t).GetInfo()
	deadline := time.Now().Add(activityDriverTimerMargin)
	if next := info.GetNextAttemptScheduleTime(); next != nil {
		deadline = next.AsTime().Add(activityDriverTimerMargin)
	}
	settled := func() bool {
		info = a.describe(t).GetInfo()
		return info.GetStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING ||
			info.GetRunState() == enumspb.PENDING_ACTIVITY_STATE_STARTED ||
			info.GetNextAttemptScheduleTime() == nil
	}
	if !activityDriverPollUntil(deadline, settled) {
		t.Errorf("%s: a dispatch is still pending %s after the time the server scheduled it for. "+
			"Last observed: %+v", e, activityDriverTimerMargin, saaActivityInfo(info))
		return
	}
	switch {
	case info.GetStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING:
		t.Errorf("%s: the activity ended as %s before its delayed dispatch, so the dispatch never happened",
			e, info.GetStatus())
	case info.GetRunState() == enumspb.PENDING_ACTIVITY_STATE_STARTED:
		t.Errorf("%s: an attempt is running, so no dispatch is pending and none can elapse", e)
	}
}

func (d *saaDriver) start(t require.TestingT, cfg activityConfig) *saaHandle {
	d.numStarted++
	id := fmt.Sprintf("%s-%d", d.activityIDPrefix, d.numStarted)
	resp, err := d.env.FrontendClient().StartActivityExecution(d.ctx, d.startRequest(cfg, id, id))
	require.NoError(t, err)
	return &saaHandle{d: d, cfg: cfg, cursor: newActivityModelCursor(cfg), activityID: id, taskQueue: id, runID: resp.RunId, establishedReqID: map[model.EventType]string{}}
}

func (d *saaDriver) startRequest(c activityConfig, activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	opt := func(v time.Duration) *durationpb.Duration {
		if v == 0 {
			return nil
		}
		return durationpb.New(v)
	}
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:              d.env.Namespace().String(),
		ActivityId:             activityID,
		ActivityType:           d.env.Tv().ActivityType(),
		Identity:               d.env.Tv().ClientIdentity(),
		Input:                  payloads.EncodeString(activityInput),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout:    durationpb.New(c.startToClose()),
		ScheduleToCloseTimeout: opt(c.ScheduleToClose),
		ScheduleToStartTimeout: opt(c.ScheduleToStart),
		HeartbeatTimeout:       opt(c.HeartbeatTimeout),
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
	if d.customizeStart != nil {
		d.customizeStart(req)
	}
	return req
}

// describe returns the DescribeActivityExecution response, including the outcome, the last failure,
// and the heartbeat details.
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

// activityInfo is the activity's ActivityExecutionInfo, projected down to a schema shared with
// workflow activity.
func (a *saaHandle) activityInfo(t require.TestingT) activityInfo {
	return saaActivityInfo(a.describe(t).GetInfo())
}

// terminal is the terminal status from Info plus the failure discriminant from the Outcome.
func (a *saaHandle) terminal(t require.TestingT) activityTerminalProjection {
	resp := a.awaitTerminal(t)
	return activityTerminalProjection{
		Status:      resp.GetInfo().GetStatus(),
		FailureType: saaFailureType(resp.GetOutcome().GetFailure()),
	}
}

// terminalStatus waits for the activity to reach a terminal state and reports it.
// PollActivityExecution resolves once the activity is no longer running. An empty response means the
// server's long-poll window expired, so resubmit.
func (a *saaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	return a.terminal(t).Status
}

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none.
func (a *saaHandle) terminalCause(t require.TestingT) failureCause {
	cause := a.awaitTerminal(t).GetOutcome().GetFailure().GetCause()
	return failureCause{Type: saaFailureType(cause), Message: cause.GetMessage()}
}

// awaitTerminal waits for the activity to stop running and then describes it. Neither the terminal
// status nor the Outcome is settled before then, so reading either without waiting reports whatever the
// activity happens to be doing. PollActivityExecution is the long poll that resolves once it is no
// longer running; it returns an empty response when its window expires, so resubmit. Each poll is
// bounded by the deadline.
func (a *saaHandle) awaitTerminal(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	deadline := time.Now().Add(activityDriverTimeout)
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
			return a.describe(t)
		}
	}
	t.Errorf("the activity did not reach a terminal status within %s of the trace finishing. Last observed: %+v",
		activityDriverTimeout, a.activityInfo(t))
	return a.describe(t)
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes.
func (a *saaHandle) heartbeatDetails(t require.TestingT) []byte {
	return firstPayloadData(a.describe(t).GetInfo().GetHeartbeatDetails())
}

// saaFailureType is the application failure Type, the TimeoutType string, or "" for neither.
func saaFailureType(f *failurepb.Failure) string {
	if app := f.GetApplicationFailureInfo(); app != nil {
		return app.GetType()
	}
	if to := f.GetTimeoutFailureInfo(); to != nil {
		return to.GetTimeoutType().String()
	}
	return ""
}

func saaActivityInfo(i *apiactivitypb.ActivityExecutionInfo) activityInfo {
	return activityInfo{
		RunState:                   i.GetRunState(),
		Attempt:                    i.GetAttempt(),
		CurrentRetryInterval:       i.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: i.GetNextAttemptScheduleTime() != nil,
	}
}

// rpc performs the frontend RPC for a non-Poll, non-timer event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.HeartbeatType:
		resp, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityHeartbeatDetails,
		})
		a.lastHeartbeat = resp
		return err
	case model.RespondCompletedType:
		_, err := fc.RespondActivityTaskCompleted(a.d.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RespondFailedType:
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(), Failure: activityFailure(e.Retryable, a.cfg.NextRetryDelay),
		})
		return err
	case model.RespondCanceledType:
		_, err := fc.RespondActivityTaskCanceled(a.d.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RequestCancelType:
		_, err := fc.RequestCancelActivityExecution(a.d.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.TerminateType:
		_, err := fc.TerminateActivityExecution(a.d.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptionsType:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaDriver: unhandled event type %v", e.Type)
	}
}

func (a *saaHandle) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
	}
	switch {
	case e.RestoreOriginal:
		req.RestoreOriginal = true
	case e.SetsStartDelay:
		req.ActivityOptions = &apiactivitypb.ActivityOptions{StartDelay: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}}
	default:
		// A minimal, always-valid update: re-set the heartbeat timeout.
		req.ActivityOptions = &apiactivitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	_, err := a.d.env.FrontendClient().UpdateActivityExecutionOptions(a.d.ctx, req)
	return err
}

// reqID is the request id for an operator command: the id that established the current state for that
// command type if the event is a SameRequestID replay, else a fresh one. It is recorded as lastReqID.
func (a *saaHandle) reqID(e model.Event) string {
	id := uuid.NewString()
	if e.SameRequestID {
		if est, ok := a.establishedReqID[e.Type]; ok {
			id = est
		}
	}
	a.lastReqID = id
	return id
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
