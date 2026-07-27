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
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type saaDriver struct {
	env              *testcore.TestEnv
	ctx              context.Context
	cfg              activityConfig
	numStarted       int
	activityIDPrefix string
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

// saaHandle is a handle to an activity instance.
type saaHandle struct {
	d          *saaDriver
	cfg        activityConfig // d.cfg with the windows this trace needs; see activityConfig.forTrace
	activityID string
	runID      string
	taskQueue  string
	token      []byte
}

// driveTrace schedules an activity, and then advances that activity through a sequence of events (a
// 'trace'). Returns a handle to the activity at the reached state.
func (d *saaDriver) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
	validateTrace(t, trace)
	a := d.start(t, d.cfg.forTrace(trace))
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *saaHandle) driveEvent(t require.TestingT, e model.Event) {
	switch {
	case e.Type == model.PollType:
		// When a trace includes a poll event, the implication is that the activity should be
		// dispatchable and that the poll will yield an activity task, so finding no task is a
		// failure.
		resp := a.pollForTask(t, activityDriverTimeout)
		require.NotNilf(t, resp, "%s: no task was dispatched within %s", e, activityDriverTimeout)
		a.token = resp.GetTaskToken()
	case isTimerEvent(e.Type):
		// A timer event is realized by waiting out its configured window.
		a.awaitTimerEvent(t, e)
	default:
		// An RPC
		require.NoError(t, a.rpc(e))
	}
}

// awaitTimerEvent blocks until a timer event's effect is visible, and fails if it does not
// become visible within (window + margin).
func (a *saaHandle) awaitTimerEvent(t require.TestingT, e model.Event) {
	if isDispatchDelayEvent(e.Type) {
		a.awaitDispatchDelay(t, e)
		return
	}
	a.awaitTimeout(t, e, time.Now().Add(a.cfg.timerDuration(e)+activityDriverTimerMargin))
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
		return got.timeout == want && (got.closed || got != before)
	}
	if activityDriverPollUntil(deadline, fired) {
		return
	}
	t.Errorf("%s: the activity did not report a %s timeout within %s of driving the event; it reports %s. "+
		"Check that the config makes this the timeout that fires.",
		e, want, a.cfg.timerDuration(e)+activityDriverTimerMargin, got.timeout)
}

// timeoutMark is the most recent timeout the activity reports, with the attempt and closed-ness that
// place it in the activity's history.
func (a *saaHandle) timeoutMark(t require.TestingT) activityTimeoutMark {
	i := a.describe(t).GetInfo()
	return activityTimeoutMark{
		timeout: i.GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType(),
		attempt: i.GetAttempt(),
		closed:  i.GetStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
	}
}

// awaitDispatchDelay polls the activity until the delayed dispatch is no longer pending, and
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
	return &saaHandle{d: d, cfg: cfg, activityID: id, runID: resp.RunId, taskQueue: id}
}

func (d *saaDriver) startRequest(c activityConfig, activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
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

// terminalStatus waits for the activity to reach a terminal state and reports it.
// PollActivityExecution resolves once the activity is no longer running. An empty response means the
// server's long-poll window expired, so resubmit.
func (a *saaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
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
			return a.describe(t).GetInfo().GetStatus()
		}
	}
	t.Errorf("the activity did not reach a terminal status within %s of the trace finishing. Last observed: %+v",
		activityDriverTimeout, a.activityInfo(t))
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

// rpc performs the frontend RPC for a non-Poll, non-timer event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.RespondFailedType:
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(), Failure: activityFailure(e.Retryable, a.cfg.NextRetryDelay),
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
