package tests

// Driver for workflow-activity (WFA) tests: it drives an activity scheduled by a workflow through a
// sequence of events (a 'trace'). Each event is either a frontend RPC, a poll, or a timer
// wait. The event vocabulary is in chasm/lib/activity/model.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
)

func wfaActivityInfo(p *workflowpb.PendingActivityInfo) activityInfo {
	return activityInfo{
		RunState:                   p.GetState(),
		Attempt:                    p.GetAttempt(),
		CurrentRetryInterval:       p.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: p.GetNextAttemptScheduleTime() != nil,
	}
}

type wfaDriver struct {
	env *testcore.TestEnv
	ctx context.Context
	cfg activityConfig
}

// newWFADriver builds a driver. cfg.StartDelay is ignored: a workflow activity has no per-activity
// start delay.
func newWFADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *wfaDriver {
	return &wfaDriver{env: env, ctx: testcontext.For(t), cfg: cfg}
}

// wfaHandle is a handle to a workflow-scheduled activity.
type wfaHandle struct {
	d          *wfaDriver
	cfg        activityConfig // d.cfg with the windows this trace needs; see activityConfig.forTrace
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	activityTQ string
	token      []byte
}

// wfaActivityParams is what the helper workflow needs to schedule the activity: the activity the
// test described, and where to put it.
type wfaActivityParams struct {
	Cfg        activityConfig
	ActivityTQ string
	ActivityID string
}

// wfaCancelSignal makes the helper workflow cancel the activity, which is how a workflow activity is
// cancelled rather than by a direct RPC.
const wfaCancelSignal = "cancel"

// wfaSingleActivityWorkflow is a workflow that schedules a single activity with the given options
// on its own task queue and waits for it to finish. No worker executes the activity — the test
// drives it with worker poll RPCs. WaitForCancellation makes the workflow wait for
// RespondActivityTaskCanceled, so a cancelled activity reaches CANCELED before the workflow closes.
func wfaSingleActivityWorkflow(ctx workflow.Context, p wfaActivityParams) error {
	c := p.Cfg
	actCtx, cancelActivity := workflow.WithCancel(ctx)
	actCtx = workflow.WithActivityOptions(actCtx, workflow.ActivityOptions{
		TaskQueue:              p.ActivityTQ,
		ActivityID:             p.ActivityID,
		StartToCloseTimeout:    c.startToClose(),
		ScheduleToCloseTimeout: c.ScheduleToClose,
		ScheduleToStartTimeout: c.ScheduleToStart,
		HeartbeatTimeout:       c.HeartbeatTimeout,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        c.retryInterval(),
			BackoffCoefficient:     cmp.Or(c.BackoffCoefficient, 1.0),
			MaximumInterval:        cmp.Or(c.MaxRetryInterval, c.retryInterval()),
			MaximumAttempts:        c.MaxAttempts,
			NonRetryableErrorTypes: c.NonRetryableErrorTypes,
		},
	})
	fut := workflow.ExecuteActivity(actCtx, "testWFA", activityInput)
	workflow.Go(ctx, func(gctx workflow.Context) {
		workflow.GetSignalChannel(gctx, wfaCancelSignal).Receive(gctx, nil)
		cancelActivity()
	})
	return fut.Get(ctx, nil)
}

// driveTrace starts a workflow, which schedules an activity, and then advances that activity
// through a sequence of events (a 'trace'). Returns a handle to the activity at the reached state.
func (d *wfaDriver) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	validateTrace(t, trace)
	a := d.start(t, d.cfg.forTrace(trace))
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *wfaHandle) driveEvent(t require.TestingT, e model.Event) {
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
		a.awaitWallClock(t, e)
	default:
		// An RPC
		require.NoError(t, a.rpc(e))
	}
}

// awaitWallClock blocks until a timer event's effect shows up in the workflow's view of the
// activity, and fails if it does not within (window + settle).
func (a *wfaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	if isDispatchDelayEvent(e.Type) {
		a.awaitDispatchDelay(t, e)
		return
	}
	a.awaitTimeout(t, e, time.Now().Add(a.cfg.timerDuration(e)+activityDriverTimerMargin))
}

// awaitTimeout blocks until the activity reports the timeout the event names, and fails if it does
// not within (window + settle). See saaHandle.awaitTimeout.
func (a *wfaHandle) awaitTimeout(t require.TestingT, e model.Event, deadline time.Time) {
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

// timeoutMark is the most recent timeout the activity reports. A closed activity has left the pending
// set, so its timeout comes from the workflow result instead.
func (a *wfaHandle) timeoutMark(t require.TestingT) activityTimeoutMark {
	if pa := a.pendingActivity(t); pa != nil {
		return activityTimeoutMark{
			timeout: pa.GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType(),
			attempt: pa.GetAttempt(),
		}
	}
	var timeoutErr *temporal.TimeoutError
	if errors.As(a.run.Get(a.d.ctx, nil), &timeoutErr) {
		return activityTimeoutMark{timeout: timeoutErr.TimeoutType(), closed: true}
	}
	return activityTimeoutMark{closed: true}
}

// awaitDispatchDelay polls the activity until the delayed dispatch is no longer pending, and
// fails if it is still pending, or if the activity ended first and so never dispatched at all.
// See saaHandle.awaitDispatchDelay.
func (a *wfaHandle) awaitDispatchDelay(t require.TestingT, e model.Event) {
	pa := a.pendingActivity(t)
	deadline := time.Now().Add(activityDriverTimerMargin)
	if next := pa.GetNextAttemptScheduleTime(); next != nil {
		deadline = next.AsTime().Add(activityDriverTimerMargin)
	}
	for {
		switch {
		case pa == nil:
			t.Errorf("%s: the activity is no longer pending, so its delayed dispatch never happened", e)
			return
		case pa.GetState() == enumspb.PENDING_ACTIVITY_STATE_STARTED:
			t.Errorf("%s: an attempt is running, so no dispatch is pending and none can elapse", e)
			return
		case pa.GetNextAttemptScheduleTime() == nil:
			return
		case !time.Now().Before(deadline):
			t.Errorf("%s: a dispatch is still pending %s after the time the server scheduled it for. "+
				"Last observed: %+v", e, activityDriverTimerMargin, wfaActivityInfo(pa))
			return
		}
		time.Sleep(activityDriverPollInterval)
		pa = a.pendingActivity(t)
	}
}

// pendingActivity is the activity's entry in the workflow's pending set, nil once it is no longer
// pending.
func (a *wfaHandle) pendingActivity(t require.TestingT) *workflowpb.PendingActivityInfo {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return pa
		}
	}
	return nil
}

// pendingActivityInfo is the activityInfo if activity is currently a pending activity, and whether
// it is pending.
func (a *wfaHandle) pendingActivityInfo(t require.TestingT) (activityInfo, bool) {
	pa := a.pendingActivity(t)
	if pa == nil {
		return activityInfo{}, false
	}
	return wfaActivityInfo(pa), true
}

func (d *wfaDriver) start(t *testing.T, cfg activityConfig) *wfaHandle {
	wfTQ := testcore.RandomizeStr("wfa-wf")
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	// Run a workflow worker for the wrapper workflow, but not an activity worker: the tests poll
	// for activity tasks.
	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(wfaSingleActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	wfID := testcore.RandomizeStr("wfa-run")
	run, err := d.env.SdkClient().ExecuteWorkflow(d.ctx,
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: wfTQ},
		wfaSingleActivityWorkflow, wfaActivityParams{Cfg: cfg, ActivityTQ: actTQ, ActivityID: actID})
	require.NoError(t, err)
	a := &wfaHandle{d: d, cfg: cfg, run: run, workflowID: wfID, runID: run.GetRunID(), activityID: actID, activityTQ: actTQ}
	// The workflow schedules the activity, so it does not exist yet when ExecuteWorkflow returns.
	require.Truef(t, activityDriverPollUntil(time.Now().Add(activityDriverTimeout),
		func() bool { return a.pendingActivity(t) != nil }),
		"the workflow did not schedule its activity within %s", activityDriverTimeout)
	return a
}

// terminalStatus waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal status is not in PendingActivities, so it is read from the workflow-result error's cause.
func (a *wfaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	err := a.run.Get(a.d.ctx, nil)
	if err == nil {
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	}
	// A canceled activity surfaces as a bare CanceledError, not wrapped in an ActivityError.
	if _, ok := errors.AsType[*temporal.CanceledError](err); ok {
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	if _, ok := actErr.Unwrap().(*temporal.TimeoutError); ok {
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	}
	return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
}

func (a *wfaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.d.ctx, timeout)
	defer cancel()
	resp, err := a.d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.activityTQ},
		Identity:  a.d.env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)
	if resp.GetActivityId() == "" {
		return nil
	}
	return resp
}

// rpc performs the frontend RPC for a non-Poll, non-timer event and returns its error.
func (a *wfaHandle) rpc(e model.Event) error {
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
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	default:
		return fmt.Errorf("wfaDriver: unhandled event type %v", e.Type)
	}
}

// activityInfo is the activity's PendingActivityInfo, projected down to a schema shared with
// standalone activity.
func (a *wfaHandle) activityInfo(t require.TestingT) activityInfo {
	p, pending := a.pendingActivityInfo(t)
	require.Truef(t, pending, "activity %q not pending; workflow may have closed", a.activityID)
	return p
}
