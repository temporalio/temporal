package tests

// Driver for workflow-activity (WFA) tests: it drives an activity scheduled by a workflow through a
// sequence of events (a 'trace'). Each event is either a frontend RPC, a poll, or a wall-clock
// wait. The event vocabulary is in chasm/lib/activity/model.

import (
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

func wfaActivityInfo(p *workflowpb.PendingActivityInfo) activityInfo {
	return activityInfo{
		RunState:                   p.GetState(),
		Attempt:                    p.GetAttempt(),
		CurrentRetryInterval:       p.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: p.GetNextAttemptScheduleTime() != nil,
	}
}

// --- driver --------------------------------------------------------------------------------

type wfaDriver struct {
	env *testcore.TestEnv
	ctx context.Context
	cfg activityConfig
}

// newWFADriver builds a driver with the test-scoped context. cfg.StartDelay is ignored: a
// workflow activity has no per-activity start delay.
func newWFADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *wfaDriver {
	return &wfaDriver{env: env, ctx: testcontext.For(t), cfg: cfg}
}

// wfaHandle is a handle to one workflow-scheduled activity.
type wfaHandle struct {
	d          *wfaDriver
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	activityTQ string
	token      []byte
}

// The server rejects an activity with neither start-to-close nor schedule-to-close set. The drivers
// always send start-to-close, defaulted long enough not to fire. The other timeouts are simply
// absent when unset.
type wfaActivityParams struct {
	ActivityTQ             string
	ActivityID             string
	StartToClose           time.Duration
	ScheduleToClose        time.Duration // 0 = unset
	ScheduleToStart        time.Duration // 0 = unset
	Heartbeat              time.Duration // 0 = unset
	RetryInterval          time.Duration // initial retry interval
	BackoffCoefficient     float64       // 0 means 1.0 (no increase over attempts)
	MaxInterval            time.Duration // 0 means cap at initial RetryInterval (no increase over attempts)
	MaxAttempts            int32
	NonRetryableErrorTypes []string
}

// wfaCancelSignal makes the helper workflow cancel the activity, which is how a workflow activity is
// cancelled rather than by a direct RPC.
const wfaCancelSignal = "cancel"

// wfaSingleActivityWorkflow is a workflow that schedules a single activity with the given options
// on its own task queue and waits for it to finish. No worker executes the activity — the test
// drives it with worker poll RPCs. WaitForCancellation makes the workflow wait for
// RespondActivityTaskCanceled, so a cancelled activity reaches CANCELED before the workflow closes.
func wfaSingleActivityWorkflow(ctx workflow.Context, p wfaActivityParams) error {
	coefficient := p.BackoffCoefficient
	if coefficient == 0 {
		coefficient = 1.0
	}
	maxInterval := p.MaxInterval
	if maxInterval == 0 {
		maxInterval = p.RetryInterval
	}
	actCtx, cancelActivity := workflow.WithCancel(ctx)
	actCtx = workflow.WithActivityOptions(actCtx, workflow.ActivityOptions{
		TaskQueue:              p.ActivityTQ,
		ActivityID:             p.ActivityID,
		StartToCloseTimeout:    p.StartToClose,
		ScheduleToCloseTimeout: p.ScheduleToClose,
		ScheduleToStartTimeout: p.ScheduleToStart,
		HeartbeatTimeout:       p.Heartbeat,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        p.RetryInterval,
			BackoffCoefficient:     coefficient,
			MaximumInterval:        maxInterval,
			MaximumAttempts:        p.MaxAttempts,
			NonRetryableErrorTypes: p.NonRetryableErrorTypes,
		},
	})
	fut := workflow.ExecuteActivity(actCtx, "testWFA")
	workflow.Go(ctx, func(gctx workflow.Context) {
		workflow.GetSignalChannel(gctx, wfaCancelSignal).Receive(gctx, nil)
		cancelActivity()
	})
	return fut.Get(ctx, nil)
}

// driveTrace starts a workflow, which schedules an activity, and then advances that activity
// through a sequence of events. Returns a handle to the activity at the reached state.
func (d *wfaDriver) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	validateTrace(t, trace)
	d.cfg = d.cfg.forTrace(trace)
	a := d.start(t)
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

// awaitWallClock blocks until a wall-clock event's effect shows up in the workflow's view of the
// activity, and fails if it does not within (window + settle).
func (a *wfaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	if isDispatchDelayEvent(e.Type) {
		a.awaitDispatchTimePassed(t, e)
		return
	}
	deadline := time.Now().Add(a.d.cfg.window(e) + activityDriverWallClockSettle)
	before, beforePending := a.pendingActivityInfo(t)
	changed := func() bool {
		now, nowPending := a.pendingActivityInfo(t)
		return nowPending != beforePending || (nowPending && now != before)
	}
	if activityDriverPollUntil(deadline, changed) {
		return
	}
	t.Errorf("%s: the activity did not change within %s of driving the event, so the event did not "+
		"take effect. Last observed: %+v", e, a.d.cfg.window(e)+activityDriverWallClockSettle, before)
}

// awaitDispatchTimePassed polls the activity until the dispatch time has passed, and fails if it
// has not.
func (a *wfaHandle) awaitDispatchTimePassed(t require.TestingT, e model.Event) {
	next := a.pendingActivity(t).GetNextAttemptScheduleTime()
	if next == nil {
		return // the dispatch time has already passed, or the activity is no longer pending
	}
	deadline := next.AsTime().Add(activityDriverWallClockSettle)
	var p activityInfo
	dispatched := func() bool {
		var pending bool
		p, pending = a.pendingActivityInfo(t)
		return !pending || !p.NextAttemptScheduleTimeSet
	}
	if activityDriverPollUntil(deadline, dispatched) {
		return
	}
	t.Errorf("%s: a dispatch is still pending %s after the time the server scheduled it for, so the "+
		"window did not elapse. Last observed: %+v", e, activityDriverWallClockSettle, p)
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

func (d *wfaDriver) start(t *testing.T) *wfaHandle {
	wfTQ := testcore.RandomizeStr("wfa-wf")
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	// Run a workflow worker for the wrapper workflow, but not an activity worker: the tests poll
	// for activity tasks.
	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(wfaSingleActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	c := d.cfg
	wfID := testcore.RandomizeStr("wfa-run")
	run, err := d.env.SdkClient().ExecuteWorkflow(d.ctx,
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: wfTQ},
		wfaSingleActivityWorkflow, wfaActivityParams{
			ActivityTQ:             actTQ,
			ActivityID:             actID,
			StartToClose:           c.startToClose(),
			ScheduleToClose:        c.ScheduleToClose,
			ScheduleToStart:        c.ScheduleToStart,
			Heartbeat:              c.Heartbeat,
			RetryInterval:          c.retryInterval(),
			BackoffCoefficient:     c.BackoffCoefficient,
			MaxInterval:            c.MaxRetryInterval,
			MaxAttempts:            c.MaxAttempts,
			NonRetryableErrorTypes: c.NonRetryableErrorTypes,
		})
	require.NoError(t, err)
	return &wfaHandle{d: d, run: run, workflowID: wfID, runID: run.GetRunID(), activityID: actID, activityTQ: actTQ}
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

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *wfaHandle) rpc(e model.Event) error {
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
