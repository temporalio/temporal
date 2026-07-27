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
	apiactivitypb "go.temporal.io/api/activity/v1"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type wfaDriver struct {
	env *testcore.TestEnv
	ctx context.Context
	cfg activityConfig

	positivePollTimeout time.Duration // bounds a "must dispatch" poll; 0 => activityDriverTimeout
}

// newWFADriver builds a driver. cfg.StartDelay is ignored: a workflow activity has no per-activity
// start delay.
func newWFADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *wfaDriver {
	return &wfaDriver{env: env, ctx: testcontext.For(t), cfg: cfg}
}

// activityDriverCancelRequestedTimeout bounds the wait for a signalled cancel to reach the activity.
const activityDriverCancelRequestedTimeout = 10 * time.Second

// wfaHandle is a handle to a workflow-scheduled activity.
type wfaHandle struct {
	cursor     *activityModelCursor // the model state reached, so driveEvent can check each event
	cfg        activityConfig       // d.cfg with the windows this trace needs; see activityConfig.forTrace
	d          *wfaDriver
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	taskQueue  string
	token      []byte
}

// driveTrace starts a workflow, which schedules an activity, and then advances that activity
// through a sequence of events (a 'trace'). Returns a handle to the activity at the reached state.
func (d *wfaDriver) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	cfg := d.cfg.forTrace(trace)
	a := d.start(t, cfg)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *wfaHandle) driveEvent(t require.TestingT, e model.Event) {
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
// not within (window + margin). See saaHandle.awaitTimeout.
func (a *wfaHandle) awaitTimeout(t require.TestingT, e model.Event, deadline time.Time) {
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

// timeoutMark is the most recent timeout the activity reports. DescribeWorkflowExecution exposes the
// last failure only while the activity is in progress; once it closes, the timeout comes from the
// workflow result instead.
func (a *wfaHandle) timeoutMark(t require.TestingT) activityTimeoutMark {
	if pa := a.pendingActivityInfo(t); pa != nil {
		return activityTimeoutMark{attemptFailure: timeoutTypeOf(pa.GetLastFailure()), attempt: pa.GetAttempt()}
	}
	m := activityTimeoutMark{closed: true}
	if outcome, ok := errors.AsType[*temporal.TimeoutError](a.run.Get(a.d.ctx, nil)); ok {
		m.outcome = outcome.TimeoutType()
		if cause, ok := errors.AsType[*temporal.TimeoutError](outcome.Unwrap()); ok {
			m.cause = cause.TimeoutType()
		}
	}
	return m
}

// awaitDispatchDelay waits for the public dispatch deadline to become due. A following Poll is what
// proves that the task actually reached Matching.
func (a *wfaHandle) awaitDispatchDelay(t require.TestingT, e model.Event) {
	awaitActivityDispatchDelay(t, e, func() (bool, enumspb.PendingActivityState, *timestamppb.Timestamp, any) {
		pa := a.pendingActivityInfo(t)
		if pa == nil {
			return false, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED, nil, "activity is no longer in progress"
		}
		return true,
			pa.GetState(),
			pa.GetNextAttemptScheduleTime(),
			wfaActivityInfo(pa)
	})
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
	a := &wfaHandle{d: d, cfg: cfg, cursor: newActivityModelCursor(cfg), run: run, workflowID: wfID, runID: run.GetRunID(), activityID: actID, taskQueue: actTQ}
	// The workflow schedules the activity, so it does not exist yet when ExecuteWorkflow returns.
	require.Truef(t, activityDriverPollUntil(time.Now().Add(activityDriverTimeout),
		func() bool {
			_, activityInProgress := a.activityInfoIfInProgress(t)
			return activityInProgress
		}),
		"the workflow did not schedule its activity within %s", activityDriverTimeout)
	return a
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
func wfaSingleActivityWorkflow(ctx workflow.Context, params wfaActivityParams) error {
	c := params.Cfg
	actCtx, cancelActivity := workflow.WithCancel(ctx)
	actCtx = workflow.WithActivityOptions(actCtx, workflow.ActivityOptions{
		TaskQueue:              params.ActivityTQ,
		ActivityID:             params.ActivityID,
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

// pendingActivityInfo is the activity's entry in the workflow's pending set, nil once it is no longer
// pending.
func (a *wfaHandle) pendingActivityInfo(t require.TestingT) *workflowpb.PendingActivityInfo {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return pa
		}
	}
	return nil
}

// activityInfo is the activity's PendingActivityInfo, projected down to a schema shared with
// standalone activity.
func (a *wfaHandle) activityInfo(t require.TestingT) activityInfo {
	info, activityInProgress := a.activityInfoIfInProgress(t)
	require.Truef(t, activityInProgress, "activity %q is no longer in progress; workflow may have closed", a.activityID)
	return info
}

// terminal waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal outcome is not in PendingActivities, so it is read from the workflow-result error's cause.
func (a *wfaHandle) terminal(t require.TestingT) activityTerminalProjection {
	err := a.run.Get(a.d.ctx, nil)
	if err == nil {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
	}
	// A canceled activity surfaces as a bare CanceledError, not wrapped in an ActivityError.
	if _, ok := errors.AsType[*temporal.CanceledError](err); ok {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	switch cause := actErr.Unwrap().(type) {
	case *temporal.ApplicationError:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: cause.Type()}
	case *temporal.TimeoutError:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: cause.TimeoutType().String()}
	default:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED}
	}
}

// terminalStatus waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal status is not in PendingActivities, so it is read from the workflow-result error's cause.
func (a *wfaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	return a.terminal(t).Status
}

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none. The
// SDK surfaces it via TimeoutError.Unwrap().
func (a *wfaHandle) terminalCause(_ require.TestingT) failureCause {
	if toErr, ok := errors.AsType[*temporal.TimeoutError](a.run.Get(a.d.ctx, nil)); ok {
		if appErr, ok := errors.AsType[*temporal.ApplicationError](toErr.Unwrap()); ok {
			return failureCause{Type: appErr.Type(), Message: appErr.Message()}
		}
	}
	return failureCause{}
}

// waitForCancelRequested blocks until the activity reports CANCEL_REQUESTED.
func (a *wfaHandle) waitForCancelRequested() error {
	var describeErr error
	cancelRequested := func() bool {
		resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
		if err != nil {
			describeErr = err
			return true
		}
		for _, pa := range resp.GetPendingActivities() {
			if pa.GetActivityId() == a.activityID && pa.GetState() == enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED {
				return true
			}
		}
		return false
	}
	if activityDriverPollUntil(time.Now().Add(activityDriverCancelRequestedTimeout), cancelRequested) {
		return describeErr
	}
	return fmt.Errorf("wfaDriver: activity %q did not reach CANCEL_REQUESTED after signal", a.activityID)
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes. Readable only
// while the activity is still pending.
func (a *wfaHandle) heartbeatDetails(t require.TestingT) []byte {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return firstPayloadData(pa.GetHeartbeatDetails())
		}
	}
	require.FailNowf(t, "no pending activity", "activity %q not pending", a.activityID)
	return nil
}

// activityInfoIfInProgress returns the shared activity projection and whether the activity still has
// a nonterminal execution.
func (a *wfaHandle) activityInfoIfInProgress(t require.TestingT) (activityInfo, bool) {
	pendingActivity := a.pendingActivityInfo(t)
	if pendingActivity == nil {
		return activityInfo{}, false
	}
	return wfaActivityInfo(pendingActivity), true
}

// wfaActivityInfo converts PendingActivityInfo to the projection shared by both the WFA and SAA
// drivers.
func wfaActivityInfo(p *workflowpb.PendingActivityInfo) activityInfo {
	return activityInfo{
		RunState:                   p.GetState(),
		Attempt:                    p.GetAttempt(),
		CurrentRetryInterval:       p.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: p.GetNextAttemptScheduleTime() != nil,
	}
}

// rpc performs the frontend RPC for a non-Poll, non-timer event and returns its error.
func (a *wfaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.HeartbeatType:
		_, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityHeartbeatDetails,
		})
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
		// WFA cancel comes from the workflow, so signal it, then wait for CANCEL_REQUESTED, which SAA's
		// RequestCancelActivityExecution reaches synchronously.
		if err := a.d.env.SdkClient().SignalWorkflow(a.d.ctx, a.workflowID, a.runID, wfaCancelSignal, nil); err != nil {
			return err
		}
		return a.waitForCancelRequested()
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptionsType:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("wfaDriver: unhandled event type %v", e.Type)
	}
}

func (a *wfaHandle) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.d.env.Namespace().String(), WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
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

func (a *wfaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.d.ctx, timeout)
	defer cancel()
	resp, err := a.d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue},
		Identity:  a.d.env.Tv().WorkerIdentity(),
	})
	require.NoError(t, err)
	if resp.GetActivityId() == "" {
		return nil
	}
	return resp
}
