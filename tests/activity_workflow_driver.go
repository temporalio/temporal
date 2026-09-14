package tests

// Driver for workflow-activity (WFA) tests: it drives an activity scheduled by a workflow through a
// sequence of events (a 'trace'). Each event is either a frontend RPC, a poll, or a timer
// wait. The event vocabulary is in chasm/lib/activity/model.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type wfaDriver struct {
	env    *testcore.TestEnv
	t      *testing.T
	cfg    activityConfig
	cfgIdx int    // labels this driver's config in the conformance explorer's logs
	wfTQ   string // task queue of the wrapper-workflow worker; set by workflowTaskQueue

	workerStarted sync.Once

	positivePollTimeout time.Duration // bounds a "must dispatch" poll; 0 => activityDriverTimeout

	// holdOpen keeps each wrapper workflow running after its activity closes; see
	// wfaSingleActivityWorkflow.
	holdOpen bool
}

// newWFADriver builds a driver. cfg.StartDelay is ignored: a workflow activity has no per-activity
// start delay.
func newWFADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *wfaDriver {
	return &wfaDriver{env: env, t: t, cfg: cfg}
}

// workflowTaskQueue is the task queue of the one workflow worker serving every activity this driver
// starts, started on first use so that constructing a driver touches nothing. The worker hosts only
// the wrapper workflow, never the activity: the tests poll for activity tasks themselves.
func (d *wfaDriver) workflowTaskQueue() string {
	d.workerStarted.Do(func() {
		d.wfTQ = testcore.RandomizeStr("wfa-wf")
		w := sdkworker.New(d.env.SdkClient(), d.wfTQ, sdkworker.Options{})
		w.RegisterWorkflow(wfaSingleActivityWorkflow)
		require.NoError(d.t, w.Start())
		d.t.Cleanup(w.Stop)
	})
	return d.wfTQ
}

// testContext returns the driver's current test context, deliberately not cached;
// see [testcontext.EnsureRemaining].
func (d *wfaDriver) testContext() context.Context {
	return testcontext.For(d.t)
}

func (a *wfaHandle) testContext() context.Context {
	return a.d.testContext()
}

// wfaHandle is a handle to a workflow-scheduled activity.
type wfaHandle struct {
	activityDriverState
	model         *activityModel // the model state reached, so driveEvent can check each event
	d             *wfaDriver
	run           sdkclient.WorkflowRun
	workflowID    string
	runID         string
	activityID    string
	taskQueue     string
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
}

// driveTrace starts a workflow, which schedules an activity, and then advances that activity
// through a sequence of events (a 'trace'). Returns a handle to the activity at the reached state.
func (d *wfaDriver) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	require.Falsef(t, d.cfg.StartDelay > 0 || slices.ContainsFunc(trace, func(e model.Event) bool {
		return e.Type == model.StartDelayElapsesType
	}), "workflow activity does not support start delay")
	a := d.start(t, d.cfg.forTrace(trace))
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

func (a *wfaHandle) driveEvent(t testing.TB, e model.Event) {
	driveActivityEvent(t, a, e, a.model)
}

func (a *wfaHandle) awaitTimeout(t testing.TB, e model.Event, deadline time.Time) {
	awaitActivityTimeout(t, a, e, deadline)
}

// timeoutInfo is the most recent timeout the activity reports. DescribeWorkflowExecution exposes the
// last failure only while the activity is in progress; once it closes, the timeout comes from the
// workflow result instead.
func (a *wfaHandle) timeoutInfo(t require.TestingT) activityTimeoutInfo {
	if pa := a.pendingActivityInfo(t); pa != nil {
		return activityTimeoutInfo{
			timeout: pa.GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType(),
			attempt: pa.GetAttempt(),
		}
	}
	if timeoutErr, ok := errors.AsType[*temporal.TimeoutError](a.run.Get(a.testContext(), nil)); ok {
		return activityTimeoutInfo{timeout: timeoutErr.TimeoutType(), terminal: true}
	}
	return activityTimeoutInfo{terminal: true}
}

// awaitDispatchDelay waits for the public dispatch deadline to become due. A following Poll is what
// proves that the task actually reached Matching.
func (a *wfaHandle) awaitDispatchDelay(t testing.TB, e model.Event) {
	awaitActivityDispatchDelay(a.testContext(), t, e, func(t require.TestingT) (bool, enumspb.PendingActivityState, *timestamppb.Timestamp, any) {
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

func (d *wfaDriver) start(t testing.TB, cfg activityConfig) *wfaHandle {
	cfg.StartDelay = 0 // WFA does not support start delay, but SAA/WFA tests often share config
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	wfID := testcore.RandomizeStr("wfa-run")
	run, err := d.env.SdkClient().ExecuteWorkflow(d.testContext(),
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: d.workflowTaskQueue()},
		wfaSingleActivityWorkflow,
		wfaActivityParams{Cfg: cfg, ActivityTQ: actTQ, ActivityID: actID, HoldOpen: d.holdOpen})

	require.NoError(t, err)
	a := &wfaHandle{
		activityDriverState: activityDriverState{
			cfg:                 cfg,
			positivePollTimeout: d.positivePollTimeout,
			establishedReqID:    map[model.EventType]string{},
		},
		model:      newActivityModel(cfg),
		d:          d,
		run:        run,
		workflowID: wfID,
		runID:      run.GetRunID(),
		activityID: actID,
		taskQueue:  actTQ,
	}
	a.awaitScheduled(t)
	return a
}

// awaitScheduled blocks until the workflow has scheduled its activity, which it has not yet done when
// ExecuteWorkflow returns.
//
// This is a plain loop rather than await.Require because it is setup, not an assertion: await.Require
// declines to poll once the test has recorded a failure, which would leave the conformance explorer
// driving an activity that does not exist yet every time it restarts after reporting a divergence.
func (a *wfaHandle) awaitScheduled(t require.TestingT) {
	deadline := time.Now().Add(activityDriverTimeout)
	for time.Now().Before(deadline) {
		if _, inProgress := a.activityInfoIfInProgress(t); inProgress {
			return
		}
		time.Sleep(activityDriverPollInterval) //nolint:forbidigo // see the comment above
	}
	t.Errorf("wfaDriver: the workflow did not schedule its activity within %s", activityDriverTimeout)
}

// wfaActivityParams is what the helper workflow needs to schedule the activity: the activity the
// test described, and where to put it.
type wfaActivityParams struct {
	Cfg        activityConfig
	ActivityTQ string
	ActivityID string
	HoldOpen   bool
}

// wfaCancelSignal makes the helper workflow cancel the activity, which is how a workflow activity is
// cancelled rather than by a direct RPC.
const wfaCancelSignal = "cancel"

// wfaCloseSignal lets a held-open workflow finish; see wfaActivityParams.HoldOpen.
const wfaCloseSignal = "close"

// wfaSingleActivityWorkflow is a workflow that schedules a single activity with the given options
// on its own task queue and waits for it to finish. No worker executes the activity — the test
// drives it with worker poll RPCs. WaitForCancellation makes the workflow wait for
// RespondActivityTaskCanceled, so a cancelled activity reaches CANCELED before the workflow closes.
//
// HoldOpen keeps the workflow running once the activity has closed. Without it the two close together,
// so an RPC naming a terminal activity is answered about a workflow that no longer exists — which says
// nothing about how a closed activity behaves. The conformance explorer sets it; the parity tests do
// not, because they read the activity's terminal outcome from the workflow result.
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
	err := fut.Get(ctx, nil)
	if params.HoldOpen {
		workflow.GetSignalChannel(ctx, wfaCloseSignal).Receive(ctx, nil)
	}
	return err
}

// pendingActivityInfo is the activity's entry in the workflow's pending set, nil once it is no longer
// pending.
func (a *wfaHandle) pendingActivityInfo(t require.TestingT) *workflowpb.PendingActivityInfo {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.testContext(), a.workflowID, a.runID)
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
	err := a.run.Get(a.testContext(), nil)
	if err == nil {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
	}
	// A canceled activity is returned as a bare CanceledError, not wrapped in an ActivityError.
	if _, ok := errors.AsType[*temporal.CanceledError](err); ok {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	switch cause := actErr.Unwrap().(type) {
	case *temporal.ApplicationError:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: cause.Type(), RetryState: actErr.RetryState()}
	case *temporal.TimeoutError:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: cause.TimeoutType().String(), RetryState: actErr.RetryState()}
	default:
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, RetryState: actErr.RetryState()}
	}
}

// terminalStatus waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal status is not in PendingActivities, so it is read from the workflow-result error's cause.
func (a *wfaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	return a.terminal(t).Status
}

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none. The
// SDK exposes it via TimeoutError.Unwrap().
func (a *wfaHandle) terminalCause(_ require.TestingT) failureCause {
	if toErr, ok := errors.AsType[*temporal.TimeoutError](a.run.Get(a.testContext(), nil)); ok {
		if appErr, ok := errors.AsType[*temporal.ApplicationError](toErr.Unwrap()); ok {
			return failureCause{Type: appErr.Type(), Message: appErr.Message()}
		}
	}
	return failureCause{}
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes. Readable only
// while the activity is still pending.
func (a *wfaHandle) heartbeatDetails(t require.TestingT) []byte {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.testContext(), a.workflowID, a.runID)
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

// observedState is the activity's state as its entry in the workflow's pending set reports it. An
// activity that has left that set has closed.
func (a *wfaHandle) observedState(t require.TestingT) activityState {
	pendingActivity := a.pendingActivityInfo(t)
	if pendingActivity == nil {
		return activityState{closed: true}
	}
	return activityState{runState: pendingActivity.GetState(), attempt: pendingActivity.GetAttempt()}
}

// wfaActivityInfo converts PendingActivityInfo to the projection shared by both the WFA and SAA
// drivers.
func wfaActivityInfo(p *workflowpb.PendingActivityInfo) activityInfo {
	return activityInfo{
		RunState:                   p.GetState(),
		Attempt:                    p.GetAttempt(),
		CurrentRetryInterval:       p.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: p.GetNextAttemptScheduleTime() != nil,
		LastHeartbeatDetails:       activityMarshalPayloads(p.GetHeartbeatDetails()),
	}
}

// waitForCancelRequested waits until the workflow-initiated cancellation reaches the activity.
func (a *wfaHandle) waitForCancelRequested(t testing.TB) {
	await.Require(a.testContext(), t, func(t *await.T) {
		pendingActivity := a.pendingActivityInfo(t)
		t.Require().NotNil(pendingActivity, "activity is no longer in progress")
		t.Require().Equal(enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, pendingActivity.GetState())
	}, activityDriverTimeout, activityDriverPollInterval)
}

func (a *wfaHandle) respondCanceledByID() error {
	_, err := a.d.env.FrontendClient().RespondActivityTaskCanceledById(
		a.testContext(),
		&workflowservice.RespondActivityTaskCanceledByIdRequest{
			Namespace:  a.d.env.Namespace().String(),
			WorkflowId: a.workflowID,
			ActivityId: a.activityID,
			RunId:      a.runID,
			Identity:   a.d.env.Tv().WorkerIdentity(),
		},
	)
	return err
}

// rpc performs the frontend RPC for a non-Poll, non-timer event and returns its error.
func (a *wfaHandle) rpc(t testing.TB, e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.HeartbeatType:
		resp, err := fc.RecordActivityTaskHeartbeat(a.testContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityRecordedHeartbeatDetails,
		})
		a.lastHeartbeat = resp
		return err
	case model.RespondCompletedType:
		_, err := fc.RespondActivityTaskCompleted(a.testContext(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
			Result: payloads.EncodeString("result"),
		})
		return err
	case model.RespondCompletedByIDType:
		_, err := fc.RespondActivityTaskCompletedById(a.testContext(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace: ns, WorkflowId: a.workflowID, RunId: a.runID, ActivityId: a.activityID, Identity: a.d.env.Tv().WorkerIdentity(),
			Result: payloads.EncodeString("result"),
		})
		return err
	case model.RespondFailedType:
		req := &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(), Failure: respondFailedFailure(e, a.cfg.NextRetryDelay),
		}
		if e.HasHeartbeatDetails {
			req.LastHeartbeatDetails = activityHeartbeatDetails
		}
		_, err := fc.RespondActivityTaskFailed(a.testContext(), req)
		return err
	case model.RespondFailedByIDType:
		req := &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace: ns, WorkflowId: a.workflowID, RunId: a.runID, ActivityId: a.activityID, Identity: a.d.env.Tv().WorkerIdentity(),
			Failure: respondFailedFailure(e, a.cfg.NextRetryDelay),
		}
		if e.HasHeartbeatDetails {
			req.LastHeartbeatDetails = activityHeartbeatDetails
		}
		_, err := fc.RespondActivityTaskFailedById(a.testContext(), req)
		return err
	case model.RespondCanceledType:
		_, err := fc.RespondActivityTaskCanceled(a.testContext(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RequestCancelType:
		// WFA cancel comes from the workflow, so signal it, then wait for CANCEL_REQUESTED, which SAA's
		// RequestCancelActivityExecution reaches synchronously.
		if err := a.d.env.SdkClient().SignalWorkflow(a.testContext(), a.workflowID, a.runID, wfaCancelSignal, nil); err != nil {
			return err
		}
		a.waitForCancelRequested(t)
		return nil
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.testContext(), &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.testContext(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.testContext(), &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			KeepPaused: e.KeepPaused, ResetHeartbeat: e.ResetHeartbeat, RestoreOriginalOptions: e.RestoreOriginal,
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
		req.ActivityOptions = &activitypb.ActivityOptions{StartDelay: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}}
	default:
		// A minimal, always-valid update: re-set the heartbeat timeout.
		req.ActivityOptions = &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	_, err := a.d.env.FrontendClient().UpdateActivityExecutionOptions(a.testContext(), req)
	return err
}

func (a *wfaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	return activityPollForTask(a.testContext(), t, "wfaDriver", a.d.env, a.taskQueue, timeout)
}
