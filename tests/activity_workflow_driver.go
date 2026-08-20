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
	activitypb "go.temporal.io/api/activity/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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
	activityDriverState
	d          *wfaDriver
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	taskQueue  string
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

func (a *wfaHandle) driveEvent(t testing.TB, e model.Event) {
	driveActivityEvent(t, a, e)
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
	var timeoutErr *temporal.TimeoutError
	if errors.As(a.run.Get(a.d.ctx, nil), &timeoutErr) {
		return activityTimeoutInfo{timeout: timeoutErr.TimeoutType(), terminal: true}
	}
	return activityTimeoutInfo{terminal: true}
}

// awaitDispatchDelay waits for the public dispatch deadline to become due. A following Poll is what
// proves that the task actually reached Matching.
func (a *wfaHandle) awaitDispatchDelay(t testing.TB, e model.Event) {
	awaitActivityDispatchDelay(a.d.ctx, t, e, func(t require.TestingT) (bool, enumspb.PendingActivityState, *timestamppb.Timestamp, any) {
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
	a := &wfaHandle{
		activityDriverState: activityDriverState{ctx: d.ctx, cfg: cfg},
		d:                   d,
		run:                 run,
		workflowID:          wfID,
		runID:               run.GetRunID(),
		activityID:          actID,
		taskQueue:           actTQ,
	}
	// The workflow schedules the activity, so it does not exist yet when ExecuteWorkflow returns.
	await.Require(d.ctx, t, func(t *await.T) {
		_, activityInProgress := a.activityInfoIfInProgress(t)
		t.Require().True(activityInProgress, "the workflow has not scheduled its activity")
	}, activityDriverTimeout, activityDriverPollInterval)
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

// terminalOutcome waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal status is not in PendingActivities, so it is read from the workflow-result error's cause.
func (a *wfaHandle) terminalOutcome(t require.TestingT) activityTerminalOutcome {
	err := a.run.Get(a.d.ctx, nil)
	if err == nil {
		return activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
	}
	// A canceled activity is returned as a bare CanceledError, not wrapped in an ActivityError.
	if _, ok := errors.AsType[*temporal.CanceledError](err); ok {
		return activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	outcome := activityTerminalOutcome{
		status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
		retryState: actErr.RetryState(),
	}
	if _, ok := actErr.Unwrap().(*temporal.TimeoutError); ok {
		outcome.status = enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	}
	return outcome
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
		LastHeartbeatDetails:       activityMarshalPayloads(p.GetHeartbeatDetails()),
	}
}

// waitForCancelRequested waits until the workflow-initiated cancellation reaches the activity.
func (a *wfaHandle) waitForCancelRequested(t testing.TB) {
	await.Require(a.d.ctx, t, func(t *await.T) {
		pendingActivity := a.pendingActivityInfo(t)
		t.Require().NotNil(pendingActivity, "activity is no longer in progress")
		t.Require().Equal(enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, pendingActivity.GetState())
	}, activityDriverTimeout, activityDriverPollInterval)
}

func (a *wfaHandle) respondCanceledByID() error {
	_, err := a.d.env.FrontendClient().RespondActivityTaskCanceledById(
		a.d.ctx,
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
		_, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityRecordedHeartbeatDetails,
		})
		return err
	case model.RespondCompletedType:
		_, err := fc.RespondActivityTaskCompleted(a.d.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
			Result: payloads.EncodeString("result"),
		})
		return err
	case model.RespondCompletedByIDType:
		_, err := fc.RespondActivityTaskCompletedById(a.d.ctx, &workflowservice.RespondActivityTaskCompletedByIdRequest{
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
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, req)
		return err
	case model.RespondFailedByIDType:
		req := &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace: ns, WorkflowId: a.workflowID, RunId: a.runID, ActivityId: a.activityID, Identity: a.d.env.Tv().WorkerIdentity(),
			Failure: respondFailedFailure(e, a.cfg.NextRetryDelay),
		}
		if e.HasHeartbeatDetails {
			req.LastHeartbeatDetails = activityHeartbeatDetails
		}
		_, err := fc.RespondActivityTaskFailedById(a.d.ctx, req)
		return err
	case model.RespondCanceledType:
		_, err := fc.RespondActivityTaskCanceled(a.d.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RequestCancelType:
		if err := a.d.env.SdkClient().SignalWorkflow(
			a.d.ctx,
			a.workflowID,
			a.runID,
			wfaCancelSignal,
			nil,
		); err != nil {
			return err
		}
		a.waitForCancelRequested(t)
		return nil
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), KeepPaused: e.KeepPaused, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.UpdateOptionsType:
		_, err := fc.UpdateActivityExecutionOptions(a.d.ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace: ns, WorkflowId: a.workflowID, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
		})
		return err
	default:
		return fmt.Errorf("wfaDriver: unhandled event type %v", e.Type)
	}
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
