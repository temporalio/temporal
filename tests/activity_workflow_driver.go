package tests

// Driver for workflow-activity (WFA) tests: it drives an activity scheduled by a workflow through a
// sequence of events (a 'trace'), and observes it via DescribeWorkflowExecution.

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

// --- shared observable projections ---------------------------------------------------------

// activityInfoProjection is the retry-scheduling contract both surfaces expose. See the two
// projection() methods.
type activityInfoProjection struct {
	State                  enumspb.PendingActivityState
	Attempt                int32
	CurrentRetryInterval   time.Duration
	NextAttemptScheduleSet bool
}

// activityTerminalProjection is the terminal status plus the failure discriminant a user sees: the
// application failure Type for FAILED, the TimeoutType string for TIMED_OUT, empty otherwise.
type activityTerminalProjection struct {
	Status      enumspb.ActivityExecutionStatus
	FailureType string
}

func projectWFA(p *workflowpb.PendingActivityInfo) activityInfoProjection {
	return activityInfoProjection{
		State:                  p.GetState(),
		Attempt:                p.GetAttempt(),
		CurrentRetryInterval:   p.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleSet: p.GetNextAttemptScheduleTime() != nil,
	}
}

// --- driver --------------------------------------------------------------------------------

type wfaDriver struct {
	env *testcore.TestEnv
	ctx context.Context
	cfg activityConfig

	positivePollTimeout time.Duration // bounds a "must dispatch" poll; 0 => activityDriverPositivePollTimeout
}

// newWFADriver builds a driver with the test-scoped context. cfg.StartDelay is ignored: a
// workflow activity has no per-activity start delay.
func newWFADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *wfaDriver {
	return &wfaDriver{env: env, ctx: testcontext.For(t), cfg: cfg}
}

// wfaHandle is a handle to one workflow-scheduled activity: the ids that address it and the workflow
// that owns it, plus the token last dispatched to it.
type wfaHandle struct {
	d          *wfaDriver
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	activityTQ string
	token      []byte
}

type wfaActivityParams struct {
	ActivityTQ             string
	ActivityID             string
	StartToClose           time.Duration
	ScheduleToClose        time.Duration // 0 = unset
	ScheduleToStart        time.Duration // 0 = unset
	Heartbeat              time.Duration // 0 = unset
	RetryInterval          time.Duration
	BackoffCoefficient     float64       // 0 = 1.0 (constant interval)
	MaxInterval            time.Duration // 0 = RetryInterval (no growth)
	MaxAttempts            int32
	NonRetryableErrorTypes []string
}

// wfaCancelSignal makes the helper workflow cancel the activity, which is how a workflow activity is
// cancelled rather than by a direct RPC.
const wfaCancelSignal = "cancel"

// wfaOneActivityWorkflow schedules a single activity with the given options on its own task queue and
// waits for it to finish. No worker executes the activity — the test drives it with raw worker RPCs.
// WaitForCancellation makes the workflow wait for RespondActivityTaskCanceled, so a cancelled activity
// reaches CANCELED before the workflow closes.
func wfaOneActivityWorkflow(ctx workflow.Context, p wfaActivityParams) error {
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
		DisableEagerExecution:  true, // force the task through matching so the test can poll it
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
	fut := workflow.ExecuteActivity(actCtx, "wfaNoop")
	workflow.Go(ctx, func(gctx workflow.Context) {
		workflow.GetSignalChannel(gctx, wfaCancelSignal).Receive(gctx, nil)
		cancelActivity()
	})
	return fut.Get(ctx, nil)
}

// driveTrace runs a trace on a fresh workflow-scheduled activity and returns a handle at the reached
// state. Model-free.
func (d *wfaDriver) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	a := d.start(t)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *wfaHandle) driveEvent(t require.TestingT, e model.Event) {
	d := a.d
	switch {
	case e.Type == model.PollType:
		// A poll captures the dispatched task token.
		if resp := a.pollForTask(t, cmp.Or(d.positivePollTimeout, activityDriverPositivePollTimeout)); resp != nil {
			a.token = resp.GetTaskToken()
		}
	case isWallClockEvent(e.Type):
		// A wall-clock event is realized by waiting out its configured window.
		a.awaitWallClock(t, e)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// awaitWallClock blocks until a wall-clock event's effect shows up in the workflow's view of the
// activity, and fails if it does not within (window + settle). The effect is a change in the
// pending-activity projection, or the activity leaving the pending set. WFA has no long-poll Describe,
// so this polls.
func (a *wfaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	before, beforePending := a.pendingSnapshot(t)
	deadline := time.Now().Add(a.d.cfg.window(e) + activityDriverWallClockSettle)
	changed := func() bool {
		now, nowPending := a.pendingSnapshot(t)
		return nowPending != beforePending || (nowPending && now != before)
	}
	if activityDriverPollUntil(deadline, changed) {
		return
	}
	t.Errorf("%s: the activity did not change within %s of driving the event, so the event did not "+
		"take effect. Last observed: %+v", e, a.d.cfg.window(e)+activityDriverWallClockSettle, before)
}

// pendingSnapshot is the activity's pending-activity projection, and whether it is currently pending. A
// Describe error is reported rather than treated as absence.
func (a *wfaHandle) pendingSnapshot(t require.TestingT) (activityInfoProjection, bool) {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return projectWFA(pa), true
		}
	}
	return activityInfoProjection{}, false
}

func (d *wfaDriver) start(t *testing.T) *wfaHandle {
	wfTQ := testcore.RandomizeStr("wfa-wf")
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	// A dedicated workflow worker runs the helper workflow. Nothing polls the activity task queue, so the
	// test is the only consumer of the activity's tasks.
	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(wfaOneActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	c := d.cfg
	wfID := testcore.RandomizeStr("wfa-run")
	run, err := d.env.SdkClient().ExecuteWorkflow(d.ctx,
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: wfTQ},
		wfaOneActivityWorkflow, wfaActivityParams{
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

// terminal waits for the activity to reach a terminal state and reports it. A workflow activity's
// terminal outcome is not in PendingActivities, so it is read from the workflow-result error's cause.
func (a *wfaHandle) terminal(t require.TestingT) activityTerminalProjection {
	err := a.run.Get(a.d.ctx, nil)
	if err == nil {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
	}
	// A canceled activity surfaces as a bare CanceledError, not wrapped in an ActivityError.
	var canceledErr *temporal.CanceledError
	if errors.As(err, &canceledErr) {
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

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error. The operator
// commands are the same *Execution APIs with WorkflowId set; cancel is the exception, see below.
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

// projection is the activity's pending-activity info as an activityInfoProjection.
func (a *wfaHandle) projection(t require.TestingT) activityInfoProjection {
	resp, err := a.d.env.SdkClient().DescribeWorkflowExecution(a.d.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return projectWFA(pa)
		}
	}
	require.FailNowf(t, "no pending activity", "activity %q not pending; workflow may have closed", a.activityID)
	return activityInfoProjection{}
}
