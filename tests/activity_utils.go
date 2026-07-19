package tests

// Driver for workflow-activity (WFA) tests, parallel to the standalone-activity driver in
// activity_standalone_utils.go. It drives an activity scheduled by a workflow through a scripted
// sequence of events (the same event DSL), realizing each event as the corresponding worker RPC /
// poll / wall-clock wait, and observes it via DescribeWorkflowExecution. Its purpose is to prove the
// standalone (CHASM) activity behaves like the workflow activity at their intersection: drive the same
// trace through both and compare the public activity info (activityInfoProjection), with WFA as the
// oracle. The worker-facing RPCs (poll / respond) are the same frontend APIs the SAA driver uses; the
// only WFA-specific parts are that the activity is scheduled by a workflow and observed through it.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
	"go.temporal.io/server/tests/testcore"
)

// --- shared observable projection ----------------------------------------------------------
//
// activityInfoProjection is the retry-scheduling contract both surfaces expose — SAA's
// ActivityExecutionInfo (see projectSAA in activity_standalone_utils.go) and WFA's PendingActivityInfo
// (see projectWFA) — and that users depend on. It is the part of the contract SAA GA locks and must
// match WFA. CurrentRetryInterval is rounded to the second: WFA derives it by subtracting two stored
// timestamps (a few µs of noise) while SAA stores it exactly, so an unrounded compare would flag a
// non-divergence. NextAttemptScheduleTime is compared by set-ness, not value (its absolute wall-clock
// value differs across two independent runs).
//
// The last-* timestamps (last started / last completed / last worker) are intentionally left out:
// their cross-surface semantics differ in ways that are separate open questions — e.g. during a
// backoff WFA reports LastStartedTime nil (reset on reschedule) where SAA keeps the prior attempt's —
// not part of this clean first-cut equivalence check.
type activityInfoProjection struct {
	State                  enumspb.PendingActivityState
	Attempt                int32
	CurrentRetryInterval   time.Duration // rounded to the second (see above)
	NextAttemptScheduleSet bool          // NextAttemptScheduleTime != nil
}

// activityTerminalProjection is the terminal-outcome contract both surfaces expose: the terminal
// status plus the failure discriminant users see — the application failure Type for FAILED, the
// TimeoutType string for TIMED_OUT, empty otherwise. SAA reads it from DescribeActivityExecution's
// outcome; WFA maps it from the workflow-result error's cause (see the two terminal() methods).
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

type wfaHarness struct {
	env            *standaloneActivityEnv
	ctx            context.Context
	maxAttempts    int32         // RetryPolicy MaximumAttempts (0 = unlimited)
	retryInterval  time.Duration // RetryPolicy interval; how long the driver waits for BackoffElapses
	nextRetryDelay time.Duration // ApplicationFailureInfo.NextRetryDelay injected into RespondFailed
	// shortTimeout, when set to one of the four timeout *Elapses kinds, makes that timeout short at
	// schedule time so a trace can trigger it (mirrors saaHarness.shortTimeout).
	shortTimeout model.EventKind
	// positivePollTimeout bounds a "must dispatch" poll; 0 => 10s.
	positivePollTimeout time.Duration
}

// wfaHandle is a handle to one workflow-scheduled activity: the token last dispatched to it plus the
// ids needed to address it and the workflow that owns it.
type wfaHandle struct {
	h          *wfaHarness
	run        sdkclient.WorkflowRun
	workflowID string
	runID      string
	activityID string
	activityTQ string
	token      []byte
}

type wfaActivityParams struct {
	ActivityTQ      string
	ActivityID      string
	StartToClose    time.Duration
	ScheduleToClose time.Duration // 0 = unset
	ScheduleToStart time.Duration // 0 = unset
	Heartbeat       time.Duration // 0 = unset
	RetryInterval   time.Duration
	MaxAttempts     int32
}

// wfaCancelSignal, when sent to the helper workflow, makes it cancel the activity — the WFA analog of
// SAA's RequestCancelActivityExecution RPC (a workflow activity is cancelled by its workflow, not by a
// direct RPC). See wfaHandle.rpc's RequestCancel case.
const wfaCancelSignal = "cancel"

// wfaOneActivityWorkflow schedules a single activity with the given options on its own task queue and
// waits for it to finish. The activity is never executed by a worker — the test drives it with raw
// worker RPCs — so the workflow simply stays running while the test polls and responds. A cancel
// signal cancels the activity; WaitForCancellation makes the workflow wait for the worker's
// RespondActivityTaskCanceled so the activity actually reaches CANCELED before the workflow closes.
func wfaOneActivityWorkflow(ctx workflow.Context, p wfaActivityParams) error {
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
			InitialInterval:    p.RetryInterval,
			BackoffCoefficient: 1.0,
			MaximumInterval:    p.RetryInterval,
			MaximumAttempts:    p.MaxAttempts,
		},
	})
	fut := workflow.ExecuteActivity(actCtx, "wfaNoop")
	workflow.Go(ctx, func(gctx workflow.Context) {
		workflow.GetSignalChannel(gctx, wfaCancelSignal).Receive(gctx, nil)
		cancelActivity()
	})
	return fut.Get(ctx, nil)
}

// driveTrace runs a trace on a fresh workflow-scheduled activity, realizing each event against the
// server, and returns a handle to it at the reached state. Model-free, parallel to
// saaHarness.driveTrace.
func (h *wfaHarness) driveTrace(t *testing.T, trace []model.Event) *wfaHandle {
	a := h.start(t)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event: a poll captures the dispatched token, a wall-clock
// event is waited out, any other event is its worker RPC (which must succeed). Parallel to
// saaHandle.driveEvent.
func (a *wfaHandle) driveEvent(t require.TestingT, e model.Event) {
	h := a.h
	switch {
	case e.Kind == model.Poll:
		timeout := 10 * time.Second
		if h.positivePollTimeout > 0 {
			timeout = h.positivePollTimeout
		}
		if resp := a.pollForTask(t, timeout); resp != nil {
			a.token = resp.GetTaskToken()
		}
	case saaIsWallClock(e.Kind):
		time.Sleep(h.eventClock(e) + saaWallClockSettle)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// eventClock is how long the clock behind a wall-clock event takes to elapse: a backoff lasts the
// retry interval, a timeout under test is configured short. (WFA has no per-activity start delay.)
func (h *wfaHarness) eventClock(e model.Event) time.Duration {
	if e.Kind == model.BackoffElapses {
		return h.retryInterval
	}
	return saaShortTimeout // the four timeouts
}

func (h *wfaHarness) start(t *testing.T) *wfaHandle {
	wfTQ := testcore.RandomizeStr("wfa-wf")
	actTQ := testcore.RandomizeStr("wfa-act")
	const actID = "act"

	// A dedicated workflow worker runs the helper workflow; nothing polls the activity task queue, so
	// the test is the only consumer of the activity's tasks.
	w := sdkworker.New(h.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(wfaOneActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	// dur returns the short timeout for the one timeout under test, long otherwise (mirrors
	// saaHarness.startRequest). Only the timeout the trace fires is set short; the rest stay long or
	// unset so they do not fire mid-scenario.
	dur := func(k model.EventKind) time.Duration {
		if h.shortTimeout == k {
			return saaShortTimeout
		}
		return time.Hour
	}
	params := wfaActivityParams{
		ActivityTQ: actTQ, ActivityID: actID,
		StartToClose:  dur(model.StartToCloseElapses),
		RetryInterval: h.retryInterval, MaxAttempts: h.maxAttempts,
	}
	if h.shortTimeout == model.ScheduleToCloseElapses {
		params.ScheduleToClose = saaShortTimeout
	}
	if h.shortTimeout == model.ScheduleToStartElapses {
		params.ScheduleToStart = saaShortTimeout
	}
	if h.shortTimeout == model.HeartbeatElapses {
		params.Heartbeat = saaShortTimeout
	}
	wfID := testcore.RandomizeStr("wfa-run")
	run, err := h.env.SdkClient().ExecuteWorkflow(h.ctx,
		sdkclient.StartWorkflowOptions{ID: wfID, TaskQueue: wfTQ},
		wfaOneActivityWorkflow, params)
	require.NoError(t, err)
	return &wfaHandle{h: h, run: run, workflowID: wfID, runID: run.GetRunID(), activityID: actID, activityTQ: actTQ}
}

// terminal waits for the activity to reach a terminal state and reports it as the shared
// activityTerminalProjection. A workflow-activity's terminal outcome is not in PendingActivities; it is
// the outcome the workflow's ExecuteActivity().Get returns, so we read status and failure discriminant
// from the workflow-result error's cause. Parallel to saaHandle.terminal.
func (a *wfaHandle) terminal(t require.TestingT) activityTerminalProjection {
	err := a.run.Get(a.h.ctx, nil)
	if err == nil {
		return activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
	}
	// A canceled activity surfaces as a CanceledError directly (not wrapped in an ActivityError), so
	// check it before asserting the ActivityError shape.
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
	ctx, cancel := context.WithTimeout(a.h.ctx, timeout)
	defer cancel()
	resp, err := a.h.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.h.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.activityTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	require.NoError(t, err)
	if resp.GetActivityId() == "" {
		return nil
	}
	return resp
}

// rpc performs the worker RPC for a non-Poll, non-wall-clock event and returns its error. Parallel to
// saaHandle.rpc, minus the operator commands (which are out of scope for the equivalence work).
func (a *wfaHandle) rpc(e model.Event) error {
	fc := a.h.env.FrontendClient()
	ns := a.h.env.Namespace().String()
	switch e.Kind {
	case model.Heartbeat:
		_, err := fc.RecordActivityTaskHeartbeat(a.h.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: saaHeartbeatDetails,
		})
		return err
	case model.RespondCompleted:
		_, err := fc.RespondActivityTaskCompleted(a.h.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RespondFailed:
		_, err := fc.RespondActivityTaskFailed(a.h.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker", Failure: saaFailure(e.Retryable, a.h.nextRetryDelay),
		})
		return err
	case model.RespondCanceled:
		_, err := fc.RespondActivityTaskCanceled(a.h.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: "worker",
		})
		return err
	case model.RequestCancel:
		// WFA cancel comes from the workflow: signal it to cancel the activity, then wait until the
		// server reflects CANCEL_REQUESTED so a following RespondCanceled is accepted (SAA's direct
		// RequestCancelActivityExecution RPC is synchronous, so this makes the two comparable).
		if err := a.h.env.SdkClient().SignalWorkflow(a.h.ctx, a.workflowID, a.runID, wfaCancelSignal, nil); err != nil {
			return err
		}
		return a.waitForCancelRequested()
	default:
		return fmt.Errorf("wfaHarness: unhandled event kind %v", e.Kind)
	}
}

func (a *wfaHandle) waitForCancelRequested() error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := a.h.env.SdkClient().DescribeWorkflowExecution(a.h.ctx, a.workflowID, a.runID)
		if err != nil {
			return err
		}
		for _, pa := range resp.GetPendingActivities() {
			if pa.GetActivityId() == a.activityID && pa.GetState() == enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("wfaHarness: activity %q did not reach CANCEL_REQUESTED after signal", a.activityID)
}

// heartbeatDetails reports the last heartbeat checkpoint the activity recorded, as the first payload's
// raw bytes. Observable while the activity is running (still pending). Parallel to
// saaHandle.heartbeatDetails.
func (a *wfaHandle) heartbeatDetails(t require.TestingT) []byte {
	resp, err := a.h.env.SdkClient().DescribeWorkflowExecution(a.h.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return firstPayloadData(pa.GetHeartbeatDetails())
		}
	}
	require.FailNowf(t, "no pending activity", "activity %q not pending", a.activityID)
	return nil
}

// projection reads the activity's public info back via DescribeWorkflowExecution, as the shared
// activityInfoProjection. Parallel to saaHandle.projection.
func (a *wfaHandle) projection(t require.TestingT) activityInfoProjection {
	resp, err := a.h.env.SdkClient().DescribeWorkflowExecution(a.h.ctx, a.workflowID, a.runID)
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == a.activityID {
			return projectWFA(pa)
		}
	}
	require.FailNowf(t, "no pending activity", "activity %q not pending; workflow may have closed", a.activityID)
	return activityInfoProjection{}
}
