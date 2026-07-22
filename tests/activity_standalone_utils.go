package tests

// Driver for standalone-activity (SAA) tests: starts an activity and drives it through a scripted
// sequence of events (a trace), realizing each event as the corresponding frontend RPC / poll /
// wall-clock wait. It does not make assertions about behavior. The event DSL is the archetype model
// package (chasm/lib/activity/model); this file is the SAA adapter that realizes it.

import (
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
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// --- driver --------------------------------------------------------------------------------

type saaHarness struct {
	env      *standaloneActivityEnv
	ctx      context.Context
	chasmCtx context.Context // for ReadComponent (observed)
	nsID     string
	cfg      model.Config
	cfgIdx   int
	counter  int
	// activity-id prefix so subtests don't collide; empty falls back to a cfgIdx-based prefix.
	idBase string
	// shortTimeout, set to one of the timeout *Elapses kinds, makes that timeout short at Start so a
	// trace can fire it. Zero (Poll) leaves all timeouts long.
	shortTimeout model.EventKind
	startDelay   time.Duration // StartActivityExecutionRequest.StartDelay
	// retryInterval is the RetryPolicy InitialInterval; 0 => a short default backoff.
	retryInterval time.Duration
	// backoffCoefficient is the RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval).
	backoffCoefficient float64
	// maxRetryInterval is the RetryPolicy MaximumInterval; 0 => the InitialInterval.
	maxRetryInterval time.Duration
	// nextRetryDelay overrides the policy backoff via ApplicationFailureInfo.NextRetryDelay on RespondFailed.
	nextRetryDelay time.Duration
	// scheduleToClose, when >0, sets a finite ScheduleToCloseTimeout (overriding the long default) so a
	// trace can make a retry fail to fit before the deadline.
	scheduleToClose time.Duration
	// positivePollTimeout bounds a "must dispatch" poll; 0 => 10s.
	positivePollTimeout time.Duration
	// customizeStart mutates the StartActivityExecutionRequest before it is sent — the seam for niche
	// start-time config the harness stays ignorant of.
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

const saaShortTimeout = 2 * time.Second

// saaWallClockSettle is slack added to a wall-clock event's clock when waiting for it to fire.
const saaWallClockSettle = 2 * time.Second

// saaPollTimeout bounds a poll: just above common.MinLongPollTimeout, so a "must not dispatch" poll
// genuinely reaches matching (below the floor the frontend rejects it, making the check vacuous).
const saaPollTimeout = common.MinLongPollTimeout + time.Second

// saaHandle is a handle to one activity instance: the token last dispatched to it plus the ids to
// address it, so a caller can issue further RPCs and read its state back.
type saaHandle struct {
	h             *saaHarness
	activityID    string
	taskQueue     string
	runID         string
	token         []byte
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse
	// establishedReqID[kind] is the request id that established the current idempotent state for an
	// operator command; a SameRequestID event reuses it. lastReqID is the id of the most recent operator
	// RPC, promoted into establishedReqID by apply when that RPC changes state.
	establishedReqID map[model.EventKind]string
	lastReqID        string
	path             []model.Event // events driven to reach the edge under test, for failure reports

	// Raw stamps read across the edge under test. observed() shifts cur->prev on each read, so their
	// inequality is the stamp bump across the last edge (see checkTaskInvalidation).
	prevStamp, curStamp       int32
	prevSTCStamp, curSTCStamp int32
}

// driveTrace runs a trace on a fresh activity, realizing each event against the server, and returns a
// handle at the reached state. Model-free: each RPC must succeed, a poll captures the dispatched token,
// a wall-clock event waits out its window.
func (h *saaHarness) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
	a := h.start(t)
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event: a poll captures the dispatched token, a wall-clock
// event is waited out, any other event is its RPC (which must succeed).
func (a *saaHandle) driveEvent(t require.TestingT, e model.Event) {
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

// driveTraceWithModelConformanceChecking drives a trace like driveTrace but checks conformance to the
// model at every step: after Start the observed state must equal model.Initial(cfg), then each event's
// outcome is predicted with model.Transition and verified via apply (see the spec harness). Use it
// whenever the config is fully modeled (no customizeStart the model cannot see).
func (h *saaHarness) driveTraceWithModelConformanceChecking(t *testing.T, trace []model.Event) *saaHandle {
	a := h.start(t)
	a.path = trace
	cur := model.Initial(h.cfg)
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Fatalf("after Start, state disagrees with Initial(cfg).\n%s", saaStateDiff(obs, cur))
	}
	for _, e := range trace {
		out := model.Transition(h.cfg, cur, e)
		a.apply(t, e, cur, out, true)
		cur = out.Next
	}
	return a
}

func (h *saaHarness) start(t require.TestingT) *saaHandle {
	if h.cfg.HasStartDelay && h.startDelay <= 0 {
		require.Fail(t, "saaHarness misconfigured: cfg.HasStartDelay requires startDelay > 0")
	}
	h.counter++
	base := h.idBase
	if base == "" {
		base = fmt.Sprintf("saaexp-%d", h.cfgIdx)
	}
	id := fmt.Sprintf("%s-%d", base, h.counter)
	resp, err := h.env.FrontendClient().StartActivityExecution(h.ctx, h.startRequest(id, id))
	require.NoError(t, err)
	return &saaHandle{h: h, activityID: id, taskQueue: id, runID: resp.RunId, establishedReqID: map[model.EventKind]string{}}
}

func (h *saaHarness) startRequest(activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	long := durationpb.New(time.Hour)
	dur := func(k model.EventKind) *durationpb.Duration {
		if h.shortTimeout == k {
			return durationpb.New(saaShortTimeout)
		}
		return long
	}
	interval := 200 * time.Millisecond
	if h.retryInterval > 0 {
		interval = h.retryInterval
	}
	coefficient := 1.0
	if h.backoffCoefficient > 0 {
		coefficient = h.backoffCoefficient
	}
	maxInterval := interval
	if h.maxRetryInterval > 0 {
		maxInterval = h.maxRetryInterval
	}
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           h.env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        h.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: dur(model.StartToCloseElapses),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(interval),
			BackoffCoefficient: coefficient,
			MaximumInterval:    durationpb.New(maxInterval),
			MaximumAttempts:    h.cfg.MaxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	if h.startDelay > 0 {
		req.StartDelay = durationpb.New(h.startDelay)
	}
	if h.cfg.HasScheduleToClose {
		if h.scheduleToClose > 0 {
			req.ScheduleToCloseTimeout = durationpb.New(h.scheduleToClose)
		} else {
			req.ScheduleToCloseTimeout = dur(model.ScheduleToCloseElapses)
		}
	}
	if h.cfg.HasScheduleToStart {
		req.ScheduleToStartTimeout = dur(model.ScheduleToStartElapses)
	}
	if h.cfg.HasHeartbeat {
		req.HeartbeatTimeout = dur(model.HeartbeatElapses)
	}
	if h.customizeStart != nil {
		h.customizeStart(req)
	}
	return req
}

// describe returns the DescribeActivityExecution response, the public surface a functional test
// asserts on. Outcome / last failure / heartbeat details are included so a caller can assert on a
// closed activity's result or a running one's checkpoint.
func (a *saaHandle) describe(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := a.h.env.FrontendClient().DescribeActivityExecution(a.h.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:               a.h.env.Namespace().String(),
		ActivityId:              a.activityID,
		RunId:                   a.runID,
		IncludeOutcome:          true,
		IncludeLastFailure:      true,
		IncludeHeartbeatDetails: true,
	})
	require.NoError(t, err)
	return resp
}

// projection reads the activity's public info back as the shared activityInfoProjection (defined in
// activity_utils.go). Parallel to wfaHandle.projection.
func (a *saaHandle) projection(t require.TestingT) activityInfoProjection {
	return projectSAA(a.describe(t).GetInfo())
}

// terminal reports the terminal state as the shared activityTerminalProjection: status from Info, the
// failure discriminant from the terminal Outcome. Parallel to wfaHandle.terminal.
func (a *saaHandle) terminal(t require.TestingT) activityTerminalProjection {
	resp := a.describe(t)
	return activityTerminalProjection{
		Status:      resp.GetInfo().GetStatus(),
		FailureType: saaFailureType(resp.GetOutcome().GetFailure()),
	}
}

// terminalCause reports the underlying failure a terminal timeout chains as its Cause, as the shared
// failureCause (empty if none). Parallel to wfaHandle.terminalCause.
func (a *saaHandle) terminalCause(t require.TestingT) failureCause {
	cause := a.describe(t).GetOutcome().GetFailure().GetCause()
	return failureCause{Type: saaFailureType(cause), Message: cause.GetMessage()}
}

// heartbeatDetails reports the last heartbeat checkpoint, as the first payload's raw bytes. Parallel
// to wfaHandle.heartbeatDetails.
func (a *saaHandle) heartbeatDetails(t require.TestingT) []byte {
	return firstPayloadData(a.describe(t).GetInfo().GetHeartbeatDetails())
}

// saaHeartbeatDetails is the fixed checkpoint payload the drivers send with a Heartbeat event, so a
// test can assert it round-trips identically on both surfaces.
var saaHeartbeatDetails = &commonpb.Payloads{Payloads: []*commonpb.Payload{{
	Metadata: map[string][]byte{"encoding": []byte("json/plain")},
	Data:     []byte(`"hb"`),
}}}

func firstPayloadData(p *commonpb.Payloads) []byte {
	if ps := p.GetPayloads(); len(ps) > 0 {
		return ps[0].GetData()
	}
	return nil
}

// saaFailureType extracts the failure discriminant a caller compares across surfaces: the application
// failure Type, the TimeoutType string, or "" if neither.
func saaFailureType(f *failurepb.Failure) string {
	if app := f.GetApplicationFailureInfo(); app != nil {
		return app.GetType()
	}
	if to := f.GetTimeoutFailureInfo(); to != nil {
		return to.GetTimeoutType().String()
	}
	return ""
}

func projectSAA(i *apiactivitypb.ActivityExecutionInfo) activityInfoProjection {
	return activityInfoProjection{
		State:                  i.GetRunState(),
		Attempt:                i.GetAttempt(),
		CurrentRetryInterval:   i.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleSet: i.GetNextAttemptScheduleTime() != nil,
	}
}

// observed reads the activity's internal state back via ReadComponent, as the model's AbstractState.
// It shifts cur->prev for the raw stamps so callers can compare the stamp change across the last edge.
func (a *saaHandle) observed() (model.AbstractState, error) {
	o, err := saaReadObserved(a.h.chasmCtx, a.h.nsID, a.activityID, a.runID)
	if err != nil {
		return model.AbstractState{}, err
	}
	a.prevStamp, a.curStamp = a.curStamp, o.Stamp
	a.prevSTCStamp, a.curSTCStamp = a.curSTCStamp, o.ScheduleToCloseStamp
	return model.Abstract(o), nil
}

// rpc performs the RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.h.env.FrontendClient()
	ns := a.h.env.Namespace().String()
	switch e.Kind {
	case model.Heartbeat:
		resp, err := fc.RecordActivityTaskHeartbeat(a.h.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: saaHeartbeatDetails,
		})
		a.lastHeartbeat = resp
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
		_, err := fc.RequestCancelActivityExecution(a.h.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.Terminate:
		_, err := fc.TerminateActivityExecution(a.h.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.Pause:
		_, err := fc.PauseActivityExecution(a.h.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op", Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.Unpause:
		_, err := fc.UnpauseActivityExecution(a.h.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			ResetAttempts: e.ResetAttempts, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.Reset:
		_, err := fc.ResetActivityExecution(a.h.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: "op",
			KeepPaused: e.KeepPaused, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptions:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaHarness: unhandled event kind %v", e.Kind)
	}
}

func (a *saaHandle) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.h.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, Identity: "op",
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
	_, err := a.h.env.FrontendClient().UpdateActivityExecutionOptions(a.h.ctx, req)
	return err
}

// reqID returns the request id for an operator command. A SameRequestID event reuses the id that
// established the current state for that command kind; otherwise a fresh id. The chosen id is recorded
// as lastReqID and promoted to establishedReqID by apply when the RPC changes state.
func (a *saaHandle) reqID(e model.Event) string {
	id := uuid.NewString()
	if e.SameRequestID {
		if est, ok := a.establishedReqID[e.Kind]; ok {
			id = est
		}
	}
	a.lastReqID = id
	return id
}

func (a *saaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.h.ctx, timeout)
	defer cancel()
	resp, err := a.h.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.h.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	// Matching signals "waited, found nothing" with an empty response and a nil error, so a genuine
	// no-task result never surfaces as an error; any error means the poll did not complete cleanly.
	if err != nil {
		if a.h.ctx.Err() != nil {
			return nil // teardown
		}
		if deadline, ok := a.h.ctx.Deadline(); ok && time.Until(deadline) < common.MinLongPollTimeout {
			t.Errorf("saaHarness: test context budget exhausted before the poll could run (%.1fs left, need >= %s). "+
				"Raise TEMPORAL_TEST_TIMEOUT and `go test -timeout`.\n  %v",
				time.Until(deadline).Seconds(), common.MinLongPollTimeout, err)
			return nil
		}
		t.Errorf("saaHarness bug: PollActivityTaskQueue did not complete cleanly (poll timeout must be >= "+
			"MinLongPollTimeout; only an empty response with a nil error means \"no task\"): %v", err)
		return nil
	}
	if resp.GetActivityId() == "" {
		return nil // matching's authoritative "waited, no task available"
	}
	return resp
}

// eventClock is how long the clock behind a wall-clock event takes to elapse: a timeout is short, a
// dispatch delay lasts dispatchDelay.
func (h *saaHarness) eventClock(e model.Event) time.Duration {
	switch e.Kind {
	case model.StartDelayElapses:
		return h.dispatchDelay(model.StartDelayPending)
	case model.BackoffElapses:
		return h.dispatchDelay(model.BackoffPending)
	default: // the four timeouts
		return saaShortTimeout
	}
}

// dispatchDelay is how long the harness configured the pending delay to last; a worker next_retry_delay
// overrides the policy interval for a backoff.
func (h *saaHarness) dispatchDelay(d model.Dispatchability) time.Duration {
	switch d {
	case model.StartDelayPending:
		return h.startDelay
	case model.BackoffPending:
		if h.nextRetryDelay > 0 {
			return h.nextRetryDelay
		}
		return h.retryInterval
	default:
		return 0
	}
}

func saaReadObserved(chasmCtx context.Context, nsID, activityID, runID string) (model.Observed, error) {
	ref := chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
		NamespaceID: nsID, BusinessID: activityID, RunID: runID,
	})
	return chasm.ReadComponent(chasmCtx, ref, func(act *activity.Activity, cctx chasm.Context, _ struct{}) (model.Observed, error) {
		attempt := act.LastAttempt.Get(cctx)
		return model.Observed{
			Status:               act.GetStatus(),
			Count:                attempt.GetCount(),
			Stamp:                attempt.GetStamp(),
			ScheduleToCloseStamp: act.GetScheduleToCloseStamp(),
			ResetKeepPaused:      act.GetResetKeepPaused(),
			ResetRestoreOptions:  act.GetResetRestoreOptions(),
			FirstAttemptStarted:  act.GetFirstAttemptStartedTime() != nil,
			DispatchTimeSet:      attempt.GetDispatchTime() != nil,
		}, nil
	}, struct{}{})
}

// saaIsWallClock reports whether an event fires on wall-clock time (the four timeouts and the two
// dispatch-delay windows) rather than synchronously like an RPC.
func saaIsWallClock(k model.EventKind) bool {
	switch k {
	case model.ScheduleToStartElapses, model.ScheduleToCloseElapses, model.StartToCloseElapses,
		model.HeartbeatElapses, model.StartDelayElapses, model.BackoffElapses:
		return true
	default:
		return false
	}
}

// saaTimeoutIn returns the timeout whose *Elapses event a trace fires (zero if none) — the signal to
// configure that timeout short at Start. Traces fire at most one timeout, as their final event.
func saaTimeoutIn(trace []model.Event) model.EventKind {
	for _, e := range trace {
		switch e.Kind {
		case model.ScheduleToStartElapses, model.ScheduleToCloseElapses,
			model.StartToCloseElapses, model.HeartbeatElapses:
			return e.Kind
		}
	}
	return 0 // none; zero value (Poll) means no timeout is shortened
}

func saaFailure(retryable bool, nextRetryDelay time.Duration) *failurepb.Failure {
	info := &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: !retryable}
	if nextRetryDelay > 0 {
		info.NextRetryDelay = durationpb.New(nextRetryDelay)
	}
	return &failurepb.Failure{
		Message:     "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
	}
}

// --- traces --------------------------------------------------------------------------------
//
// A trace is an event sequence run once on one fresh activity. The timing knobs configure the activity
// and how long the driver waits; writing a timeout's *Elapses event into the script is what makes the
// harness configure that timeout short so it fires.

type saaTrace struct {
	trace          []model.Event
	maxAttempts    int32         // RetryPolicy MaximumAttempts (0 = unlimited); the rest of Config is derived (see config)
	startDelayed   bool          // activity created with a start_delay; the window length is derived (see startDelay)
	retryInterval  time.Duration // RetryPolicy interval; how long the driver waits for BackoffElapses
	nextRetryDelay time.Duration // worker-supplied next_retry_delay override of the policy backoff
	// customizeStart mutates the StartActivityExecutionRequest before it is sent — the seam for niche,
	// start-time config the harness does not model (see saaHarness.customizeStart).
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

// config derives the model Config from the trace. Only MaxAttempts is free; the timeout flags are
// implied by which timeouts the trace fires.
func (tr saaTrace) config() model.Config {
	cfg := model.Config{MaxAttempts: tr.maxAttempts, HasStartDelay: tr.startDelayed}
	for _, e := range tr.trace {
		switch e.Kind {
		case model.ScheduleToStartElapses:
			cfg.HasScheduleToStart = true
		case model.ScheduleToCloseElapses:
			cfg.HasScheduleToClose = true
		case model.HeartbeatElapses:
			cfg.HasHeartbeat = true
		}
	}
	return cfg
}

// startDelay is the activity's start_delay: short (a real wait) when the trace fires StartDelayElapses,
// otherwise long enough to stay open for the whole trace. Zero when not start-delayed.
func (tr saaTrace) startDelay() time.Duration {
	if !tr.startDelayed {
		return 0
	}
	for _, e := range tr.trace {
		if e.Kind == model.StartDelayElapses {
			return saaDelayWindow
		}
	}
	return saaLongStartDelay
}

// saaDelayWindow is long enough to outlast a valid negative long poll (> the long-poll minimum), so
// "not dispatchable yet" is observable during a start-delay or backoff window.
const saaDelayWindow = 5 * time.Second

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole trace.
const saaLongStartDelay = time.Hour

var (
	saaPoll               = model.Event{Kind: model.Poll}
	saaComplete           = model.Event{Kind: model.RespondCompleted}
	saaFailRetryably      = model.Event{Kind: model.RespondFailed, Retryable: true}
	saaFailNonRetryably   = model.Event{Kind: model.RespondFailed, Retryable: false}
	saaPause              = model.Event{Kind: model.Pause}
	saaRequestCancel      = model.Event{Kind: model.RequestCancel}
	saaStartDelayElapse   = model.Event{Kind: model.StartDelayElapses}
	saaBackoffDelayElapse = model.Event{Kind: model.BackoffElapses}
	saaStartToCloseElapse = model.Event{Kind: model.StartToCloseElapses}
)
