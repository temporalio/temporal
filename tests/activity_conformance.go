package tests

// The model-conformance engine shared by the activity surfaces: it drives each event with the surface's
// driver and checks the result against model.Transition(). Holds the random-walk explorer, the per-edge
// checker, error classification, and the parts of failure formatting that do not depend on how much a
// surface can see.
//
// SAA supplies its half in activity_standalone_conformance.go, WFA in activity_workflow_conformance.go.
// The BFS graph traversal is SAA-only and lives with it: it replays each path on a fresh activity, which
// on WFA would mean a workflow start per edge.

import (
	"cmp"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- the surface seam ------------------------------------------------------------------------

// activityConformanceSurface is what an explorer needs from a product surface: its event alphabet, and
// the ability to start a fresh activity of the surface's configuration.
type activityConformanceSurface interface {
	surfaceName() string
	configIndex() int
	candidateEvents() []model.Event
	startForConformance(t testing.TB) activityConformanceTarget
}

// activityConformanceTarget is one activity instance an explorer drives and checks. Beyond driving
// events (drivenActivity), a surface must be able to observe the activity and say whether what it sees
// agrees with the state the model predicts. How much it can see differs: SAA reads the CHASM component
// directly, WFA only what DescribeWorkflowExecution and the workflow history expose. A surface asserts
// what it can and reports the rest as agreeing, so the engine never demands an observation a surface
// cannot make.
type activityConformanceTarget interface {
	drivenActivity

	// conformsTo takes one observation of the activity and reports whether it agrees with the state the
	// model predicts, rendering the fields that differ when it does not.
	conformsTo(t require.TestingT, expected model.AbstractState) (bool, string)

	// awaitConformsTo polls the observation until it agrees with expected, or the deadline passes.
	awaitConformsTo(t testing.TB, expected model.AbstractState, deadline time.Time)

	// checkFinalEdge runs the checks worth an extra read only on the edge under test. A surface with
	// nothing to add beyond conformsTo does nothing here.
	checkFinalEdge(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome)

	// heartbeatFlags is the worker-facing flags the last heartbeat response carried.
	heartbeatFlags() model.HeartbeatFlags

	// nextAttemptScheduleTime is the dispatch deadline the server currently reports, nil when none.
	nextAttemptScheduleTime(t require.TestingT) *timestamppb.Timestamp
}

// activityPollTimeout is a poll timeout above common.MinLongPollTimeout, the floor below which the
// frontend rejects the poll rather than reaching matching.
const activityPollTimeout = common.MinLongPollTimeout + time.Second

// --- driving and checking one edge -------------------------------------------------------------

// activityApply is the outcome of driving one event, for the coverage ledger.
type activityApply int

const (
	activityVerified       activityApply = iota // the RPC was driven and the result checked
	activityMismatch                            // driven, but the result did not match the model
	activitySkippedNoToken                      // a worker RPC with no task token held; not drivable on this path
)

// applyActivityEvent drives one event and checks the result against the model's Outcome. final says
// whether this is the edge under test: only then is a divergence reported, and only then are the checks
// that cost an extra read run.
func applyActivityEvent(
	t testing.TB,
	a activityConformanceTarget,
	e model.Event,
	cur model.AbstractState,
	out model.Outcome,
	final bool,
) activityApply {
	if e.Type == model.PollType {
		return applyActivityPoll(t, a, cur, out, final)
	}
	if isTimerEvent(e.Type) {
		return applyActivityWallClock(t, a, e, cur, out, final)
	}
	s := a.driverState()
	// A worker RPC needs a task token, held only after a poll. An empty token yields a different error
	// than the model's NotFound, so the edge is not drivable on a never-polled path.
	if model.NeedsToken(e.Type) && s.token == nil {
		return activitySkippedNoToken
	}
	err := a.rpc(t, e)
	if model.CarriesReqID(e.Type) && out.Reject == model.NoError && out.Next.Status != cur.Status {
		// This request established a new state, so its id is the one a later SameRequestID reuses. An
		// intervening rejected or no-op request must not overwrite it.
		s.establishedReqID[e.Type] = s.lastReqID
	}
	// Drop an established id once the model no longer treats that op's SameRequestID replay as an
	// idempotent no-op: beyond that the server dedupes the consumed id, while the model, which tracks no
	// id history, expects a fresh op.
	for k := range s.establishedReqID {
		probe := model.Transition(s.cfg.modelConfig(), out.Next, model.Event{Type: k, SameRequestID: true})
		if probe.Reject != model.NoError || !probe.Next.SameObserved(out.Next) {
			delete(s.establishedReqID, k)
		}
	}
	ok := verifyActivityEdge(t, a, e, cur, out, err, final)
	if e.Type == model.HeartbeatType && out.Reject == model.NoError {
		observed, expected := a.heartbeatFlags(), model.ExpectedHeartbeatFlags(cur)
		if observed != expected {
			ok = false
			if final {
				t.Errorf("%s", activityFlagsFailure(s, e, cur.Status, observed, expected))
			}
		}
	}
	if ok {
		return activityVerified
	}
	return activityMismatch
}

// verifyActivityEdge checks the observed state and the RPC's accept/reject outcome against the model's
// predicted Outcome.
func verifyActivityEdge(
	t require.TestingT,
	a activityConformanceTarget,
	e model.Event,
	cur model.AbstractState,
	out model.Outcome,
	rpcErr error,
	final bool,
) bool {
	gotKind := activityRejectKind(rpcErr)
	stateOK, diff := a.conformsTo(t, out.Next)
	if final {
		if gotKind != out.Reject {
			t.Errorf("%s", activityRejectFailure(a.driverState(), e, cur.Status, gotKind, out.Reject, rpcErr))
		}
		if !stateOK {
			t.Errorf("%s", activityStateFailure(a.driverState(), e, cur, diff))
		}
		a.checkFinalEdge(t, e, cur, out)
	}
	return gotKind == out.Reject && stateOK
}

// applyActivityPoll drives a Poll and checks the dispatch against the model: a dispatchable activity
// must dispatch a task, and a delayed or paused one must not.
func applyActivityPoll(
	t testing.TB,
	a activityConformanceTarget,
	cur model.AbstractState,
	out model.Outcome,
	final bool,
) activityApply {
	s := a.driverState()
	poll := model.Poll
	switch {
	case cur.Status == model.Scheduled && out.Next.Status == model.Started:
		// A dispatchable activity must dispatch. The traces bound the deadline, so "Dispatchable" means
		// "dispatches promptly".
		timeout := cmp.Or(s.positivePollTimeout, activityDriverTimeout)
		resp := a.pollForTask(t, timeout)
		if resp == nil {
			if final {
				t.Errorf("%s: model expected STARTED but no task was dispatched within %s (scheduled, never dispatched)\n%s",
					s.edge(poll, cur.Status), timeout, s.pathLine())
			}
			return activityMismatch
		}
		s.token = resp.GetTaskToken()
		if final && resp.GetAttempt() != out.Next.AttemptCount {
			t.Errorf("%s: dispatched task attempt number disagrees — server saw %d, model expected %d\n%s",
				s.edge(poll, cur.Status), resp.GetAttempt(), out.Next.AttemptCount, s.pathLine())
		}
	case cur.Status == model.Scheduled && cur.Dispatchability != model.Dispatchable:
		// A start_delay or backoff is still pending, so the poll must find no task. Only worth a negative
		// poll when the configured delay outlasts a valid long poll; otherwise the state comparison below
		// suffices.
		if s.cfg.dispatchDelay(cur.Dispatchability) > activityPollTimeout {
			switch result, resp := activityNegativePoll(t, a); result {
			case dispatchedEarly:
				if final {
					t.Errorf("%s: model expected no dispatch (%s pending) but a task WAS dispatched (attempt %d)\n%s",
						s.edge(poll, cur.Status), cur.Dispatchability, resp.GetAttempt(), s.pathLine())
				}
				return activityMismatch
			case windowOutrun:
				if final {
					t.Errorf("%s: %s — %s was configured, but its dispatch time arrived before the check could "+
						"establish anything, so this edge went unchecked. Lengthen the window, or shorten the "+
						"trace ahead of it.\n%s",
						s.edge(poll, cur.Status), outranDispatchWindow, cur.Dispatchability, s.pathLine())
				}
				return activityMismatch
			default: // dispatchedNothing: the window held, which is what the model requires
			}
		}
	case cur.Status == model.Paused && !model.SkipNegativePoll():
		// A PAUSED activity must not dispatch: Pause invalidated the pending dispatch task.
		if resp := a.pollForTask(t, activityPollTimeout); resp != nil {
			if final {
				t.Errorf("%s: model expected no advance but a task WAS dispatched\n%s",
					s.edge(poll, cur.Status), s.pathLine())
			}
			return activityMismatch
		}
	default: // no other status can dispatch, so the state comparison below suffices
	}

	stateOK, diff := a.conformsTo(t, out.Next)
	if final {
		if !stateOK {
			t.Errorf("%s", activityStateFailure(s, poll, cur, diff))
		}
		a.checkFinalEdge(t, poll, cur, out)
	}
	if stateOK {
		return activityVerified
	}
	return activityMismatch
}

// applyActivityWallClock waits for a wall-clock event to take effect, then asserts the observed state
// equals out.Next. Where the model predicts an observable change it polls for that state; where it
// predicts none, the only way to confirm is to wait the window out and see nothing move.
func applyActivityWallClock(
	t testing.TB,
	a activityConformanceTarget,
	e model.Event,
	cur model.AbstractState,
	out model.Outcome,
	final bool,
) activityApply {
	s := a.driverState()
	deadline := time.Now().Add(s.cfg.timerDuration(e) + activityDriverTimerMargin)
	switch {
	case isDispatchDelayEvent(e.Type) && out.Next.Dispatchability == model.Dispatchable &&
		cur.Dispatchability != model.Dispatchable:
		// The delay elapsing is not visible in the state the oracle compares — Dispatchability is masked
		// out of SameObserved — so assert the public dispatch time passing instead.
		a.awaitDispatchDelay(t, e)
	case out.Next.SameObserved(cur):
		// The event leaves nothing for a read to observe, so there is no condition to poll for:
		// wait the window out and let the state comparison below stand for it.
		time.Sleep(time.Until(deadline)) //nolint:forbidigo
	default:
		a.awaitConformsTo(t, out.Next, deadline)
	}
	stateOK, diff := a.conformsTo(t, out.Next)
	if final {
		if !stateOK {
			t.Errorf("%s", activityStateFailure(s, e, cur, diff))
		}
		a.checkFinalEdge(t, e, cur, out)
	}
	if stateOK {
		return activityVerified
	}
	return activityMismatch
}

// --- the negative dispatch poll -----------------------------------------------------------------

// outranDispatchWindow marks a report as a driver failure rather than a product one: the dispatch
// window the negative poll meant to check had already closed by the time it ran.
const outranDispatchWindow = "the driver outran the dispatch window"

// negativePollResult is what a negative poll established about a pending dispatch window.
type negativePollResult int

const (
	dispatchedNothing negativePollResult = iota // the window held
	dispatchedEarly                             // a task arrived while the window was still open
	windowOutrun                                // the window closed too soon to establish either
)

// activityNegativePoll checks that a pending dispatch window dispatches nothing, bounding the poll by
// what the server says is left of the window. A task it does find is adjudicated by whether the dispatch
// time had arrived by the time the poll returned; a poll that straddles the boundary establishes nothing.
func activityNegativePoll(t require.TestingT, a activityConformanceTarget) (negativePollResult, *workflowservice.PollActivityTaskQueueResponse) {
	next := a.nextAttemptScheduleTime(t)
	if next == nil {
		return windowOutrun, nil // the dispatch time passed before the check began
	}
	dispatchTime := next.AsTime()
	timeout := min(activityPollTimeout, time.Until(dispatchTime))
	if timeout < common.MinLongPollTimeout {
		return windowOutrun, nil // too little left for a poll that reaches matching
	}
	resp := a.pollForTask(t, timeout)
	if resp == nil {
		return dispatchedNothing, nil
	}
	return adjudicateDispatch(time.Now(), dispatchTime), resp
}

// adjudicateDispatch says whether a task a negative poll found arrived early, or only as the window it
// was checking closed underneath it.
func adjudicateDispatch(polledUntil, dispatchTime time.Time) negativePollResult {
	if polledUntil.Before(dispatchTime) {
		return dispatchedEarly
	}
	return windowOutrun
}

// dispatchDelay is how long the pending delay in dispatchability d lasts under this config.
func (c activityConfig) dispatchDelay(d model.Dispatchability) time.Duration {
	switch d {
	case model.StartDelayPending:
		return c.StartDelay
	case model.BackoffPending:
		return cmp.Or(c.NextRetryDelay, c.retryInterval())
	default:
		return 0
	}
}

// --- the random walk ------------------------------------------------------------------------

// activityRandomWalk drives one activity forward, picking a random applicable event each step and
// checking it against model.Transition. On reaching a terminal state, or diverging, it restarts on a
// fresh activity, until the step budget is spent.
func activityRandomWalk(t *testing.T, s activityConformanceSurface, rng *rand.Rand, maxSteps int) {
	verbose := model.Verbose()
	seen := map[string]bool{}
	walks := 0

	a, cfg, cur := activityWalkStart(t, s)
	var trace []model.Event // events driven since the last (re)start, for the divergence report
	seen[model.Fingerprint(cur)] = true
	walks++
	if verbose {
		t.Logf("%s cfg %d walk %d: start %s", s.surfaceName(), s.configIndex(), walks, cur.Status)
	}

	for step := range maxSteps {
		if cur.Status.Terminal() {
			a, cfg, cur = activityWalkStart(t, s)
			trace = nil
			seen[model.Fingerprint(cur)] = true
			walks++
			if verbose {
				t.Logf("%s cfg %d walk %d: start %s (restart after terminal)", s.surfaceName(), s.configIndex(), walks, cur.Status)
			}
			continue
		}
		e := pickActivityWalkEvent(rng, s, a, cfg, cur)
		trace = append(trace, e)
		a.driverState().path = trace
		out := model.Transition(cfg, cur, e)
		res := applyActivityEvent(t, a, e, cur, out, true)
		if verbose {
			t.Logf("%s cfg %d walk %d step %d: %s", s.surfaceName(), s.configIndex(), walks, step, activityStepDesc(cur, e, out, res))
		}
		switch res {
		case activityVerified:
			cur = out.Next
			seen[model.Fingerprint(cur)] = true
		case activitySkippedNoToken:
			trace = trace[:len(trace)-1] // not driven, so not part of the trace
		case activityMismatch:
			// applyActivityEvent has already reported the divergence; restart from a known state.
			a, cfg, cur = activityWalkStart(t, s)
			trace = nil
			walks++
		default:
			t.Fatalf("unhandled apply result %d", res)
		}
	}
	t.Logf("%s cfg %d: random walk done — %d steps, %d walks, %d distinct states covered",
		s.surfaceName(), s.configIndex(), maxSteps, walks, len(seen))
}

// activityWalkStart begins a fresh activity and asserts it matches Initial(cfg).
func activityWalkStart(t *testing.T, s activityConformanceSurface) (activityConformanceTarget, model.Config, model.AbstractState) {
	a := s.startForConformance(t)
	cfg := a.driverState().cfg.modelConfig()
	cur := model.Initial(cfg)
	if ok, diff := a.conformsTo(t, cur); !ok {
		t.Fatalf("%s cfg %d: fresh activity disagrees with Initial(cfg)\n%s", s.surfaceName(), s.configIndex(), diff)
	}
	return a, cfg, cur
}

// pickActivityWalkEvent chooses the next event, strongly preferring one that makes non-terminal progress
// so the walk goes deep rather than restarting every few steps. It still sometimes takes a terminal or a
// reject/no-op edge, so those are exercised deep too. Events that cannot occur in the current state, or
// that need a task token the handle does not hold, are skipped.
func pickActivityWalkEvent(
	rng *rand.Rand,
	s activityConformanceSurface,
	a activityConformanceTarget,
	cfg model.Config,
	cur model.AbstractState,
) model.Event {
	var applicable, changing, deep []model.Event
	for _, e := range s.candidateEvents() {
		if !model.Possible(cfg, cur, e.Type) || (model.NeedsToken(e.Type) && a.driverState().token == nil) {
			continue
		}
		applicable = append(applicable, e)
		if out := model.Transition(cfg, cur, e); out.Reject == model.NoError && !out.Next.SameObserved(cur) {
			changing = append(changing, e)
			if !out.Next.Status.Terminal() {
				deep = append(deep, e)
			}
		}
	}
	switch {
	case len(deep) > 0 && rng.Float64() < 0.85:
		return deep[rng.Intn(len(deep))]
	case len(changing) > 0 && rng.Float64() < 0.5:
		return changing[rng.Intn(len(changing))]
	case len(applicable) > 0:
		return applicable[rng.Intn(len(applicable))]
	default:
		return model.Poll // needs no token, so always applicable
	}
}

// --- error / outcome classification --------------------------------------------------------

func activityRejectKindName(k model.ErrorKind) string {
	switch k {
	case model.NoError:
		return "NoError"
	case model.FailedPrecondition:
		return "FailedPrecondition"
	case model.NotFound:
		return "NotFound"
	case model.InvalidArgument:
		return "InvalidArgument"
	default:
		return fmt.Sprintf("unrecognized(%d)", int(k))
	}
}

func activityOutcomeDesc(k model.ErrorKind) string {
	if k == model.NoError {
		return "accepted"
	}
	return "rejected with " + activityRejectKindName(k)
}

// --- failure reporting ---------------------------------------------------------------------
//
// A failure means the server ("observed") disagreed with model.Transition ("expected") after one event.
// Each report is a one-line summary, the path to the edge, then the field-aligned diff the surface
// rendered.

// activityStepDesc renders one walk step as "FromStatus --Event--> ToStatus", annotated with the reject
// kind or a no-token skip.
func activityStepDesc(cur model.AbstractState, e model.Event, out model.Outcome, res activityApply) string {
	desc := fmt.Sprintf("%s --%s--> %s", cur.Status, e, out.Next.Status)
	switch {
	case res == activitySkippedNoToken:
		desc += "  [skipped: no token]"
	case out.Reject != model.NoError:
		desc += "  [" + activityRejectKindName(out.Reject) + "]"
	default: // a driven event the server accepted needs no annotation
	}
	return desc
}

// activityStateFailure reports that the state after an event disagreed with the model. An event that
// cannot occur is a legitimate edge to drive here, unlike in a trace: the model expects it to change
// nothing, and the failure is that the server changed something.
func activityStateFailure(s *activityDriverState, e model.Event, cur model.AbstractState, diff string) string {
	summary := "the state after this event disagrees with the model"
	if !model.Possible(s.cfg.modelConfig(), cur, e.Type) {
		summary = fmt.Sprintf("%s cannot occur in %s, so the model expected no change", e, cur.Status)
	}
	return fmt.Sprintf("%s: %s\n%s\n%s", s.edge(e, cur.Status), summary, s.pathLine(), diff)
}

// activityRejectFailure reports that the RPC's accept/reject outcome disagreed with the model.
func activityRejectFailure(s *activityDriverState, e model.Event, src model.Status, got, expected model.ErrorKind, err error) string {
	msg := fmt.Sprintf("%s: server %s, model expected %s\n%s",
		s.edge(e, src), activityOutcomeDesc(got), activityOutcomeDesc(expected), s.pathLine())
	if err != nil {
		msg += fmt.Sprintf("\n  server error: %v", err)
	}
	return msg
}

// activityFlagsFailure reports that the worker-facing heartbeat response flags disagreed with the model.
func activityFlagsFailure(s *activityDriverState, e model.Event, src model.Status, observed, expected model.HeartbeatFlags) string {
	fields := [][3]string{
		{"CancelRequested", fmt.Sprint(observed.CancelRequested), fmt.Sprint(expected.CancelRequested)},
		{"ActivityPaused", fmt.Sprint(observed.ActivityPaused), fmt.Sprint(expected.ActivityPaused)},
		{"ActivityReset", fmt.Sprint(observed.ActivityReset), fmt.Sprint(expected.ActivityReset)},
	}
	return fmt.Sprintf("%s: heartbeat response flags disagree\n%s\n%s",
		s.edge(e, src), s.pathLine(), activityDiffBlock(activitySplitFields(fields)))
}

// activityPathString renders the event sequence that reached an edge, e.g.
// "Schedule → Poll → RespondFailed[retryable=false]". The origin is labeled Schedule, the status the
// start lands in, rather than Started.
func activityPathString(path []model.Event) string {
	parts := make([]string, 0, len(path)+1)
	parts = append(parts, "Schedule")
	for _, e := range path {
		parts = append(parts, e.String())
	}
	return strings.Join(parts, " → ")
}

// activitySplitFields partitions (field, observed, expected) triples into those that differ and those
// that agree, the latter rendered as "field=value".
func activitySplitFields(fields [][3]string) (rows [][3]string, agree []string) {
	for _, f := range fields {
		if f[1] != f[2] {
			rows = append(rows, f)
		} else {
			agree = append(agree, f[0]+"="+f[1])
		}
	}
	return rows, agree
}

// activityDiffBlock formats differing (field, observed, expected) rows as an aligned three-column table
// and lists the agreeing "field=value" pairs beneath it.
func activityDiffBlock(rows [][3]string, agree []string) string {
	const hName, hObs, hExp = "field", "observed (server)", "expected (model)"
	nameW, obsW := len(hName), len(hObs)
	for _, r := range rows {
		nameW = max(nameW, len(r[0]))
		obsW = max(obsW, len(r[1]))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  %-*s   %-*s   %s\n", nameW, hName, obsW, hObs, hExp)
	for _, r := range rows {
		fmt.Fprintf(&b, "  %-*s   %-*s   %s\n", nameW, r[0], obsW, r[1], r[2])
	}
	if len(agree) > 0 {
		fmt.Fprintf(&b, "  agree: %s", strings.Join(agree, " "))
	}
	return b.String()
}
