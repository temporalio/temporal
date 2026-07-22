package tests

// Model-checking engine for the standalone-activity spec. It builds on the model-free driver in
// activity_standalone_utils.go: for each event it drives the corresponding action against a real
// onebox server (via the driver) and checks the result against model.Transition(). The graph
// traversal, the random-walk explorer, and the per-step trace checker live here, along with the
// tuning knobs; below the "helpers" divider are fingerprints, event enumeration, error
// classification, and failure formatting. The editable configs and test entry points are in
// activity_standalone_spec_test.go.

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
)

// --- tuning knobs --------------------------------------------------------------------------

// saaMaxDepth is the BFS depth cap. The default keeps CI fast; TEMPORAL_SAASPEC_MAX_DEPTH raises it
// for deeper local verification. Cost grows with depth — mostly the per-Paused negative poll, which
// TEMPORAL_SAASPEC_NO_NEGATIVE_POLL can disable (see applyPoll).
func saaMaxDepth() int {
	if v := os.Getenv("TEMPORAL_SAASPEC_MAX_DEPTH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 4
}

// saaSkipNegativePoll disables the ~3s "a Paused activity must not dispatch" long-poll — the
// dominant cost of deep walks. Set TEMPORAL_SAASPEC_NO_NEGATIVE_POLL for fast deep runs; the
// per-edge state check still verifies the Paused transition, only the matching-level assertion is
// dropped.
func saaSkipNegativePoll() bool { return os.Getenv("TEMPORAL_SAASPEC_NO_NEGATIVE_POLL") != "" }

func saaWalkSteps() int {
	if v := os.Getenv("TEMPORAL_SAASPEC_WALK_STEPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 200 // a bare `go test` (90s per-test context) smoke; raise it with TEMPORAL_TEST_TIMEOUT for real exploration
}

func saaWalkSeed() int64 {
	if v := os.Getenv("TEMPORAL_SAASPEC_WALK_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 1 // deterministic default; override for a different walk
}

func saaVerbose() bool { return os.Getenv("TEMPORAL_SAASPEC_VERBOSE") != "" }

// --- harness / engine ----------------------------------------------------------------------

// traverse does a breadth-first walk of the model's reachable states, verifying every decided
// edge against the server.
func (h *saaHarness) traverse(t *testing.T) {
	type node struct {
		path  []model.Event
		state model.AbstractState
	}
	start := model.Initial(h.cfg)
	visited := map[string]bool{model.Fingerprint(start): true}
	frontier := []node{{nil, start}}

	verifiedCells := map[saaCell]bool{}
	skippedCells := map[saaCell]bool{}
	// Fingerprint-granularity ledger (includes attempt-count bucket etc.) for the completeness check.
	verifiedFine := map[string]bool{}
	skippedFine := map[string]bool{}

	h.verifyPath(t, nil) // the freshly started activity matches Initial(cfg)

	edges, states := 0, 1
	maxDepth := saaMaxDepth()
	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []node
		for _, nd := range frontier {
			for _, e := range saaCandidateEvents() {
				out := model.Transition(h.cfg, nd.state, e)
				edges++
				path := append(append([]model.Event{}, nd.path...), e)
				res, reached := h.verifyPath(t, path)
				if reached {
					c := saaCell{nd.state.Status, e.Kind}
					key := model.CellKey(nd.state, e.Kind)
					if res == saaSkippedNoToken {
						skippedCells[c] = true
						skippedFine[key] = true
					} else {
						verifiedCells[c] = true
						verifiedFine[key] = true
					}
				}
				if out.Reject != model.NoError {
					continue // rejected/no-op: no new state to extend from
				}
				fp := model.Fingerprint(out.Next)
				if !visited[fp] {
					visited[fp] = true
					states++
					next = append(next, node{path, out.Next})
				}
			}
		}
		frontier = next
	}

	// Coverage ledger. The only decided edges the traversal does not verify are worker RPCs reached
	// on a path that never polled (no task token to send); surface those so the gap stays visible.
	t.Logf("cfg %d: verified %d decided edges (%d distinct cells) across %d reachable states (depth<=%d)",
		h.cfgIdx, edges, len(verifiedCells), states, maxDepth)

	// Coverage detail (the no-token-skip ledger) prints only under TEMPORAL_SAASPEC_COMPLETENESS, so
	// the default output is just the spec violations.
	if os.Getenv("TEMPORAL_SAASPEC_COMPLETENESS") != "" {
		var unexercised []string
		for c := range skippedCells {
			if !verifiedCells[c] {
				unexercised = append(unexercised, fmt.Sprintf("%s/%s", c.status, model.KindName(c.kind)))
			}
		}
		sort.Strings(unexercised)
		if len(unexercised) > 0 {
			t.Logf("cfg %d: decided cells NOT exercised (worker RPC, no token on a never-polled path): %v",
				h.cfgIdx, unexercised)
		}
	}

	h.checkCompleteness(t, verifiedFine, skippedFine)
}

// checkCompleteness is an informational (never-failing) coverage report: it compares what this run
// verified/skipped against the model's own reachable set (computed server-free to fixpoint, no depth
// bound). Cells the model can reach but this run did not are what the depth cap left out.
func (h *saaHarness) checkCompleteness(t *testing.T, verifiedFine, skippedFine map[string]bool) {
	if os.Getenv("TEMPORAL_SAASPEC_COMPLETENESS") == "" {
		return
	}
	var gaps []string
	for key := range model.Reachable(h.cfg, saaCandidateEvents()) {
		if verifiedFine[key] || skippedFine[key] {
			continue
		}
		gaps = append(gaps, key)
	}
	if len(gaps) == 0 {
		return
	}
	sort.Strings(gaps)
	shown := gaps
	suffix := ""
	if len(shown) > 30 {
		shown, suffix = shown[:30], fmt.Sprintf("\n  … and %d more", len(gaps)-30)
	}
	t.Logf("cfg %d: %d model-reachable cell(s) not exercised at depth<=%d (raise TEMPORAL_SAASPEC_MAX_DEPTH to reach deeper).\n"+
		"  fingerprint = Status|count|resetKeepPaused|resetHeartbeats|resetRestoreOpts|firstStarted|dispatchSet|dispatch\n  %s%s",
		h.cfgIdx, len(gaps), saaMaxDepth(), strings.Join(shown, "\n  "), suffix)
}

// verifyPath starts a fresh activity, replays the path (asserting only the final edge), and
// aborts silently if a prefix edge diverges — that edge is reported when it is itself a final
// edge of its own shorter path.
func (h *saaHarness) verifyPath(t require.TestingT, path []model.Event) (saaApply, bool) {
	a := h.start(t)
	a.path = path
	cur := model.Initial(h.cfg)

	// The freshly started activity should match Initial(cfg).
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Errorf("cfg %d: state immediately after StartActivityExecution disagrees with Initial(cfg).\n%s",
			h.cfgIdx, saaStateDiff(obs, cur))
		return saaMismatch, false
	}

	for i, e := range path {
		out := model.Transition(h.cfg, cur, e)
		final := i == len(path)-1
		res := a.apply(t, e, cur, out, final)
		if final {
			return res, true // res concerns the final edge, which the ledger records
		}
		if res != saaVerified {
			return res, false // prefix diverged or was skipped; that edge is checked as its own path
		}
		cur = out.Next
	}
	return saaVerified, false // empty path: only the Initial check ran
}

// saaApply is the outcome of driving one event, for the coverage ledger.
type saaApply int

const (
	saaVerified       saaApply = iota // the RPC was driven and the result checked
	saaMismatch                       // driven, but the result did not match the model
	saaSkippedNoToken                 // a worker RPC with no task token held; not drivable on this path
)

// saaCell identifies a (source status, event kind) pair for the coverage ledger.
type saaCell struct {
	status model.Status
	kind   model.EventKind
}

func (a *saaHandle) apply(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome, final bool) saaApply {
	if e.Kind == model.Poll {
		return a.applyPoll(cur, out, final, t)
	}
	if saaIsWallClock(e.Kind) {
		return a.applyWallClock(t, e, cur, out, final)
	}
	// Worker RPCs need a task token, held only after a poll. On a never-polled path there is no token,
	// and an empty token yields a different error than the spec's NotFound, so the edge is not drivable
	// here; the ledger records the skip.
	if model.NeedsToken(e.Kind) && a.token == nil {
		return saaSkippedNoToken
	}
	err := a.rpc(e)
	if model.CarriesReqID(e.Kind) && out.Reject == model.NoError && out.Next.Status != cur.Status {
		// This request established a new state; its id is the one a later SameRequestID reuses for the
		// server's request-id idempotency. An intervening rejected/no-op request must not overwrite it.
		a.establishedReqID[e.Kind] = a.lastReqID
	}
	// Drop an established id once the activity leaves the region where the model still treats that op's
	// SameRequestID replay as an idempotent no-op (probe the model, so this tracks the idempotency spec
	// exactly — e.g. a keep-paused Reset preserves a pending Pause's id). Beyond that region the server
	// would dedupe the stale, already-consumed id, but the model — which can't track id history —
	// expects a fresh op, so a later SameRequestID must use a fresh id.
	for k := range a.establishedReqID {
		probe := model.Transition(a.h.cfg, out.Next, model.Event{Kind: k, SameRequestID: true})
		if probe.Reject != model.NoError || !probe.Next.SameObserved(out.Next) {
			delete(a.establishedReqID, k)
		}
	}
	ok := a.verify(t, e, cur, out, err, final)
	if e.Kind == model.Heartbeat && out.Reject == model.NoError {
		observed := model.HeartbeatFlags{
			CancelRequested: a.lastHeartbeat.GetCancelRequested(),
			ActivityPaused:  a.lastHeartbeat.GetActivityPaused(),
			ActivityReset:   a.lastHeartbeat.GetActivityReset(),
		}
		expected := model.ExpectedHeartbeatFlags(cur)
		if observed != expected {
			ok = false
			if final {
				t.Errorf("%s", a.flagsFailure(e, cur.Status, observed, expected))
			}
		}
	}
	if ok {
		return saaVerified
	}
	return saaMismatch
}

// verify checks the current state against the model's predicted Outcome and, on the final edge, the
// public Describe projection.
func (a *saaHandle) verify(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome, rpcErr error, final bool) bool {
	gotKind := saaRejectKind(rpcErr)
	obs, err := a.observed()
	require.NoError(t, err)
	ok := gotKind == out.Reject && out.Next.SameObserved(obs)
	if final {
		if gotKind != out.Reject {
			t.Errorf("%s", a.rejectFailure(e, cur.Status, gotKind, out.Reject, rpcErr))
		}
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(e, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
		a.checkTaskInvalidation(t, e, cur, out)
	}
	return ok
}

// checkTaskInvalidation compares whether each raw stamp changed across the edge under test against the
// model's per-transition invalidation bools. observed() has already refreshed cur/prev for this edge.
func (a *saaHandle) checkTaskInvalidation(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome) {
	gotAttempt := a.curStamp != a.prevStamp
	gotSTC := a.curSTCStamp != a.prevSTCStamp
	if gotAttempt != out.AttemptTasksInvalidated {
		t.Errorf("%s: attempt-task invalidation disagrees — server %v, model %v\n%s",
			a.edge(e, cur.Status), gotAttempt, out.AttemptTasksInvalidated, a.pathLine())
	}
	if gotSTC != out.ScheduleToCloseTaskInvalidated {
		t.Errorf("%s: schedule-to-close-task invalidation disagrees — server %v, model %v\n%s",
			a.edge(e, cur.Status), gotSTC, out.ScheduleToCloseTaskInvalidated, a.pathLine())
	}
}

func (a *saaHandle) applyPoll(cur model.AbstractState, out model.Outcome, final bool, t require.TestingT) saaApply {
	poll := model.Event{Kind: model.Poll}
	switch {
	case cur.Status == model.Scheduled && out.Next.Status == model.Started:
		// Positive: a SCHEDULED+Dispatchable activity must be dispatched. The traces bound this
		// deadline so "Dispatchable" means "dispatches promptly".
		timeout := 10 * time.Second
		if a.h.positivePollTimeout > 0 {
			timeout = a.h.positivePollTimeout
		}
		resp := a.pollForTask(t, timeout)
		if resp == nil {
			if final {
				t.Errorf("%s: model expected STARTED but no task was dispatched within %s (scheduled, never dispatched)\n%s",
					a.edge(poll, cur.Status), timeout, a.pathLine())
			}
			return saaMismatch
		}
		a.token = resp.GetTaskToken()
		if final && resp.GetAttempt() != out.Next.AttemptCount {
			t.Errorf("%s: dispatched task attempt number disagrees — server saw %d, model expected %d\n%s",
				a.edge(poll, cur.Status), resp.GetAttempt(), out.Next.AttemptCount, a.pathLine())
		}
	case cur.Status == model.Scheduled && cur.Dispatchability != model.Dispatchable:
		// Delayed dispatch: a start_delay or backoff is still pending, so the poll finds no task. Verify
		// with a negative poll, but only when the delay outlasts a valid long poll (not under the
		// fast-backoff configs, where the state comparison below suffices).
		if dur := a.h.dispatchDelay(cur.Dispatchability); dur > saaPollTimeout {
			if resp := a.pollForTask(t, saaPollTimeout); resp != nil {
				if final {
					t.Errorf("%s: model expected no dispatch (%s pending) but a task WAS dispatched (attempt %d)\n%s",
						a.edge(poll, cur.Status), cur.Dispatchability, resp.GetAttempt(), a.pathLine())
				}
				return saaMismatch
			}
		}
	case cur.Status == model.Paused && !saaSkipNegativePoll():
		// A PAUSED activity must not be dispatchable (Pause invalidated the pending dispatch task).
		// The only status where a spurious dispatch is possible, so the only place we pay the
		// full long-poll wait; the timeout must exceed MinLongPollTimeout. TEMPORAL_SAASPEC_NO_NEGATIVE_POLL
		// disables it — the state check below still confirms Paused/dispatch.
		if resp := a.pollForTask(t, saaPollTimeout); resp != nil {
			if final {
				t.Errorf("%s: model expected no advance but a task WAS dispatched\n%s",
					a.edge(poll, cur.Status), a.pathLine())
			}
			return saaMismatch
		}
	}
	// Other statuses cannot dispatch, so skip the poll and rely on the state comparison below.
	obs, err := a.observed()
	require.NoError(t, err)
	if final {
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(poll, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
		a.checkTaskInvalidation(t, poll, cur, out)
	}
	if out.Next.SameObserved(obs) {
		return saaVerified
	}
	return saaMismatch
}

// applyWallClock waits for a wall-clock event (a timeout or a dispatch-delay clock) to take effect,
// then asserts the observed state equals Model.Next. When the model predicts an observable change it
// polls until the state reaches that target (so a late timer is tolerated up to the window) rather than
// reading once after a blind sleep; when it predicts no observable change — a stale/no-op timeout, or a
// dispatch-delay elapse whose only effect is the latent readiness a later Poll verifies — the only way
// to confirm the transition is to wait the window out and see nothing move.
func (a *saaHandle) applyWallClock(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome, final bool) saaApply {
	deadline := time.Now().Add(a.h.eventClock(e) + saaWallClockSettle)
	if out.Next.SameObserved(cur) {
		time.Sleep(time.Until(deadline))
	} else {
		a.awaitObservedMatch(out.Next, deadline)
	}
	obs, err := a.observed()
	require.NoError(t, err)
	if final {
		if !out.Next.SameObserved(obs) {
			t.Errorf("%s", a.stateFailure(e, cur.Status, obs, out.Next))
		}
		a.checkDescribe(t, out.Next)
		a.checkTaskInvalidation(t, e, cur, out)
	}
	if out.Next.SameObserved(obs) {
		return saaVerified
	}
	return saaMismatch
}

// The trace drivers live in activity_standalone_utils.go: driveTrace applies each event model-free,
// and driveTraceWithModelConformanceChecking drives each event through apply (below), checking it against the
// model. runTrace picks between them by whether a customizeStart hook injects config the model cannot
// see.

// checkDescribe asserts the public status and run state DescribeActivityExecution reports match
// ExpectedDescribe.
func (a *saaHandle) checkDescribe(t require.TestingT, expected model.AbstractState) {
	st, rs := model.ExpectedDescribe(expected)
	resp, err := a.h.env.FrontendClient().DescribeActivityExecution(a.h.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace: a.h.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
	})
	require.NoError(t, err)
	gotSt, gotRs := resp.GetInfo().GetStatus(), resp.GetInfo().GetRunState()
	if gotSt != st || gotRs != rs {
		t.Errorf("Describe while in internal status %s does not match model expectation\n%s\n"+
			"  server saw:     status=%v run=%v\n"+
			"  model expected: status=%v run=%v",
			expected.Status, a.pathLine(), gotSt, gotRs, st, rs)
	}
	if gotAttempt := resp.GetInfo().GetAttempt(); gotAttempt != expected.AttemptCount {
		t.Errorf("Describe attempt while in internal status %s does not match model expectation\n%s\n"+
			"  server saw:     attempt=%d\n"+
			"  model expected: attempt=%d",
			expected.Status, a.pathLine(), gotAttempt, expected.AttemptCount)
	}
}

// randomWalk drives one activity, picking a random applicable event each step and checking it against
// Model(); on reaching a terminal state (or after a divergence) it starts a fresh activity and keeps
// going until the step budget is spent. It reports the distinct states (by fingerprint) it covered.
func (h *saaHarness) randomWalk(t *testing.T, rng *rand.Rand, maxSteps int) {
	verbose := saaVerbose()
	seen := map[string]bool{}
	walks := 0

	a, cur := h.walkStart(t)
	var trace []model.Event // events driven since the last (re)start, so a divergence prints its path
	seen[model.Fingerprint(cur)] = true
	walks++
	if verbose {
		t.Logf("cfg %d walk %d: start %s", h.cfgIdx, walks, cur.Status)
	}

	for step := range maxSteps {
		if cur.Status.Terminal() {
			a, cur = h.walkStart(t)
			trace = nil
			seen[model.Fingerprint(cur)] = true
			walks++
			if verbose {
				t.Logf("cfg %d walk %d: start %s (restart after terminal)", h.cfgIdx, walks, cur.Status)
			}
			continue
		}
		e := h.pickWalkEvent(rng, a, cur)
		trace = append(trace, e)
		a.path = trace
		out := model.Transition(h.cfg, cur, e)
		res := a.apply(t, e, cur, out, true)
		if verbose {
			t.Logf("cfg %d walk %d step %d: %s", h.cfgIdx, walks, step, saaStepDesc(cur, e, out, res))
		}
		switch res {
		case saaVerified:
			cur = out.Next
			seen[model.Fingerprint(cur)] = true
		case saaSkippedNoToken:
			trace = trace[:len(trace)-1] // event not driven; drop it from the segment trace
		case saaMismatch:
			// apply() already reported the divergence (with a.path = this trace); restart from a known
			// state so the walk keeps exploring rather than compounding from a suspect one.
			a, cur = h.walkStart(t)
			trace = nil
			walks++
		}
	}
	t.Logf("cfg %d: random walk done — %d steps, %d walks, %d distinct states covered",
		h.cfgIdx, maxSteps, walks, len(seen))
}

// walkStart begins a fresh activity and asserts it matches Initial(cfg).
func (h *saaHarness) walkStart(t *testing.T) (*saaHandle, model.AbstractState) {
	a := h.start(t)
	cur := model.Initial(h.cfg)
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Fatalf("cfg %d: fresh activity disagrees with Initial(cfg)\n%s", h.cfgIdx, saaStateDiff(obs, cur))
	}
	return a, cur
}

// pickWalkEvent chooses the next event. It strongly prefers non-terminal progress so the walk
// wanders deep instead of restarting every few steps (terminal-reaching events like Terminate /
// RespondCompleted end a walk immediately); it still occasionally takes any changing edge (maybe
// terminal) or a reject/no-op so those are exercised in deep contexts too. Terminal edges are
// covered exhaustively by the BFS; here depth is the goal. Events needing a task token we do not
// hold are skipped (they would be un-drivable no-ops).
func (h *saaHarness) pickWalkEvent(rng *rand.Rand, a *saaHandle, cur model.AbstractState) model.Event {
	var applicable, changing, deep []model.Event
	for _, e := range saaCandidateEvents() {
		if model.NeedsToken(e.Kind) && a.token == nil {
			continue
		}
		applicable = append(applicable, e)
		if out := model.Transition(h.cfg, cur, e); out.Reject == model.NoError && !out.Next.SameObserved(cur) {
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
		return model.Event{Kind: model.Poll} // always applicable (needs no token)
	}
}

// --- helpers -------------------------------------------------------------------------------

// saaStepDesc renders one walk step as "FromStatus --Event--> ToStatus", annotated with the reject
// kind or a no-token skip.
func saaStepDesc(cur model.AbstractState, e model.Event, out model.Outcome, res saaApply) string {
	desc := fmt.Sprintf("%s --%s--> %s", cur.Status, model.EventLabel(e), out.Next.Status)
	switch {
	case res == saaSkippedNoToken:
		desc += "  [skipped: no token]"
	case out.Reject != model.NoError:
		desc += "  [" + saaRejectKindName(out.Reject) + "]"
	}
	return desc
}

func saaCandidateEvents() []model.Event {
	var out []model.Event
	simple := []model.EventKind{
		model.Poll, model.Heartbeat, model.RespondCompleted, model.RespondCanceled, model.UpdateOptions,
	}
	for _, k := range simple {
		out = append(out, model.Event{Kind: k})
	}
	// An update that changes start_delay: the model rejects it outside the StartDelayPending window
	// (the only window it is mutable in), which is every state the RPC-only traversal reaches.
	out = append(out, model.Event{Kind: model.UpdateOptions, SetsStartDelay: true})
	for _, r := range []bool{false, true} {
		out = append(out, model.Event{Kind: model.RespondFailed, Retryable: r})
	}
	for _, sr := range []bool{false, true} {
		out = append(out,
			model.Event{Kind: model.Pause, SameRequestID: sr},
			model.Event{Kind: model.Terminate, SameRequestID: sr},
			model.Event{Kind: model.RequestCancel, SameRequestID: sr},
		)
	}
	for _, kp := range []bool{false, true} {
		for _, ro := range []bool{false, true} {
			out = append(out, model.Event{Kind: model.Reset, KeepPaused: kp, RestoreOriginal: ro})
		}
	}
	for _, ra := range []bool{false, true} {
		out = append(out, model.Event{Kind: model.Unpause, ResetAttempts: ra})
	}
	return out
}

// --- error / outcome classification --------------------------------------------------------

func saaRejectKind(err error) model.ErrorKind {
	if err == nil {
		return model.NoError
	}
	// The FrontendClient returns Temporal serviceerror types, so classify by type rather than by
	// gRPC status code.
	var nf *serviceerror.NotFound
	var fp *serviceerror.FailedPrecondition
	var ia *serviceerror.InvalidArgument
	switch {
	case errors.As(err, &nf):
		return model.NotFound
	case errors.As(err, &fp):
		return model.FailedPrecondition
	case errors.As(err, &ia):
		return model.InvalidArgument
	default:
		return model.ErrorKind(-1) // unrecognized: will not match any predicted kind
	}
}

// --- failure reporting ---------------------------------------------------------------------
//
// A failure means the real server ("observed") disagreed with model.Transition ("expected") after one
// event. Each report opens with a one-line summary of what diverged, then the path to the edge, then
// a field-aligned diff.

// edge names the event and the status it was driven from, e.g. "RespondFailed[retryable=true] from
// Started".
func (a *saaHandle) edge(e model.Event, src model.Status) string {
	return fmt.Sprintf("%s from %s", model.EventLabel(e), src)
}

func (a *saaHandle) pathLine() string {
	return "  path: " + saaPathString(a.path)
}

// stateFailure reports that the persisted state after an event disagreed with the model.
func (a *saaHandle) stateFailure(e model.Event, src model.Status, observed, expected model.AbstractState) string {
	var summary string
	if observed.Status != expected.Status {
		summary = fmt.Sprintf("model expected %s, server saw %s", expected.Status, observed.Status)
	} else {
		summary = fmt.Sprintf("status %s agrees but persisted state differs", observed.Status)
	}
	return fmt.Sprintf("%s: %s\n%s\n%s", a.edge(e, src), summary, a.pathLine(), saaStateDiff(observed, expected))
}

// rejectFailure reports that the RPC's accept/reject outcome disagreed with the model.
func (a *saaHandle) rejectFailure(e model.Event, src model.Status, got, want model.ErrorKind, err error) string {
	msg := fmt.Sprintf("%s: server %s, model expected %s\n%s",
		a.edge(e, src), saaOutcomeDesc(got), saaOutcomeDesc(want), a.pathLine())
	if err != nil {
		msg += fmt.Sprintf("\n  server error: %v", err)
	}
	return msg
}

// flagsFailure reports that the worker-facing heartbeat response flags disagreed with the model.
func (a *saaHandle) flagsFailure(e model.Event, src model.Status, observed, expected model.HeartbeatFlags) string {
	rows, agree := saaFlagRows(observed, expected)
	return fmt.Sprintf("%s: heartbeat response flags disagree\n%s\n%s",
		a.edge(e, src), a.pathLine(), saaDiffBlock(rows, agree))
}

// saaPathString renders the event sequence that reached an edge, e.g.
// "Schedule → Poll → RespondFailed[retryable=false]". The origin is labeled Schedule (the status the
// StartActivityExecution RPC lands in), to avoid confusion with Started.
func saaPathString(path []model.Event) string {
	parts := make([]string, 0, len(path)+1)
	parts = append(parts, "Schedule")
	for _, e := range path {
		parts = append(parts, model.EventLabel(e))
	}
	return strings.Join(parts, " → ")
}

// saaStateDiff renders the AbstractState fields that differ in aligned columns, with the agreeing
// fields listed as field=value beneath.
func saaStateDiff(observed, expected model.AbstractState) string {
	b2s := func(b bool) string { return fmt.Sprint(b) }
	fields := [][3]string{
		{"Status", observed.Status.String(), expected.Status.String()},
		{"Count", fmt.Sprint(observed.AttemptCount), fmt.Sprint(expected.AttemptCount)},
		{"ResetKeepPaused", b2s(observed.ResetKeepPaused), b2s(expected.ResetKeepPaused)},
		{"ResetHeartbeats", b2s(observed.ResetHeartbeats), b2s(expected.ResetHeartbeats)},
		{"ResetRestoreOptions", b2s(observed.ResetRestoreOptions), b2s(expected.ResetRestoreOptions)},
		{"FirstAttemptStarted", b2s(observed.FirstAttemptStarted), b2s(expected.FirstAttemptStarted)},
		{"DispatchTimeSet", b2s(observed.DispatchTimeSet), b2s(expected.DispatchTimeSet)},
	}
	return saaDiffBlock(saaSplit(fields))
}

func saaFlagRows(observed, expected model.HeartbeatFlags) (rows [][3]string, agree []string) {
	fields := [][3]string{
		{"CancelRequested", fmt.Sprint(observed.CancelRequested), fmt.Sprint(expected.CancelRequested)},
		{"ActivityPaused", fmt.Sprint(observed.ActivityPaused), fmt.Sprint(expected.ActivityPaused)},
		{"ActivityReset", fmt.Sprint(observed.ActivityReset), fmt.Sprint(expected.ActivityReset)},
	}
	return saaSplit(fields)
}

// saaSplit partitions (field, observed, expected) triples into the ones that differ (rows) and the
// ones that agree, the latter rendered as "field=value" so the report shows every field's value.
func saaSplit(fields [][3]string) (rows [][3]string, agree []string) {
	for _, f := range fields {
		if f[1] != f[2] {
			rows = append(rows, f)
		} else {
			agree = append(agree, f[0]+"="+f[1])
		}
	}
	return rows, agree
}

// saaDiffBlock formats differing (field, observed, expected) rows as an aligned three-column table
// and lists the agreeing "field=value" pairs beneath it.
func saaDiffBlock(rows [][3]string, agree []string) string {
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

func saaOutcomeDesc(k model.ErrorKind) string {
	if k == model.NoError {
		return "accepted"
	}
	return "rejected with " + saaRejectKindName(k)
}

func saaRejectKindName(k model.ErrorKind) string {
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
