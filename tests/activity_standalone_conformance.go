package tests

// Standalone activity's (SAA) half of the model-conformance machinery: how the surface observes an
// activity and what it can therefore check, plus the breadth-first graph traversal, which is SAA-only
// because it replays each path on a fresh activity. The shared engine — the per-edge checker and the
// random walk — is in activity_conformance.go; the configs and test entry points are in
// activity_standalone_conformance_test.go.
//
// SAA reads the CHASM activity component directly, so it checks the full AbstractState, the per-attempt
// and schedule-to-close task stamps, and the public Describe projection on top.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/await"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- the surface seam ------------------------------------------------------------------------

func (d *saaDriver) surfaceName() string { return "SAA" }
func (d *saaDriver) configIndex() int    { return d.cfgIdx }

func (d *saaDriver) startForConformance(t testing.TB) activityConformanceTarget {
	return d.start(t, d.cfg)
}

// candidateEvents is SAA's event alphabet: the worker RPCs and the operator commands, with a variant per
// outcome-affecting flag.
func (d *saaDriver) candidateEvents() []model.Event {
	var out []model.Event
	simple := []model.EventType{
		model.PollType, model.HeartbeatType, model.RespondCompletedType, model.RespondCanceledType, model.UpdateOptionsType,
	}
	for _, k := range simple {
		out = append(out, model.Event{Type: k})
	}
	// start_delay is mutable only within the StartDelayPending window, so the model rejects this in every
	// state the RPC-only traversal reaches.
	out = append(out, model.Event{Type: model.UpdateOptionsType, SetsStartDelay: true})
	for _, r := range []bool{false, true} {
		out = append(out, model.Event{Type: model.RespondFailedType, Failure: &model.Failure{NonRetryable: !r}})
	}
	for _, sr := range []bool{false, true} {
		out = append(out,
			model.Event{Type: model.PauseType, SameRequestID: sr},
			model.Event{Type: model.TerminateType, SameRequestID: sr},
			model.Event{Type: model.RequestCancelType, SameRequestID: sr},
		)
	}
	out = append(out, model.Event{Type: model.UnpauseType})
	for _, kp := range []bool{false, true} {
		for _, ro := range []bool{false, true} {
			out = append(out, model.Event{Type: model.ResetType, KeepPaused: kp, RestoreOriginal: ro})
		}
	}
	return out
}

// --- observation -----------------------------------------------------------------------------

// conformsTo compares the activity's internal component state with the state the model predicts.
func (a *saaHandle) conformsTo(t require.TestingT, expected model.AbstractState) (bool, string) {
	obs, err := a.observed()
	require.NoError(t, err)
	if expected.SameObserved(obs) {
		return true, ""
	}
	return false, saaStateDiff(obs, expected)
}

// awaitConformsTo polls the internal state until it matches expected, or the deadline passes.
func (a *saaHandle) awaitConformsTo(t testing.TB, expected model.AbstractState, deadline time.Time) {
	await.Require(a.testContext(), t, func(t *await.T) {
		obs, err := a.observedRaw()
		t.Require().NoError(err)
		t.Require().True(expected.SameObserved(obs))
	}, max(0, time.Until(deadline)), activityDriverPollInterval)
}

// checkFinalEdge adds the checks SAA can make beyond the internal state: the public Describe projection,
// and the task invalidations the model predicts, which show as stamp changes.
func (a *saaHandle) checkFinalEdge(t require.TestingT, e model.Event, cur model.AbstractState, out model.Outcome) {
	a.checkDescribe(t, out.Next)
	a.checkTaskInvalidation(t, e, cur, out)
}

func (a *saaHandle) heartbeatFlags() model.HeartbeatFlags {
	return model.HeartbeatFlags{
		CancelRequested: a.lastHeartbeat.GetCancelRequested(),
		ActivityPaused:  a.lastHeartbeat.GetActivityPaused(),
		ActivityReset:   a.lastHeartbeat.GetActivityReset(),
	}
}

func (a *saaHandle) nextAttemptScheduleTime(t require.TestingT) *timestamppb.Timestamp {
	return a.describe(t).GetInfo().GetNextAttemptScheduleTime()
}

// checkTaskInvalidation compares each raw stamp's change across the edge under test against the model's
// per-transition invalidation bools. observed() has already refreshed cur/prev for this edge.
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

// checkDescribe asserts that the status, run state, and attempt DescribeActivityExecution reports match
// model.ExpectedDescribe.
func (a *saaHandle) checkDescribe(t require.TestingT, expected model.AbstractState) {
	st, rs := model.ExpectedDescribe(expected)
	resp, err := a.d.env.FrontendClient().DescribeActivityExecution(a.testContext(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
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

// observed is the activity's internal state as the model's AbstractState. It shifts the raw stamps
// cur->prev, so a caller can compare the stamp change across the last edge.
func (a *saaHandle) observed() (model.AbstractState, error) {
	o, err := a.readObserved()
	if err != nil {
		return model.AbstractState{}, err
	}
	a.prevStamp, a.curStamp = a.curStamp, o.Stamp
	a.prevSTCStamp, a.curSTCStamp = a.curSTCStamp, o.ScheduleToCloseStamp
	return model.Abstract(o), nil
}

// observedRaw is observed without the stamp shift, for use in a polling loop.
func (a *saaHandle) observedRaw() (model.AbstractState, error) {
	o, err := a.readObserved()
	if err != nil {
		return model.AbstractState{}, err
	}
	return model.Abstract(o), nil
}

// chasmContext is the context ReadComponent needs to read internal component state, memoized.
func (d *saaDriver) chasmContext() (context.Context, error) {
	if d.chasmCtx == nil {
		ctx, err := d.env.GetTestCluster().Host().ChasmContext(d.testContext())
		if err != nil {
			return nil, err
		}
		d.chasmCtx = ctx
	}
	return d.chasmCtx, nil
}

// readObserved reads the activity's internal component state.
func (a *saaHandle) readObserved() (model.Observed, error) {
	chasmCtx, err := a.d.chasmContext()
	if err != nil {
		return model.Observed{}, err
	}
	ref := chasm.NewComponentRef[*activity.Activity](chasm.ExecutionKey{
		NamespaceID: a.d.env.NamespaceID().String(), BusinessID: a.activityID, RunID: a.runID,
	})
	return chasm.ReadComponent(chasmCtx, ref, func(act *activity.Activity, cctx chasm.Context, _ struct{}) (model.Observed, error) {
		attempt := act.LastAttempt.Get(cctx)
		return model.Observed{
			Status:               act.GetStatus(),
			Count:                attempt.GetCount(),
			Stamp:                attempt.GetStamp(),
			ScheduleToCloseStamp: act.GetScheduleToCloseStamp(),
			ResetKeepPaused:      act.GetResetShouldPause(),
			ResetRestoreOptions:  act.GetResetRestoreOptions(),
			FirstAttemptStarted:  act.GetFirstAttemptStartedTime() != nil,
			DispatchTimeSet:      attempt.GetDispatchTime() != nil,
		}, nil
	}, struct{}{})
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
	return activityDiffBlock(activitySplitFields(fields))
}

// --- the breadth-first graph traversal --------------------------------------------------------

// traverse does a breadth-first walk of the model's reachable states, verifying every decided
// edge against the server.
func (d *saaDriver) traverse(t *testing.T) {
	type node struct {
		path  []model.Event
		state model.AbstractState
	}
	start := model.Initial(d.cfg.modelConfig())
	visited := map[string]bool{model.Fingerprint(start): true}
	frontier := []node{{nil, start}}

	verifiedCells := map[saaCell]bool{}
	skippedCells := map[saaCell]bool{}
	// Fingerprint-granularity ledger, for the completeness check.
	verifiedFine := map[string]bool{}
	skippedFine := map[string]bool{}

	d.verifyPath(t, nil) // the freshly started activity matches Initial(cfg)

	edges, states := 0, 1
	maxDepth := model.MaxDepth()
	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		var next []node
		for _, nd := range frontier {
			for _, e := range d.candidateEvents() {
				out := model.Transition(d.cfg.modelConfig(), nd.state, e)
				edges++
				path := append(append([]model.Event{}, nd.path...), e)
				res, reached := d.verifyPath(t, path)
				if reached {
					c := saaCell{nd.state.Status, e.Type}
					key := model.CellKey(nd.state, e.Type)
					if res == activitySkippedNoToken {
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

	t.Logf("cfg %d: verified %d decided edges (%d distinct cells) across %d reachable states (depth<=%d)",
		d.cfgIdx, edges, len(verifiedCells), states, maxDepth)

	// The only decided edges the traversal cannot verify are worker RPCs on a path that never polled, so
	// holds no task token.
	if model.ReportCompleteness() {
		var unexercised []string
		for c := range skippedCells {
			if !verifiedCells[c] {
				unexercised = append(unexercised, fmt.Sprintf("%s/%s", c.status, c.eventType))
			}
		}
		slices.Sort(unexercised)
		if len(unexercised) > 0 {
			t.Logf("cfg %d: decided cells NOT exercised (worker RPC, no token on a never-polled path): %v",
				d.cfgIdx, unexercised)
		}
	}

	d.checkCompleteness(t, verifiedFine, skippedFine)
}

// checkCompleteness logs the cells the model can reach but this run did not — what the depth cap left
// out. Informational; it never fails.
func (d *saaDriver) checkCompleteness(t *testing.T, verifiedFine, skippedFine map[string]bool) {
	if !model.ReportCompleteness() {
		return
	}
	var gaps []string
	for key := range model.Reachable(d.cfg.modelConfig(), d.candidateEvents()) {
		if verifiedFine[key] || skippedFine[key] {
			continue
		}
		gaps = append(gaps, key)
	}
	if len(gaps) == 0 {
		return
	}
	slices.Sort(gaps)
	shown := gaps
	suffix := ""
	if len(shown) > 30 {
		shown, suffix = shown[:30], fmt.Sprintf("\n  … and %d more", len(gaps)-30)
	}
	t.Logf("cfg %d: %d model-reachable cell(s) not exercised at depth<=%d (raise TEMPORAL_SAASPEC_MAX_DEPTH to reach deeper).\n"+
		"  fingerprint = Status|count|resetKeepPaused|resetHeartbeats|resetRestoreOpts|firstStarted|dispatchSet|dispatch\n  %s%s",
		d.cfgIdx, len(gaps), model.MaxDepth(), strings.Join(shown, "\n  "), suffix)
}

// verifyPath starts a fresh activity, replays the path, and asserts only its final edge. A prefix edge
// that diverges aborts the replay silently; that edge is reported when it is the final edge of its own
// shorter path.
func (d *saaDriver) verifyPath(t testing.TB, path []model.Event) (activityApply, bool) {
	a := d.start(t, d.cfg)
	a.path = path
	cur := model.Initial(d.cfg.modelConfig())

	if ok, diff := a.conformsTo(t, cur); !ok {
		t.Errorf("cfg %d: state immediately after StartActivityExecution disagrees with Initial(cfg).\n%s",
			d.cfgIdx, diff)
		return activityMismatch, false
	}

	for i, e := range path {
		out := model.Transition(d.cfg.modelConfig(), cur, e)
		final := i == len(path)-1
		res := applyActivityEvent(t, a, e, cur, out, final)
		if final {
			return res, true // res concerns the final edge, which the ledger records
		}
		if res != activityVerified {
			return res, false // prefix diverged or was skipped; that edge is checked as its own path
		}
		cur = out.Next
	}
	return activityVerified, false // empty path: only the Initial check ran
}

// saaCell identifies a (source status, event type) pair for the coverage ledger.
type saaCell struct {
	status    model.Status
	eventType model.EventType
}

// --- model-conformance additions to the SAA driver -------------------------------------------

// driveTraceWithModelConformanceChecking drives a trace like driveTrace, additionally checking each
// step against model.Transition (see applyActivityEvent). The state after Start must equal
// model.Initial(cfg). Requires a config the model can see in full, so no customizeStart.
func (d *saaDriver) driveTraceWithModelConformanceChecking(t *testing.T, trace []model.Event) *saaHandle {
	a := d.start(t, d.cfg)
	a.path = trace
	cur := model.Initial(d.cfg.modelConfig())
	if ok, diff := a.conformsTo(t, cur); !ok {
		t.Fatalf("after Start, state disagrees with Initial(cfg).\n%s", diff)
	}
	for _, e := range trace {
		out := model.Transition(d.cfg.modelConfig(), cur, e)
		applyActivityEvent(t, a, e, cur, out, true)
		cur = out.Next
	}
	return a
}

// --- traces --------------------------------------------------------------------------------
//
// A trace is an event sequence run once on one fresh activity. Writing a timeout's *Elapses event into
// the sequence is what makes the driver configure that timeout short, so that it fires.

type saaTrace struct {
	trace        []model.Event
	cfg          activityConfig
	startDelayed bool // activity created with a start_delay; see startDelay for the window length
	// customizeStart mutates the StartActivityExecutionRequest before it is sent.
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

// config is the activity the trace implies: cfg, plus a short window for each timeout the trace fires
// so that it can, and the start delay when the trace needs one.
func (tr saaTrace) config() activityConfig {
	c := tr.cfg.forTrace(tr.trace)
	c.StartDelay = tr.startDelay()
	return c
}

// startDelay is the activity's start_delay: short when the trace fires StartDelayElapses, otherwise
// long enough to stay open for the whole trace. Zero when not start-delayed.
func (tr saaTrace) startDelay() time.Duration {
	if !tr.startDelayed {
		return 0
	}
	for _, e := range tr.trace {
		if e.Type == model.StartDelayElapsesType {
			return activityDelayWindow
		}
	}
	return activityLongDuration
}

// activityDelayWindow is a dispatch-delay window long enough to outlast a valid negative long poll, so that
// "not dispatchable yet" is observable within it.
const activityDelayWindow = 5 * time.Second
