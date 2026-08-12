// Package probe is a prototype of the single "Umpire" API: describe a state to
// reach, drive to it, break the underlying calls, and judge the result with the
// existing safety/liveness rulebook — with no hand-written faults or assertions.
// It closes the plan -> drive -> fault -> judge loop over the pieces already
// built (Planner, Driver, Monitor, and the gRPC fault seam). See UMPIRE_SPEC.md
// and UMPIRE_TRACING.md.
//
// Every execution (the baseline and each fault scenario) runs in its own fresh
// test environment — its own namespace — so scenarios are fully isolated. The
// faults themselves are learned from a happy-path trace (FaultEachObservedCall),
// not hand-picked.
package probe

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpirev1/model"
	"go.temporal.io/server/tests/umpirev1/planner"
)

// defaultMaxFaults bounds how many observed calls become fault scenarios, so a
// large footprint doesn't blow up wall-clock. Raise with MaxFaults.
const defaultMaxFaults = 12

// skipObserved are calls excluded from fault scenarios by default: long-polls and
// cluster-info RPCs are client-pull / ambient traffic, not part of a transition's
// server-side causal footprint. Dropping them only ever trivially stalls a poller.
// The exclusion is reported, never silent.
var skipObserved = []string{"Poll", "GetSystemInfo", "GetClusterInfo", "DescribeNamespace"}

// DriveFunc realizes "reach the target state" as real traffic within one execution's
// environment. iter is the per-scenario counter.
type DriveFunc func(ctx context.Context, iter int) error

// EnvFunc creates a fresh, isolated test environment (its own namespace) and the
// DriveFunc that realizes the target within it, for scenario iter. The probe calls
// it once per execution so every scenario is fully isolated.
type EnvFunc func(t *testing.T, iter int) (*testcore.TestEnv, DriveFunc)

// Probe is the fluent entry point. Build it with Umpire, describe a target with
// Reach, say how to create+drive each execution with Execution, arm faults, then Judge.
type Probe struct {
	t         *testing.T
	mk        EnvFunc
	entity    string
	state     string
	routes    [][]string
	faults    []string
	holds     []holdFault
	observe   bool
	maxFaults int
	dropN     int
	timeout   time.Duration
	cov       *Coverage
}

// holdFault delays a matching call by d before letting it through.
type holdFault struct {
	method string
	d      time.Duration
}

// fault arms a namespace-scoped fault for one execution and returns an unregister func.
type fault func(env *testcore.TestEnv, nsID string, fired *bool) func()

// Umpire starts a probe. The cluster's Monitor observes and judges every namespace;
// each execution gets its own namespace via Execution.
func Umpire(t *testing.T) *Probe {
	return &Probe{t: t, timeout: 30 * time.Second, maxFaults: defaultMaxFaults, dropN: 1, cov: NewCoverage()}
}

// Timeout bounds how long one drive may take before it is deemed not to have
// reached the target (a stranding fault will otherwise hang until the deadline).
func (s *Probe) Timeout(d time.Duration) *Probe { s.timeout = d; return s }

// Transient sets how many occurrences of a faulted call are failed before the
// call is allowed through (default 1). Faulting transiently exercises the
// retry/recovery path; setting it very high approximates a permanent outage.
func (s *Probe) Transient(n int) *Probe { s.dropN = n; return s }

// MaxFaults caps how many observed calls become fault scenarios (0 = uncapped).
func (s *Probe) MaxFaults(n int) *Probe { s.maxFaults = n; return s }

// FaultEachObservedCall records the gRPC calls that a happy-path drive makes to
// reach the target — the transition's observed footprint — and then adds one drop
// scenario per distinct call. This is the trace-derived heart of the tool: the
// faults are learned from observation, not named by hand.
func (s *Probe) FaultEachObservedCall() *Probe { s.observe = true; return s }

// Reach names the target state and validates — via the Planner, with no traffic —
// that it is reachable over the model. Unreachable ⇒ the probe fails fast here.
func (s *Probe) Reach(entity, state string) *Probe {
	s.entity, s.state = entity, state
	plan, err := planner.DefaultModels().PlanTo(entity, state, planner.Shortest, planner.Constraints{})
	if err != nil {
		s.t.Fatalf("probe: %s:%s is not reachable under the model: %v", entity, state, err)
	}
	s.routes = plan.Routes
	s.t.Logf("[probe] PLAN reach %s:%s via %v", entity, state, plan.Routes)
	return s
}

// Execution registers how to create a fresh environment and drive it, called once
// per scenario so every execution runs in its own namespace.
func (s *Probe) Execution(mk EnvFunc) *Probe { s.mk = mk; return s }

// InjectDropOn adds one scenario that drops every gRPC call whose full method
// equals name or ends in "/"+name (so "AddWorkflowTask" matches the matching
// service's method). Faults derived from a trace are added by FaultEachObservedCall.
func (s *Probe) InjectDropOn(name string) *Probe { s.faults = append(s.faults, name); return s }

// InjectHoldOn adds one scenario that delays (holds) the first matching call by d before
// letting it through — exercising latency- and ordering-sensitive paths (e.g. a slow
// dependency, or making one call land after another). Method matching is the same as
// InjectDropOn; the fault seam is shared, so it also matches the Nexus HTTP invocation
// (method "HTTP <METHOD> <path>"), not just gRPC.
func (s *Probe) InjectHoldOn(name string, d time.Duration) *Probe {
	s.holds = append(s.holds, holdFault{method: name, d: d})
	return s
}

// Judge runs a baseline (no fault) then one scenario per armed fault — each in its
// own environment — judging each with the Monitor's rulebook, and returns a Report.
func (s *Probe) Judge() Report {
	if s.mk == nil {
		s.t.Fatal("probe: Execution(...) is required before Judge()")
	}
	rep := Report{Entity: s.entity, State: s.state, Routes: s.routes}

	if s.observe {
		// The baseline drive doubles as the footprint capture: record every gRPC
		// call it makes, then fault each distinct one.
		rep.Observed, rep.Baseline = s.recordAndRun()
		s.faults = append(s.selectFaults(rep.Observed), s.faults...)
	} else {
		rep.Baseline = s.run(0, "", "baseline (no fault)", nil)
	}

	iter := 1
	for _, m := range s.faults {
		method := m
		rep.Scenarios = append(rep.Scenarios, s.run(iter, method, "drop "+shortName(method),
			func(env *testcore.TestEnv, nsID string, fired *bool) func() {
				return s.armDrop(env, nsID, method, fired)
			}))
		iter++
	}
	for _, h := range s.holds {
		hold := h
		rep.Scenarios = append(rep.Scenarios, s.run(iter, hold.method, "hold "+shortName(hold.method),
			func(env *testcore.TestEnv, nsID string, fired *bool) func() {
				return s.armHold(env, nsID, hold.method, hold.d, fired)
			}))
		iter++
	}
	if lc, ok := planner.DefaultModels().Lifecycle(s.entity); ok {
		rep.Coverage = s.cov.Report(s.entity, lc.Edges())
	}
	rep.log(s.t)
	return rep
}

// Coverage returns the probe's transition-coverage tracker, so several probe runs can
// accumulate into a shared summary. Set it before Judge to aggregate across runs.
func (s *Probe) Coverage() *Coverage { return s.cov }

// WithCoverage makes the probe accumulate into an existing coverage tracker (shared
// across runs) rather than its own.
func (s *Probe) WithCoverage(c *Coverage) *Probe { s.cov = c; return s }

// recordAndRun drives the happy path once in a fresh namespace with a recording
// callback, returning the sorted set of distinct gRPC methods it made and the baseline.
func (s *Probe) recordAndRun() ([]string, Scenario) {
	var sc Scenario
	seen := map[string]struct{}{}
	var mu sync.Mutex

	// Own subtest / own *testing.T, so the env teardown is scoped to this execution (see run).
	s.t.Run(uniqueSubtestName("baseline-observe"), func(t *testing.T) {
		env, drive := s.mk(t, 0)
		nsID := env.NamespaceID().String()
		cleanup := s.record(env, nsID, seen, &mu)

		// Standalone context, not env.Context(): the latter carries the whole test's
		// deadline, and a stranding drive would otherwise burn it down.
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel()
		sc = Scenario{Label: "baseline + observe"}
		sc.DriveErr = drive(ctx, 0)
		cleanup()
		s.judge(env, nsID, &sc)
		s.stopExecutions(env, nsID)
		env.GetMonitor().PurgeNamespace(nsID)
	})

	mu.Lock()
	methods := make([]string, 0, len(seen))
	for m := range seen {
		methods = append(methods, m)
	}
	mu.Unlock()
	sort.Strings(methods)
	return methods, sc
}

// selectFaults filters the observed footprint down to the calls worth faulting:
// drop the ambient/long-poll traffic (reported, never silent) and cap the count.
func (s *Probe) selectFaults(observed []string) []string {
	var kept, skipped []string
	for _, m := range observed {
		if matchesAny(m, skipObserved) {
			skipped = append(skipped, m)
			continue
		}
		kept = append(kept, m)
	}
	if len(skipped) > 0 {
		s.t.Logf("[probe] skipping %d ambient call(s) (not part of the transition footprint): %v", len(skipped), shortNames(skipped))
	}
	if s.maxFaults > 0 && len(kept) > s.maxFaults {
		s.t.Logf("[probe] observed %d faultable call(s); capping to %d (raise MaxFaults to cover all)", len(kept), s.maxFaults)
		kept = kept[:s.maxFaults]
	}
	return kept
}

func (s *Probe) run(iter int, method, label string, arm fault) Scenario {
	var sc Scenario
	// Each execution runs in its own subtest with its own *testing.T. The env it creates
	// registers its teardown (CheckAndPurgeMonitor) on that subtest's t, so the check runs
	// promptly when this execution ends — not stacked behind every other execution at the
	// end of the whole test (where a still-cycling op would have re-dirtied the namespace).
	s.t.Run(uniqueSubtestName(label), func(t *testing.T) {
		env, drive := s.mk(t, iter)
		nsID := env.NamespaceID().String()

		sc = Scenario{Label: label, Method: method}
		if arm != nil {
			cleanup := arm(env, nsID, &sc.Fired)
			defer cleanup()
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		defer cancel()
		sc.DriveErr = drive(ctx, iter)
		s.judge(env, nsID, &sc)
		// Stop the drivers, then purge, so this subtest's own teardown check stays clean:
		// an intentionally provoked never-settling op keeps cycling server-side and would
		// otherwise re-populate the namespace between the purge and the teardown re-check.
		s.stopExecutions(env, nsID)
		env.GetMonitor().PurgeNamespace(nsID)
	})
	return sc
}

// stopExecutions terminates every caller workflow the Monitor observed in this
// namespace, so an operation deliberately provoked never to settle (e.g. a
// retryable-forever Nexus handler) stops cycling server-side. Left running, the live
// operation keeps emitting chasm.transition telemetry that re-populates the namespace
// between PurgeNamespace and the subtest's teardown liveness check, which would then
// re-flag it. The workflow ID is read from the entity key (a Workflow entity created
// only as a parent placeholder for its operations never has its WorkflowID field
// populated by a fact). Errors are ignored: a workflow that already closed needs none.
func (s *Probe) stopExecutions(env *testcore.TestEnv, nsID string) {
	nsRoot := umpire.NewEntityID(model.NamespaceType, nsID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, e := range env.GetMonitor().ModelState().QueryEntities(model.WorkflowType, 0, &nsRoot) {
		wfID := workflowIDFromKey(e.Key)
		if wfID == "" {
			continue
		}
		_ = env.SdkClient().TerminateWorkflow(ctx, wfID, "", "umpire probe cleanup")
	}
}

// workflowIDFromKey extracts the workflow ID from a WorkflowType entity's registry key
// ("Namespace:<ns>@Workflow:<wfID>"): the leaf segment's ID.
func workflowIDFromKey(key string) string {
	leaf := key[strings.LastIndex(key, "@")+1:]
	return strings.TrimPrefix(leaf, string(model.WorkflowType)+":")
}

// execSeq makes subtest names unique across sibling probes under the same parent test,
// so the testing package never appends a "#NN" collision suffix (whose '#' is invalid in
// t.Name()-derived identifiers such as Nexus endpoint names).
var execSeq atomic.Int64

// uniqueSubtestName is a sanitized, globally-unique subtest name for one execution.
func uniqueSubtestName(label string) string {
	return fmt.Sprintf("%s-%d", subtestName(label), execSeq.Add(1))
}

// subtestName reduces a human label to a subtest name safe for t.Name()-derived
// identifiers (e.g. Nexus endpoint names must be alphanumeric-plus-hyphen): every run of
// non-alphanumeric characters collapses to a single hyphen.
func subtestName(label string) string {
	var b strings.Builder
	prevHyphen := false
	for _, r := range label {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
		} else if !prevHyphen {
			b.WriteByte('-')
			prevHyphen = true
		}
	}
	return strings.Trim(b.String(), "-")
}

// judge fills a scenario's verdict from the Monitor: any rulebook violation is a
// bug (Flagged); otherwise the target entity's terminal disposition decides —
// a Success terminal is Recovered, a Failure terminal is (acceptable) Degraded,
// and never settling is target-not-reached. All read from the model; nothing about
// the outcome is hand-written.
func (s *Probe) judge(env *testcore.TestEnv, nsID string, sc *Scenario) {
	for _, v := range env.GetMonitor().CheckNamespace(context.Background(), nsID) {
		sc.Violations = append(sc.Violations, v.Rule)
	}
	sc.Terminal, sc.Disposition = s.inspectTarget(env, nsID)
	switch {
	case len(sc.Violations) > 0:
		sc.Verdict = Flagged
	case sc.Terminal == "":
		sc.Verdict = Unreached
	case sc.Disposition == umpire.Failure:
		sc.Verdict = Degraded
	default: // Success or an untagged terminal
		sc.Verdict = Recovered
	}
}

// inspectTarget returns the target entity's terminal state and modeled disposition
// (or "", Unset if none settled), and records every target-entity's traversed edges
// into the coverage tracker — so a run accumulates which model transitions the real
// impl actually exercised.
func (s *Probe) inspectTarget(env *testcore.TestEnv, nsID string) (string, umpire.Disposition) {
	nsRoot := umpire.NewEntityID(model.NamespaceType, nsID)
	term, disp := "", umpire.Unset
	for _, e := range env.GetMonitor().ModelState().QueryEntities(umpire.EntityType(s.entity), 0, &nsRoot) {
		lc, ok := e.Entity.(umpire.Lifecycled)
		if !ok {
			continue
		}
		l := lc.Lifecycle()
		s.cov.Record(s.entity, l.VisitedEdges())
		if term == "" && l.IsTerminal() {
			term, disp = l.Current(), l.CurrentDisposition()
		}
	}
	return term, disp
}

// armDrop registers a namespace-scoped fault that transiently fails the matching
// gRPC call (the first dropN occurrences), reusing the cluster's imported
// RPCFaultGenerator. Returns an unregister func.
func (s *Probe) armDrop(env *testcore.TestEnv, nsID, name string, fired *bool) func() {
	gen := env.GetTestCluster().Host().GetFaultInjector()
	if gen == nil {
		s.t.Fatal("probe: fault injector is nil (build with -tags test_dep)")
	}
	nsName := env.Namespace().String()
	var seen atomic.Int32
	return gen.RegisterCallback(func(_ context.Context, fullMethod string, req, _ any, _ error) (bool, any, error) {
		if !methodMatches(fullMethod, name) || !namespaceMatches(req, nsID, nsName) {
			return false, nil, nil
		}
		// Transient: fail only the first dropN occurrences, then let the call
		// through, so the caller's retry/recovery path is exercised rather than
		// trivially proving a permanently-dead call never completes.
		if int(seen.Add(1)) > s.dropN {
			return false, nil, nil
		}
		*fired = true
		return true, nil, serviceerror.NewUnavailable("umpire probe: injected transient drop of " + shortName(name))
	})
}

// armHold registers a namespace-scoped fault that holds (blocks) the first dropN matching
// calls for d before letting them through, exercising latency- and ordering-sensitive
// paths. It reuses the same RPCFaultGenerator seam as armDrop, so it matches Nexus HTTP
// invocations as well as gRPC. The hold happens on the intercepted call's own goroutine
// (declining the match after sleeping), so the call proceeds normally once released.
func (s *Probe) armHold(env *testcore.TestEnv, nsID, name string, d time.Duration, fired *bool) func() {
	gen := env.GetTestCluster().Host().GetFaultInjector()
	if gen == nil {
		s.t.Fatal("probe: fault injector is nil (build with -tags test_dep)")
	}
	nsName := env.Namespace().String()
	var seen atomic.Int32
	return gen.RegisterCallback(func(_ context.Context, fullMethod string, req, _ any, _ error) (bool, any, error) {
		if !methodMatches(fullMethod, name) || !namespaceMatches(req, nsID, nsName) {
			return false, nil, nil
		}
		if int(seen.Add(1)) > s.dropN {
			return false, nil, nil
		}
		*fired = true
		time.Sleep(d)          //nolint:forbidigo // holding the call for d is the point of this fault
		return false, nil, nil // released: let the call proceed
	})
}

// record registers a no-op callback that records the full method of every gRPC
// call in this namespace, reusing the cluster's fault generator as the recorder.
func (s *Probe) record(env *testcore.TestEnv, nsID string, seen map[string]struct{}, mu *sync.Mutex) func() {
	gen := env.GetTestCluster().Host().GetFaultInjector()
	if gen == nil {
		s.t.Fatal("probe: fault injector is nil (build with -tags test_dep)")
	}
	nsName := env.Namespace().String()
	return gen.RegisterCallback(func(_ context.Context, fullMethod string, req, _ any, _ error) (bool, any, error) {
		if namespaceMatches(req, nsID, nsName) {
			mu.Lock()
			seen[fullMethod] = struct{}{}
			mu.Unlock()
		}
		return false, nil, nil // observe only, never fault
	})
}

func methodMatches(fullMethod, name string) bool {
	return fullMethod == name || strings.HasSuffix(fullMethod, "/"+name)
}

func matchesAny(fullMethod string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(fullMethod, sub) {
			return true
		}
	}
	return false
}

// shortName reduces a full gRPC method to "Service/Method" for readable output,
// keeping the service so that e.g. the frontend and history RespondWorkflowTaskCompleted
// calls stay distinguishable.
func shortName(fullMethod string) string {
	parts := strings.Split(strings.Trim(fullMethod, "/"), "/")
	method := parts[len(parts)-1]
	if len(parts) < 2 {
		return method
	}
	svc := parts[len(parts)-2]
	if i := strings.LastIndex(svc, "."); i >= 0 {
		svc = svc[i+1:] // ".../v1.WorkflowService" -> "WorkflowService"
	}
	return svc + "/" + method
}

func shortNames(methods []string) []string {
	out := make([]string, len(methods))
	for i, m := range methods {
		out[i] = shortName(m)
	}
	return out
}

// namespaceMatches keeps a fault scoped to this probe's namespace when the request
// exposes one; requests carrying no namespace field are left untouched.
func namespaceMatches(req any, nsID, nsName string) bool {
	if r, ok := req.(interface{ GetNamespaceId() string }); ok {
		return r.GetNamespaceId() == nsID
	}
	if r, ok := req.(interface{ GetNamespace() string }); ok {
		return r.GetNamespace() == nsName
	}
	return false
}
