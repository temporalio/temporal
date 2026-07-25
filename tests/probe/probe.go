// Package probe is a prototype of the single "Umpire" API: describe a state to
// reach, drive to it, break the underlying calls, and judge the result with the
// existing safety/liveness rulebook — with no hand-written faults or assertions.
// It closes the plan -> drive -> fault -> judge loop over the pieces already
// built (Planner, Driver, Monitor, and the gRPC fault seam). See UMPIRE_SPEC.md
// and UMPIRE_TRACING.md.
//
// Step 1 (this file): one *named* gRPC fault per scenario, to prove the loop and
// the API. Step 2 replaces the named method with the calls *observed* from a
// happy-path trace (a `FaultEachObservedCall`), so faults stop being hand-picked.
package probe

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire/model"
	"go.temporal.io/server/tests/umpire/planner"
)

// defaultMaxFaults bounds how many observed calls become fault scenarios, so a
// large footprint doesn't blow up wall-clock. Raise with MaxFaults.
const defaultMaxFaults = 12

// skipObserved are calls excluded from fault scenarios by default: long-polls and
// cluster-info RPCs are client-pull / ambient traffic, not part of a transition's
// server-side causal footprint. Dropping them only ever trivially stalls a poller.
// The exclusion is reported, never silent.
var skipObserved = []string{"Poll", "GetSystemInfo", "GetClusterInfo", "DescribeNamespace"}

// DriveFunc realizes "reach the target state" as real traffic. iter is a
// per-scenario counter so the caller can mint a unique workflow ID each run.
type DriveFunc func(ctx context.Context, iter int) error

// Probe is the fluent entry point. Build it with Umpire, describe a target with
// Reach, say how to realize it with Drive, arm faults, then Judge.
type Probe struct {
	t         *testing.T
	env       *testcore.TestEnv
	entity    string
	state     string
	routes    [][]string
	drive     DriveFunc
	faults    []string
	observe   bool
	maxFaults int
	dropN     int
	timeout   time.Duration
}

// Umpire starts a probe against a live cluster (via env) whose Monitor already
// observes and judges every namespace.
func Umpire(t *testing.T, env *testcore.TestEnv) *Probe {
	return &Probe{t: t, env: env, timeout: 30 * time.Second, maxFaults: defaultMaxFaults, dropN: 1}
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

// Drive registers how to realize the target as traffic against the SUT.
func (s *Probe) Drive(fn DriveFunc) *Probe { s.drive = fn; return s }

// InjectDropOn adds one scenario that drops every gRPC call whose full method
// equals name or ends in "/"+name (so "AddWorkflowTask" matches the matching
// service's method). Step 1 names these by hand; Step 2 derives them from a trace.
func (s *Probe) InjectDropOn(name string) *Probe { s.faults = append(s.faults, name); return s }

// Judge runs a baseline (no fault) then one scenario per armed fault, judging each
// with the Monitor's rulebook, and returns a Report. It leaves the namespace clean
// so the harness teardown check does not trip on an intentionally provoked violation.
func (s *Probe) Judge() Report {
	if s.drive == nil {
		s.t.Fatal("probe: Drive(...) is required before Judge()")
	}
	rep := Report{Entity: s.entity, State: s.state, Routes: s.routes}

	if s.observe {
		// The baseline drive doubles as the footprint capture: record every gRPC
		// call it makes, then fault each distinct one.
		rep.Observed, rep.Baseline = s.recordAndRun()
		s.faults = append(s.selectFaults(rep.Observed), s.faults...)
	} else {
		rep.Baseline = s.run(0, "", "baseline (no fault)")
	}

	for i, m := range s.faults {
		rep.Scenarios = append(rep.Scenarios, s.run(i+1, m, "drop "+shortName(m)))
	}
	s.env.GetMonitor().PurgeNamespace(s.env.NamespaceID().String())
	rep.log(s.t)
	return rep
}

// recordAndRun drives the happy path once with a recording callback, returning the
// sorted set of distinct gRPC methods it made (scoped to this namespace) and the
// baseline scenario.
func (s *Probe) recordAndRun() ([]string, Scenario) {
	nsID := s.env.NamespaceID().String()
	s.env.GetMonitor().PurgeNamespace(nsID)

	seen := map[string]struct{}{}
	var mu sync.Mutex
	cleanup := s.record(nsID, seen, &mu)

	// Use a standalone context, not env.Context(): the latter carries the whole
	// test's deadline, and a stranding drive would otherwise burn it down.
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	sc := Scenario{Label: "baseline + observe"}
	sc.DriveErr = s.drive(ctx, 0)
	cleanup()
	s.judge(nsID, &sc)

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

// record registers a no-op callback that records the full method of every gRPC
// call in this namespace, reusing the cluster's fault generator as the recorder.
func (s *Probe) record(nsID string, seen map[string]struct{}, mu *sync.Mutex) func() {
	gen := s.env.GetTestCluster().Host().GetFaultInjector()
	if gen == nil {
		s.t.Fatal("probe: fault injector is nil (build with -tags test_dep)")
	}
	nsName := s.env.Namespace().String()
	return gen.RegisterCallback(func(_ context.Context, fullMethod string, req, _ any, _ error) (bool, any, error) {
		if namespaceMatches(req, nsID, nsName) {
			mu.Lock()
			seen[fullMethod] = struct{}{}
			mu.Unlock()
		}
		return false, nil, nil // observe only, never fault
	})
}

func (s *Probe) run(iter int, method, label string) Scenario {
	nsID := s.env.NamespaceID().String()
	s.env.GetMonitor().PurgeNamespace(nsID) // fresh model per scenario

	sc := Scenario{Label: label, Method: method}
	if method != "" {
		cleanup := s.armDrop(nsID, method, &sc.Fired)
		defer cleanup()
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	sc.DriveErr = s.drive(ctx, iter)
	s.judge(nsID, &sc)
	return sc
}

// judge fills a scenario's verdict from the Monitor: any rulebook violation is a
// bug (Flagged); otherwise the target entity's terminal disposition decides —
// a Success terminal is Recovered, a Failure terminal is (acceptable) Degraded,
// and never settling is target-not-reached. All of this is read from the model;
// nothing about the outcome is hand-written.
func (s *Probe) judge(nsID string, sc *Scenario) {
	for _, v := range s.env.GetMonitor().CheckNamespace(context.Background(), nsID) {
		sc.Violations = append(sc.Violations, v.Rule)
	}
	sc.Terminal, sc.Disposition = s.inspectTarget(nsID)
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

// inspectTarget returns the target entity's terminal state and modeled disposition,
// or ("", Unset) if no entity of the target type settled in the namespace.
func (s *Probe) inspectTarget(nsID string) (string, umpire.Disposition) {
	nsRoot := umpire.NewEntityID(model.NamespaceType, nsID)
	for _, e := range s.env.GetMonitor().ModelState().QueryEntities(umpire.EntityType(s.entity), 0, &nsRoot) {
		lc, ok := e.Entity.(umpire.Lifecycled)
		if !ok {
			continue
		}
		if l := lc.Lifecycle(); l.IsTerminal() {
			return l.Current(), l.CurrentDisposition()
		}
	}
	return "", umpire.Unset
}

// armDrop registers a namespace-scoped fault that transiently fails the matching
// gRPC call (the first dropN occurrences), reusing the cluster's imported
// RPCFaultGenerator. Returns an unregister func.
func (s *Probe) armDrop(nsID, name string, fired *bool) func() {
	gen := s.env.GetTestCluster().Host().GetFaultInjector()
	if gen == nil {
		s.t.Fatal("probe: fault injector is nil (build with -tags test_dep)")
	}
	nsName := s.env.Namespace().String()
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
