package action

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	umpire "go.temporal.io/server/common/testing/umpire"
)

// Fault actions perturb a call in the plan's footprint rather than causing a transition: Drop
// fails the first matching call, Hold blocks it for a duration. Both arm the cluster's
// RPCFaultGenerator — the same seam the probe uses, covering gRPC and Nexus HTTP — scoped to
// the drive's namespace and transient (first occurrence only), and unregister on Ctx.Cleanup.
// Fault kind: installed up front (before the actions fire), no effect of their own.

func Drop(method string) umpire.Action {
	return umpire.Action{Name: "fault:Drop(" + method + ")", Kind: umpire.Fault, Realize: dropFault{method: method}}
}

func Hold(method string, d time.Duration) umpire.Action {
	return umpire.Action{Name: "fault:Hold(" + method + ")", Kind: umpire.Fault, Realize: holdFault{method: method, d: d}}
}

type dropFault struct{ method string }

func (f dropFault) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	return armFault(rc, f.method, 0)
}
func (dropFault) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

type holdFault struct {
	method string
	d      time.Duration
}

func (f holdFault) Install(rc umpire.RealizeContext, _ umpire.Action) error {
	return armFault(rc, f.method, f.d)
}
func (holdFault) Fire(context.Context, umpire.RealizeContext, umpire.Action) error { return nil }

// armFault registers a namespace-scoped, transient perturbation of the first matching call:
// hold > 0 blocks it for that long then lets it through; hold == 0 fails it once.
func armFault(rc umpire.RealizeContext, method string, hold time.Duration) error {
	c := rc.(*Ctx)
	gen := c.Env.GetTestCluster().Host().GetFaultInjector()
	if gen == nil {
		return fmt.Errorf("fault injector is nil (build with -tags test_dep)")
	}
	nsID := c.Env.NamespaceID().String()
	nsName := c.Env.Namespace().String()
	var seen atomic.Int32
	cleanup := gen.RegisterCallback(func(_ context.Context, fullMethod string, req, _ any, _ error) (bool, any, error) {
		if !methodMatches(fullMethod, method) || !namespaceMatches(req, nsID, nsName) {
			return false, nil, nil
		}
		if int(seen.Add(1)) > 1 {
			return false, nil, nil
		}
		if hold > 0 {
			time.Sleep(hold) //nolint:forbidigo // holding the call is the point of this fault
			return false, nil, nil
		}
		return true, nil, serviceerror.NewUnavailable("umpire action: injected drop of " + method)
	})
	c.addCleanup(cleanup)
	return nil
}

// ambientCalls are observed calls that are never resilience targets: long-polls and cluster
// metadata that happen during any drive but are not part of a transition's footprint. Mirrors the
// probe's skipObserved so the action model and the probe agree on what "the footprint" is.
var ambientCalls = []string{"Poll", "GetSystemInfo", "GetClusterInfo", "DescribeNamespace"}

// LearnFootprint drives plan once with an observe-only callback on the cluster's fault injector and
// returns the distinct RPC/HTTP calls it made in the drive's namespace, sorted. This is the
// *learned* footprint: the calls a plan actually makes, discovered rather than declared. The two
// statically declared points (Action.Entry) are only the client-entry RPCs; the internal calls a
// fault can meaningfully perturb are knowable only by observation — so fault targeting is sourced
// from here (via FaultTargets), not from static declarations. (UMPIRE_ACTIONS.md "What remains".)
func LearnFootprint(dctx context.Context, rc umpire.RealizeContext, oracle umpire.StateOracle,
	resolver umpire.EffectResolver, poll time.Duration, plan []umpire.Action) ([]string, error) {
	c := rc.(*Ctx)
	gen := c.Env.GetTestCluster().Host().GetFaultInjector()
	if gen == nil {
		return nil, fmt.Errorf("fault injector is nil (build with -tags test_dep)")
	}
	nsID, nsName := c.Env.NamespaceID().String(), c.Env.Namespace().String()
	var mu sync.Mutex
	seen := map[string]struct{}{}
	cleanup := gen.RegisterCallback(func(_ context.Context, fullMethod string, req, _ any, _ error) (bool, any, error) {
		if namespaceMatches(req, nsID, nsName) {
			mu.Lock()
			seen[fullMethod] = struct{}{}
			mu.Unlock()
		}
		return false, nil, nil // observe only, never fault
	})
	defer cleanup()
	if err := umpire.Drive(dctx, rc, oracle, resolver, poll, plan); err != nil {
		return nil, err
	}
	mu.Lock()
	defer mu.Unlock()
	methods := make([]string, 0, len(seen))
	for m := range seen {
		methods = append(methods, m)
	}
	sort.Strings(methods)
	return methods, nil
}

// FaultTargets reduces a learned footprint to the calls worth faulting: the observed calls minus
// the plan's own Entry calls (a Drop of an entry RPC just fails the drive) and ambient traffic. It
// is pure, so the reduction is unit-testable independent of a live drive.
func FaultTargets(plan []umpire.Action, learned []string) []string {
	var entry []string
	for _, a := range plan {
		entry = append(entry, a.Entry...)
	}
	var out []string
	for _, m := range learned {
		if matchesAny(m, ambientCalls) || matchesAnyMethod(m, entry) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// FaultVariants returns, for each learned resilience target of the plan, a copy of the plan with a
// transient Drop of that call prepended (installed before the drive). Driving a variant exercises
// the operation's resilience to that fault. The targets come from the *observed* footprint
// (LearnFootprint) reduced by FaultTargets — the internal/retryable calls, not the client-entry
// Entry RPCs whose drop just fails the drive.
func FaultVariants(plan []umpire.Action, learned []string) [][]umpire.Action {
	var variants [][]umpire.Action
	for _, m := range FaultTargets(plan, learned) {
		variants = append(variants, append([]umpire.Action{Drop(m)}, plan...))
	}
	return variants
}

// matchesAny reports whether fullMethod contains any of subs (substring match, for ambient traffic).
func matchesAny(fullMethod string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(fullMethod, sub) {
			return true
		}
	}
	return false
}

// matchesAnyMethod reports whether fullMethod is any of names by exact gRPC-method suffix match.
func matchesAnyMethod(fullMethod string, names []string) bool {
	for _, n := range names {
		if methodMatches(fullMethod, n) {
			return true
		}
	}
	return false
}

func methodMatches(fullMethod, name string) bool {
	return fullMethod == name || strings.HasSuffix(fullMethod, "/"+name)
}

// namespaceMatches keeps a fault scoped to the drive's namespace (works for gRPC request protos
// and the Nexus HTTP fault request, both of which expose the namespace).
func namespaceMatches(req any, nsID, nsName string) bool {
	if r, ok := req.(interface{ GetNamespaceId() string }); ok {
		return r.GetNamespaceId() == nsID
	}
	if r, ok := req.(interface{ GetNamespace() string }); ok {
		return r.GetNamespace() == nsName
	}
	return false
}
