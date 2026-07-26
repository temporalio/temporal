package action

import (
	"context"
	"fmt"
	"strings"
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

// FaultVariants returns, for each faultable point across the plan's actions, a copy of the plan
// with a transient Drop of that point prepended (installed before the drive). Driving a variant
// exercises the operation's resilience to that fault. Note: dropping a client-entry RPC (the
// action's own call) fails the drive rather than testing resilience; the meaningful points are
// the internal/retryable calls in an action's footprint.
func FaultVariants(plan []umpire.Action) [][]umpire.Action {
	var variants [][]umpire.Action
	for _, a := range plan {
		for _, m := range a.Faultable {
			variants = append(variants, append([]umpire.Action{Drop(m)}, plan...))
		}
	}
	return variants
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
