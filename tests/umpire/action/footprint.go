package action

import (
	"fmt"
	"sort"

	umpire "go.temporal.io/server/common/testing/umpire"
)

// Footprint reconciliation is the wire-level analog of umpire.Reconcile (which grounds an action's
// declared Effects against the observed lifecycle edges). Where LearnFootprint discovers the calls
// a plan makes and FaultTargets reduces them to resilience targets, ReconcileFootprint grounds the
// calls an action *declares* it should make (Action.Footprint) against what was observed — so a
// refactor that adds or drops an internal call trips a drift the effect-level check cannot see.

// FootprintDrift is one wire-level divergence: an expected internal call the drive did not make, or
// an observed non-ambient call no action in the plan declared.
type FootprintDrift struct {
	Call   string
	Reason string
}

func (d FootprintDrift) String() string { return fmt.Sprintf("footprint %q: %s", d.Call, d.Reason) }

// ReconcileFootprint grounds a plan's declared footprint against the observed one. Every internal
// call an action declares (Action.Footprint) must appear in the observed footprint ("expected but
// not observed"), and no observed non-ambient call may fall outside the plan's declared surface
// (Entry ∪ Footprint) ("observed but not declared"). It is opt-in: a plan whose actions declare no
// Footprint returns nil, so footprint reconciliation is a deliberate regression gate on the
// transitions that pin their expected calls, not a blanket assertion on every drive.
func ReconcileFootprint(plan []umpire.Action, observed []string) []FootprintDrift {
	declared := planCalls(plan, func(a umpire.Action) []string { return a.Footprint })
	if len(declared) == 0 {
		return nil // opt-in: nothing declared, nothing to reconcile
	}
	surface := append(planCalls(plan, func(a umpire.Action) []string { return a.Entry }), declared...)

	var drift []FootprintDrift
	for _, c := range declared {
		if !observedHas(observed, c) {
			drift = append(drift, FootprintDrift{c, "expected but not observed"})
		}
	}
	for _, m := range observed {
		if matchesAny(m, ambientCalls) || matchesAnyMethod(m, surface) {
			continue
		}
		drift = append(drift, FootprintDrift{m, "observed but not declared"})
	}
	sort.Slice(drift, func(i, j int) bool { return drift[i].Call < drift[j].Call })
	return drift
}

// planCalls unions a per-action string slice (Entry or Footprint) across the plan.
func planCalls(plan []umpire.Action, pick func(umpire.Action) []string) []string {
	var out []string
	for _, a := range plan {
		out = append(out, pick(a)...)
	}
	return out
}

// observedHas reports whether any observed call matches name (exact, or gRPC-method suffix).
func observedHas(observed []string, name string) bool {
	for _, o := range observed {
		if methodMatches(o, name) {
			return true
		}
	}
	return false
}
