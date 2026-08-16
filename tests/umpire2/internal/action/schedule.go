package action

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
)

// The coverage-guided fault scheduler turns the earlier pieces — AutoCoverPlans (the plan list),
// LearnFootprint (a plan's observed calls), and FaultTargets (the resilience targets) — into a
// novelty-prioritized, budget-bounded drive list. It is the deterministic upgrade of the
// uniform-random TestProbeNexusRandomized: rather than sampling, it schedules each distinct fault
// target once (breadth) before any repeat, so a tight budget buys the widest fault coverage, and it
// reports what the budget forced it to drop — never a silent truncation.

// PlanFootprint pairs a plan with the footprint learned by driving it once (LearnFootprint).
type PlanFootprint struct {
	Plan    []umpire.Action
	Label   string
	Learned []string
}

// FaultDrive is one scheduled drive: the plan with a transient Drop of Target prepended.
type FaultDrive struct {
	Plan   []umpire.Action
	Label  string
	Target string
}

// ScheduleFaults produces a coverage-guided, budget-bounded sequence of fault drives from a set of
// plans and their learned footprints. The coverage unit is the (plan, fault-target) pair. Novelty
// order: the first appearance of each distinct target across all plans comes first (breadth — cover
// every target once), then the remaining pairs (depth — the same target under a different plan).
// The first `budget` drives are returned; the rest are `dropped`, so a caller can log exactly what
// coverage the budget cost. Deterministic given the input order.
func ScheduleFaults(plans []PlanFootprint, budget int) (drives, dropped []FaultDrive) {
	type cand struct {
		pf     PlanFootprint
		target string
	}
	var cands []cand
	for _, pf := range plans {
		for _, tgt := range FaultTargets(pf.Plan, pf.Learned) {
			cands = append(cands, cand{pf, tgt})
		}
	}
	// Breadth first: the first occurrence of each distinct target, then the repeats.
	seen := map[string]bool{}
	var first, rest []cand
	for _, c := range cands {
		if seen[c.target] {
			rest = append(rest, c)
			continue
		}
		seen[c.target] = true
		first = append(first, c)
	}
	ordered := append(first, rest...)
	for i, c := range ordered {
		d := FaultDrive{
			Plan:   append([]umpire.Action{Drop(c.target)}, c.pf.Plan...),
			Label:  fmt.Sprintf("%s +drop:%s", c.pf.Label, c.target),
			Target: c.target,
		}
		if i < budget {
			drives = append(drives, d)
		} else {
			dropped = append(dropped, d)
		}
	}
	return drives, dropped
}
