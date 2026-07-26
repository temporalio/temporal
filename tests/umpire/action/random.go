package action

import (
	"fmt"
	"math/rand"

	umpire "go.temporal.io/server/common/testing/umpire"
)

// RandomPlan builds a reproducible random drive over the NexusOperation model. Given a seed it
// picks one drivable settling edge, routes to it via PlanEdge, and — with the same seed — may
// prepend a transient Drop of one call in the route's fault footprint. It returns the action
// sequence and a human label naming the sampled edge and fault, so a failing iteration replays
// from its seed alone (log the seed, re-run).
//
// The declared outcome is preserved under the strict Reconcile oracle: the fault is drawn only
// from the plan's own Faultable points, which are the internal/retryable calls (not the
// client-entry RPC), so the drop is transient and recoverable by construction — the operation
// still settles at its declared terminal. Route selection is Shortest for now; the free variables
// are which settling edge and which fault point, sampled across seeds. (Route randomization —
// PlanTo's Random mode — is a natural next axis once the model grows deeper paths.)
func RandomPlan(seed int64) ([]umpire.Action, string) {
	edges := settlingEdges()
	if len(edges) == 0 {
		return nil, "empty-model"
	}
	rng := rand.New(rand.NewSource(seed))
	e := edges[rng.Intn(len(edges))]
	seq, err := PlanEdge(e.from, e.event, e.hosting)
	if err != nil {
		// settlingEdges only returns drivable edges, so PlanEdge cannot fail here; guard anyway.
		return nil, fmt.Sprintf("unplannable %s--%s-->", e.from, e.event)
	}
	label := fmt.Sprintf("%s--%s-->settle (%s)", e.from, e.event, e.hosting)
	if fault, ok := maybeFault(rng, seq); ok {
		seq = append([]umpire.Action{fault}, seq...)
		label += " +" + fault.Name
	}
	return seq, label
}

// maybeFault returns a transient Drop of a randomly chosen call from the plan's fault footprint,
// or ok=false when the plan declares no faultable point or the roll leaves the run fault-free
// (~1 in 3, so both the clean and the perturbed path are sampled). Only the plan's own Faultable
// points are eligible — dropping them is recoverable, so the declared outcome (and the strict
// Reconcile oracle) still holds.
func maybeFault(rng *rand.Rand, plan []umpire.Action) (umpire.Action, bool) {
	var methods []string
	for _, a := range plan {
		methods = append(methods, a.Faultable...)
	}
	if len(methods) == 0 || rng.Intn(3) == 0 {
		return umpire.Action{}, false
	}
	return Drop(methods[rng.Intn(len(methods))]), true
}
