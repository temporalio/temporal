package action

import (
	"fmt"
	"math/rand"

	"go.temporal.io/server/common/testing/umpire"
)

// RandomPlan builds a reproducible random drive over the NexusOperation model: given a seed it
// picks one drivable settling edge and routes to it via PlanEdge, returning the action sequence
// and a human label naming the sampled edge. A failing iteration replays from its logged seed
// alone (log the seed, re-run RandomPlan(seed)).
//
// It intentionally carries no fault. Meaningful resilience faults attach to a plan's *learned*
// footprint (the calls the drive actually makes, captured at runtime; see LearnFootprint), not to
// statically declared points — the two declared Entry calls are client-entry RPCs whose drop just
// fails the drive. The randomized loop therefore samples the settling edge here and lets the probe
// fault each observed call of the resulting footprint. Route selection is Shortest for now; the free
// variable is which settling edge, sampled across seeds. (Route randomization — PlanTo's Random
// mode — is a natural next axis once the model grows deeper paths.)
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
	return seq, fmt.Sprintf("%s--%s-->settle (%s)", e.from, e.event, e.hosting)
}
