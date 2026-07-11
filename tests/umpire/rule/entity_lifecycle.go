package rule

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
)

// EntityTransitionLegality is a generic safety rule: no entity backed by a
// Lifecycle may observe an illegal (impossible / out-of-order) state transition.
// It subsumes the per-entity monotonicity rules — a stage regression is simply
// not a legal edge — and works for every Lifecycled entity type at once.
type EntityTransitionLegality struct{}

func (EntityTransitionLegality) Name() string { return "EntityTransitionLegalityRule" }

func (EntityTransitionLegality) CheckSafety(c *umpire.SafetyContext) {
	for r := range umpire.ChangedLifecycles(c) {
		lc := r.Entity.Lifecycle()
		illegal := lc.Illegal()
		c.Eval(r.Key, len(illegal) == 0, umpire.Violation{
			Message: fmt.Sprintf("illegal state transition observed: %+v", illegal),
			Tags: map[string]string{
				"state": lc.Current(),
			},
		})
	}
}

// EntityProgress is a generic liveness rule: an entity must not be left in a
// state its Lifecycle marks as "must progress" (LifecycleSpec.MustProgress).
// For WorkflowUpdate, whose must-progress states are {admitted, accepted}, it
// replaces both WorkflowUpdateLossPrevention (stuck admitted) and
// WorkflowUpdateCompletion (stuck accepted). Entities that declare no
// must-progress states are unaffected, so it is safe across all entity types.
type EntityProgress struct{}

func (EntityProgress) Name() string { return "EntityProgressRule" }

func (EntityProgress) CheckLiveness(c *umpire.LivenessContext) {
	for r := range umpire.ChangedLifecycles(c) {
		lc := r.Entity.Lifecycle()
		if !lc.MustProgress() {
			c.Resolve(r.Key)
			continue
		}
		c.Pending(r.Key, umpire.Violation{
			Message: fmt.Sprintf("entity did not progress out of state %q", lc.Current()),
			Tags: map[string]string{
				"entity": r.Key,
				"state":  lc.Current(),
			},
		})
	}
}
