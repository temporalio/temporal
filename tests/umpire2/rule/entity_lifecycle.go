package rule

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
)

// Illegal-transition conformance (no entity may observe an impossible / out-of-order
// state change) is no longer a rule here: it is surfaced generically by the framework's
// built-in conformance check over every Lifecycled entity (RuleRegistry.Check →
// checkConformance), reading the illegal transitions Lifecycle.Fire records at fire-time.

// EntityProgress is a generic liveness rule: an entity must not be left in a
// state its Lifecycle marks as "must progress" (LifecycleSpec.MustProgress).
// Entities that declare no must-progress states are unaffected, so it is safe
// across all entity types.
type EntityProgress struct{}

func (EntityProgress) Name() string { return "EntityProgressRule" }

func (EntityProgress) CheckLiveness(c *umpire.LivenessContext) {
	for r := range c.ChangedLifecycles() {
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
