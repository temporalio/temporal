package taskqueue

import (
	"testing"

	"go.temporal.io/server/tests/testcore/umpire"
)

// rules is a placeholder. Task queues have no observable state-machine
// transitions in the model (registration is implicit; there is no public
// delete API). Cross-entity rules that involve task queues live in the
// component that owns the relationship.
var rules umpire.RuleSet

// RunExamples runs every rule's example tests as parallel subtests. Use it
// from a top-level Go test to exercise the curated functional examples
// without bringing up the full property-test harness.
func RunExamples(t *testing.T) { rules.RunExamples(t) }
