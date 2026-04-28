package nexusendpoint

import (
	"fmt"

	"testing"

	"go.temporal.io/server/tests/testcore/umpire"
)

var rules umpire.RuleSet

// nexus-endpoint-terminal-is-absorbing — once an endpoint is Deleted, it
// cannot transition back.
var _ = rules.Register(func(r *umpire.RuleBuilder) umpire.SafetyRule {
	terminalAbsorbing := r.CoveragePoint(
		"endpoint-terminal-absorbing",
		"endpoint stayed in its first terminal status",
	)

	return umpire.SafetyRule{
		Name: "nexus-endpoint-terminal-is-absorbing",
		Check: func(ctx *umpire.RuleContext, history []*umpire.Record) {
			firstTerminal := make(map[string]Status)
			for _, rec := range history {
				tr, ok := rec.Fact.(*umpire.Transition[Status])
				if !ok {
					continue
				}
				prev, seen := firstTerminal[tr.EntityID]
				if !seen {
					if IsTerminal(tr.To) {
						firstTerminal[tr.EntityID] = tr.To
						ctx.Check(terminalAbsorbing, tr.EntityID, true, "", nil)
					}
					continue
				}
				ctx.Check(
					terminalAbsorbing,
					tr.EntityID,
					false,
					fmt.Sprintf("endpoint %s transitioned out of terminal status %v -> %v", tr.EntityID, prev, tr.To),
					map[string]string{
						"endpoint": tr.EntityID,
						"from":     fmt.Sprintf("%v", prev),
						"to":       fmt.Sprintf("%v", tr.To),
					},
				)
			}
		},
	}
})

// nexus-endpoint-create-emits-fact — every endpoint transition should have a
// matching EntityCreated fact in history.
var _ = rules.Register(func(r *umpire.RuleBuilder) umpire.SafetyRule {
	createObserved := r.CoveragePoint(
		"endpoint-create-fact-emitted",
		"endpoint creation emitted an EntityCreated fact",
	)

	return umpire.SafetyRule{
		Name: "nexus-endpoint-create-emits-fact",
		Check: func(ctx *umpire.RuleContext, history []*umpire.Record) {
			created := make(map[string]bool)
			for _, rec := range history {
				ec, ok := rec.Fact.(*umpire.EntityCreated)
				if !ok || ec.Type != "nexus-endpoint" {
					continue
				}
				created[ec.EntityID] = true
			}
			for _, rec := range history {
				tr, ok := rec.Fact.(*umpire.Transition[Status])
				if !ok {
					continue
				}
				ctx.Check(
					createObserved,
					tr.EntityID,
					created[tr.EntityID],
					fmt.Sprintf("endpoint %s transitioned without prior EntityCreated", tr.EntityID),
					map[string]string{"endpoint": tr.EntityID},
				)
			}
		},
	}
})

// RunExamples runs every rule's example tests as parallel subtests. Use it
// from a top-level Go test to exercise the curated functional examples
// without bringing up the full property-test harness.
func RunExamples(t *testing.T) { rules.RunExamples(t) }
