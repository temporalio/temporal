// Package ksdriver bridges the planner (abstract routes over entity models) to the copied
// Omes kitchensink workload: Compile turns a planner route into a kitchensink TestInput
// (the worker-side program the interpreter runs, plus the client-side RPCs), and RunPlan
// drives it against a live cluster. The Monitor judges the resulting traffic out of band.
package ksdriver

import (
	"fmt"

	ks "go.temporal.io/server/tests/umpire/kitchensink"
)

// holdKey is a workflow-state key the compiled program blocks on to keep a workflow
// running until the planned route says it should close.
const holdKey = "umpire_hold"

// Compile turns a planner route (a sequence of abstract model events for one entity) into
// a kitchensink TestInput that realizes exactly those transitions. It is pure — no client,
// no cluster — so the route→workload mapping is unit-testable on its own.
func Compile(entity string, route []string) (*ks.TestInput, error) {
	if len(route) == 0 {
		return nil, fmt.Errorf("ksdriver: empty route")
	}
	switch entity {
	case "Workflow":
		return compileWorkflow(route)
	case "WorkflowUpdate", "WorkflowTask", "NexusOperation":
		// These compile to update/signal/activity/nexus programs (the interesting
		// AwaitableChoice-driven lifecycles); deferred until the entity models settle.
		return nil, fmt.Errorf("ksdriver: route compiler for %q not implemented yet (Workflow only in this slice)", entity)
	default:
		return nil, fmt.Errorf("ksdriver: no route compiler for entity %q", entity)
	}
}

func compileWorkflow(route []string) (*ks.TestInput, error) {
	if route[0] != "start" {
		return nil, fmt.Errorf("ksdriver: Workflow route must begin with \"start\", got %v", route)
	}
	final := route[len(route)-1]
	var initial *ks.ActionSet
	switch final {
	case "complete":
		// Start, then immediately return → the workflow reaches started then completed.
		initial = ks.SingleActionSet(ks.NewEmptyReturnResultAction())
	case "start":
		// Start, then block on a state that is never set → the workflow stays started.
		initial = ks.SingleActionSet(ks.NewAwaitWorkflowStateAction(holdKey, "1"))
	default:
		return nil, fmt.Errorf("ksdriver: unsupported Workflow route terminal %q", final)
	}
	return &ks.TestInput{
		WorkflowInput: &ks.WorkflowInput{InitialActions: []*ks.ActionSet{initial}},
	}, nil
}
