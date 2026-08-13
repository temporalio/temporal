// Package ksdriver bridges the planner (abstract routes over entity models) to the copied
// Omes kitchensink workload: Compile turns a planner route into a kitchensink TestInput
// (the worker-side program the interpreter runs, plus the client-side RPCs), and RunPlan
// drives it against a live cluster. The Monitor judges the resulting traffic out of band.
package ksdriver

import (
	"fmt"

	ks "go.temporal.io/server/tests/umpire1/kitchensink"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// holdKey is a workflow-state key the compiled program blocks on to keep a workflow
// running until the planned route says it should close.
const holdKey = "umpire_hold"

// config holds compile-time inputs a route needs but can't carry itself (e.g. a Nexus
// endpoint, which is created per-test). Set via Option.
type config struct {
	nexusEndpoint  string
	nexusOperation string
}

// Option configures Compile for entities that need out-of-band inputs.
type Option func(*config)

// WithNexus supplies the endpoint (and operation name) a NexusOperation route schedules
// against. Required for the NexusOperation entity.
func WithNexus(endpoint, operation string) Option {
	return func(c *config) { c.nexusEndpoint, c.nexusOperation = endpoint, operation }
}

// Compile turns a planner route (a sequence of abstract model events for one entity) into
// a kitchensink TestInput that realizes exactly those transitions. It is pure — no client,
// no cluster — so the route→workload mapping is unit-testable on its own.
func Compile(entity string, route []string, opts ...Option) (*ks.TestInput, error) {
	if len(route) == 0 {
		return nil, fmt.Errorf("ksdriver: empty route")
	}
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	switch entity {
	case "Workflow":
		return compileWorkflow(route)
	case "NexusOperation":
		return compileNexus(route, cfg)
	case "WorkflowUpdate", "WorkflowTask":
		return nil, fmt.Errorf("ksdriver: route compiler for %q not implemented yet", entity)
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

func compileNexus(route []string, cfg config) (*ks.TestInput, error) {
	if route[0] != "schedule" {
		return nil, fmt.Errorf("ksdriver: NexusOperation route must begin with \"schedule\", got %v", route)
	}
	if cfg.nexusEndpoint == "" {
		return nil, fmt.Errorf("ksdriver: NexusOperation routes need WithNexus(endpoint, operation)")
	}
	op := cfg.nexusOperation
	if op == "" {
		op = "operation"
	}
	// A route stopping at "start" only wants the op to reach STARTED; any other terminal
	// (succeed/fail/cancel/timeout) is realized by the handler, so we await completion.
	var choice *ks.AwaitableChoice
	if route[len(route)-1] == "start" {
		choice = &ks.AwaitableChoice{Condition: &ks.AwaitableChoice_WaitStarted{WaitStarted: &emptypb.Empty{}}}
	} else {
		choice = &ks.AwaitableChoice{Condition: &ks.AwaitableChoice_WaitFinish{WaitFinish: &emptypb.Empty{}}}
	}
	nexusAction := &ks.Action{Variant: &ks.Action_NexusOperation{NexusOperation: &ks.ExecuteNexusOperation{
		Endpoint:        cfg.nexusEndpoint,
		Operation:       op,
		AwaitableChoice: choice,
	}}}
	// The workflow schedules the operation, awaits per the choice, then returns.
	return &ks.TestInput{
		WorkflowInput: &ks.WorkflowInput{InitialActions: []*ks.ActionSet{ks.SingleActionSet(nexusAction)}},
	}, nil
}
