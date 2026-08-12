package ksdriver

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	ks "go.temporal.io/server/tests/umpire2/kitchensink"
	"go.temporal.io/server/tests/umpire2/planner"
)

// RunOptions configures how RunPlan drives kitchensink workflows against a cluster.
type RunOptions struct {
	Namespace      string
	TaskQueue      string
	WorkflowType   string // registered name of worker.KitchenSinkWorkflow on the worker
	WorkflowIDBase string // per-route workflow IDs derive from this

	// NexusEndpoint/NexusOperation are required for NexusOperation routes (the endpoint
	// is created per-test), ignored otherwise.
	NexusEndpoint  string
	NexusOperation string
}

// RunPlan compiles each route in the plan for the given entity into a kitchensink
// TestInput and drives it against the cluster: it starts a kitchensink workflow with the
// compiled worker-side program and executes the compiled client sequence. The Monitor
// judges the resulting traffic out of band.
//
// This is the cluster-facing half — it needs a live server plus a worker with
// worker.KitchenSinkWorkflow registered under opts.WorkflowType. The pure mapping it
// relies on (Compile) is unit-tested independently.
func RunPlan(ctx context.Context, c client.Client, opts RunOptions, entity string, plan *planner.Plan) error {
	var compileOpts []Option
	if opts.NexusEndpoint != "" {
		compileOpts = append(compileOpts, WithNexus(opts.NexusEndpoint, opts.NexusOperation))
	}
	for i, route := range plan.Routes {
		ti, err := Compile(entity, route, compileOpts...)
		if err != nil {
			return fmt.Errorf("ksdriver: compile route %d: %w", i, err)
		}
		exec := &ks.ClientActionsExecutor{
			Client:        c,
			Namespace:     opts.Namespace,
			WorkflowType:  opts.WorkflowType,
			WorkflowInput: ti.GetWorkflowInput(),
			WorkflowOptions: client.StartWorkflowOptions{
				ID:        fmt.Sprintf("%s-%d", opts.WorkflowIDBase, i),
				TaskQueue: opts.TaskQueue,
			},
		}
		if err := exec.Start(ctx, ti.GetWithStartAction()); err != nil {
			return fmt.Errorf("ksdriver: start route %d: %w", i, err)
		}
		if seq := ti.GetClientSequence(); seq != nil {
			if err := exec.ExecuteClientSequence(ctx, seq); err != nil {
				return fmt.Errorf("ksdriver: run route %d: %w", i, err)
			}
		}
	}
	return nil
}
