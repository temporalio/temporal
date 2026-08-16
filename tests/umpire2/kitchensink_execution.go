package umpire2

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	ks "go.temporal.io/server/tests/umpire2/internal/kitchensink"
	ksworker "go.temporal.io/server/tests/umpire2/internal/kitchensink/worker"
	"google.golang.org/protobuf/types/known/emptypb"
)

// KitchenSinkEnvironment provides the client and worker used by the hidden kitchen-sink adapter.
type KitchenSinkEnvironment interface {
	SdkClient() client.Client
	SdkWorker() sdkworker.Worker
}

// KitchenSinkRunOptions configures how RunKitchenSinkPlan drives kitchensink workflows against a cluster.
type KitchenSinkRunOptions struct {
	Namespace      string
	TaskQueue      string
	WorkflowType   string // registered name of worker.KitchenSinkWorkflow on the worker
	WorkflowIDBase string // per-route workflow IDs derive from this

	// NexusEndpoint/NexusOperation are required for NexusOperation routes (the endpoint
	// is created per-test), ignored otherwise.
	NexusEndpoint  string
	NexusOperation string
}

// RunKitchenSinkPlan compiles each route in the plan into a kitchensink workload and drives it.
func RunKitchenSinkPlan(ctx context.Context, environment KitchenSinkEnvironment, options KitchenSinkRunOptions, entity string, plan *umpirefw.Plan) error {
	environment.SdkWorker().RegisterWorkflow(ksworker.KitchenSinkWorkflow)
	c := environment.SdkClient()
	for i, route := range plan.Routes {
		testInput, err := compileKitchenSinkRoute(entity, route, options.NexusEndpoint, options.NexusOperation)
		if err != nil {
			return fmt.Errorf("umpire2: compile kitchensink route %d: %w", i, err)
		}
		executor := &ks.ClientActionsExecutor{
			Client:        c,
			Namespace:     options.Namespace,
			WorkflowType:  options.WorkflowType,
			WorkflowInput: testInput.GetWorkflowInput(),
			WorkflowOptions: client.StartWorkflowOptions{
				ID:        fmt.Sprintf("%s-%d", options.WorkflowIDBase, i),
				TaskQueue: options.TaskQueue,
			},
		}
		if err := executor.Start(ctx, testInput.GetWithStartAction()); err != nil {
			return fmt.Errorf("umpire2: start kitchensink route %d: %w", i, err)
		}
		if sequence := testInput.GetClientSequence(); sequence != nil {
			if err := executor.ExecuteClientSequence(ctx, sequence); err != nil {
				return fmt.Errorf("umpire2: run kitchensink route %d: %w", i, err)
			}
		}
	}
	return nil
}

// holdKey is a workflow-state key the compiled program blocks on to keep a workflow
// running until the planned route says it should close.
const holdKey = "umpire_hold"

// compileKitchenSinkRoute turns an abstract model route into a kitchensink workload.
func compileKitchenSinkRoute(entity string, route []string, nexusEndpoint, nexusOperation string) (*ks.TestInput, error) {
	if len(route) == 0 {
		return nil, errors.New("empty route")
	}
	switch entity {
	case "Workflow":
		return compileKitchenSinkWorkflow(route)
	case "NexusOperation":
		return compileKitchenSinkNexus(route, nexusEndpoint, nexusOperation)
	case "WorkflowUpdate", "WorkflowTask":
		return nil, fmt.Errorf("route compiler for %q not implemented yet", entity)
	default:
		return nil, fmt.Errorf("no route compiler for entity %q", entity)
	}
}

func compileKitchenSinkWorkflow(route []string) (*ks.TestInput, error) {
	if route[0] != "start" {
		return nil, fmt.Errorf("workflow route must begin with \"start\", got %v", route)
	}
	var initial *ks.ActionSet
	switch final := route[len(route)-1]; final {
	case "complete":
		// Start, then immediately return → the workflow reaches started then completed.
		initial = ks.SingleActionSet(ks.NewEmptyReturnResultAction())
	case "start":
		// Start, then block on a state that is never set → the workflow stays started.
		initial = ks.SingleActionSet(ks.NewAwaitWorkflowStateAction(holdKey, "1"))
	default:
		return nil, fmt.Errorf("unsupported Workflow route terminal %q", final)
	}
	return &ks.TestInput{WorkflowInput: &ks.WorkflowInput{InitialActions: []*ks.ActionSet{initial}}}, nil
}

func compileKitchenSinkNexus(route []string, endpoint, operation string) (*ks.TestInput, error) {
	if route[0] != "schedule" {
		return nil, fmt.Errorf("nexus operation route must begin with \"schedule\", got %v", route)
	}
	if endpoint == "" {
		return nil, errors.New("nexus operation routes need an endpoint")
	}
	if operation == "" {
		operation = "operation"
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
		Endpoint: endpoint, Operation: operation, AwaitableChoice: choice,
	}}}
	// The workflow schedules the operation, awaits per the choice, then returns.
	return &ks.TestInput{WorkflowInput: &ks.WorkflowInput{InitialActions: []*ks.ActionSet{ks.SingleActionSet(nexusAction)}}}, nil
}
