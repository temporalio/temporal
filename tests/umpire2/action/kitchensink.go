package action

import (
	"context"
	"fmt"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	umpire "go.temporal.io/server/common/testing/umpire"
	ks "go.temporal.io/server/tests/umpire2/kitchensink"
	ksworker "go.temporal.io/server/tests/umpire2/kitchensink/worker"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/planner"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// The worker layer: WorkerCommand actions are realized by the *real* Omes kitchensink
// interpreter (the SDK-level program a user's workflow would run), not a bespoke test
// workflow. The mapping below is the single source of truth — a WorkerCommand action's name
// maps to the kitchensink action a worker issues for it — and the worker program is derived
// from it. ValidateKitchensinkMappings checks the mapping is exhaustive before any test runs,
// so a new WorkerCommand action can't be silently undrivable. See UMPIRE.md.

const ksWorkflowType = "kitchenSink" // the registered name of ksworker.KitchenSinkWorkflow

// ksActionFor maps a WorkerCommand action to the kitchensink action that realizes it, or false
// if unmapped. This is the derivation table.
func ksActionFor(actionName, endpoint, operation string) (*ks.Action, bool) {
	switch actionName {
	case ScheduleEmbedded.Name: // "cmd:ScheduleNexusOperation"
		return &ks.Action{Variant: &ks.Action_NexusOperation{NexusOperation: &ks.ExecuteNexusOperation{
			Endpoint:        endpoint,
			Operation:       operation,
			AwaitableChoice: &ks.AwaitableChoice{Condition: &ks.AwaitableChoice_WaitFinish{WaitFinish: &emptypb.Empty{}}},
		}}}, true
	}
	return nil, false
}

// kitchensink realizes a WorkerCommand action by starting the real kitchensink workflow with
// the derived program — the operation is a child of that workflow (embedded hosting). Non-
// blocking: later actions (handler, callback) drive the operation's outcome.
type kitchensink struct{}

func (kitchensink) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (kitchensink) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	act, ok := ksActionFor(a.Name, c.Endpoint, "operation")
	if !ok {
		return fmt.Errorf("kitchensink: no mapping for %s (run ValidateKitchensinkMappings)", a.Name)
	}
	wfID := fmt.Sprintf("umpire-action-caller-%d", c.Iter)
	input := &ks.WorkflowInput{InitialActions: []*ks.ActionSet{ks.SingleActionSet(act)}}
	c.Env.SdkWorker().RegisterWorkflowWithOptions(ksworker.KitchenSinkWorkflow, workflow.RegisterOptions{Name: ksWorkflowType})
	if _, err := c.Env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: c.Env.WorkerTaskQueue(),
	}, ksWorkflowType, input); err != nil {
		return err
	}
	bindFresh(rc, a, wfID) // the embedded op is keyed by its caller workflow id
	return nil
}

// ValidateKitchensinkMappings checks that every WorkerCommand action the registry can produce
// has a kitchensink mapping. Call it before running any drives (a unit test and the exploration
// both gate on it) so a WorkerCommand action added without its mapping fails fast, not mid-run.
func ValidateKitchensinkMappings() error {
	lc, ok := planner.DefaultModels().Lifecycle(string(model.NexusOperationType))
	if !ok {
		return fmt.Errorf("kitchensink: no NexusOperation lifecycle")
	}
	seen := map[string]bool{}
	var missing []string
	for _, e := range lc.Edges() {
		a, ok := actionFor(e.From, e.Event, umpire.Embedded)
		if !ok || a.Kind != umpire.WorkerCommand || seen[a.Name] {
			continue
		}
		seen[a.Name] = true
		if _, mapped := ksActionFor(a.Name, "endpoint", "operation"); !mapped {
			missing = append(missing, a.Name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("kitchensink mapping incomplete for WorkerCommand actions: %v", missing)
	}
	return nil
}
