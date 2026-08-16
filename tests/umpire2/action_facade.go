package umpire2

import (
	"context"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2/internal/action"
)

// ActionEnvironment is the live Temporal capability set used by action execution.
type ActionEnvironment = action.Environment

// RegressionEnvironment is the capability set used by sparse regression execution.
type RegressionEnvironment = action.RegressionEnvironment

// RegressionEnvironmentFactory creates one isolated sparse-regression environment.
type RegressionEnvironmentFactory = action.RegressionEnvironmentFactory

// PlanFootprint pairs a plan with the calls learned by executing it.
type PlanFootprint = action.PlanFootprint

// FaultDrive is one coverage-scheduled fault execution.
type FaultDrive = action.FaultDrive

// FootprintDrift is one difference between declared and observed calls.
type FootprintDrift = action.FootprintDrift

// ActionRunner owns the Temporal response policy and generic drive collaborators.
type ActionRunner struct {
	environment ActionEnvironment
	policy      *action.ResponsePolicy
}

// ActionRunResult exposes only semantic results retained after one execution.
type ActionRunResult struct {
	context *action.Ctx
	oracle  action.Oracle
	plan    []umpire.Action
	drift   []umpire.Drift
}

// NewActionRunner creates a runner for one isolated Temporal environment.
func NewActionRunner(environment ActionEnvironment) *ActionRunner {
	return &ActionRunner{environment: environment, policy: action.NewResponsePolicy()}
}

// Handler returns the programmable Nexus handler owned by the runner.
func (r *ActionRunner) Handler() nexustest.Handler {
	return r.policy.Handler()
}

// Run executes and reconciles a plan against observed Temporal state.
func (r *ActionRunner) Run(ctx context.Context, endpoint string, iteration int, plan []umpire.Action) (*ActionRunResult, error) {
	realization := action.NewCtx(r.environment, endpoint, r.policy, iteration)
	defer realization.Cleanup()
	oracle := action.Oracle{Env: r.environment}
	result := &ActionRunResult{context: realization, oracle: oracle, plan: slices.Clone(plan)}
	if err := umpire.Drive(ctx, realization, oracle, action.Resolver{}, 50*time.Millisecond, plan); err != nil {
		return result, err
	}
	result.drift = umpire.Reconcile(oracle, realization, plan)
	if len(result.drift) != 0 {
		return result, fmt.Errorf("actions model drift: %v", result.drift)
	}
	return result, nil
}

// LearnFootprint executes a plan while collecting its distinct RPC and HTTP calls.
func (r *ActionRunner) LearnFootprint(ctx context.Context, endpoint string, iteration int, plan []umpire.Action) ([]string, error) {
	realization := action.NewCtx(r.environment, endpoint, r.policy, iteration)
	defer realization.Cleanup()
	return action.LearnFootprint(ctx, realization, action.Oracle{Env: r.environment}, action.Resolver{}, 50*time.Millisecond, plan)
}

// Binding returns an identity grounded by the completed run.
func (r *ActionRunResult) Binding(name string) (string, bool) {
	if r == nil {
		return "", false
	}
	return r.context.Binding(name)
}

// Current returns the observed current state of an entity.
func (r *ActionRunResult) Current(entityType umpire.EntityType, id string) (string, bool) {
	if r == nil {
		return "", false
	}
	return r.oracle.Current(entityType, id)
}

// Plan returns the completed action plan supplied to the runner.
func (r *ActionRunResult) Plan() []umpire.Action {
	if r == nil {
		return nil
	}
	return slices.Clone(r.plan)
}

// Drift returns every declared effect not grounded by observed state.
func (r *ActionRunResult) Drift() []umpire.Drift {
	if r == nil {
		return nil
	}
	return slices.Clone(r.drift)
}

// CountEntities returns the number of observed entities of one type.
func CountEntities(environment ActionEnvironment, entityType umpire.EntityType) int {
	return action.CountEntities(environment, entityType)
}

func StartFieldVariants() []umpire.Action                      { return action.StartFieldVariants() }
func Hold(method string, duration time.Duration) umpire.Action { return action.Hold(method, duration) }
func StandaloneCompletion() []umpire.Action {
	return mustDefaultEdgePlan(NexusOperationType, NexusStarted, NexusSucceed, umpire.Standalone)
}
func StandaloneTerminate(state string) []umpire.Action {
	if state == NexusScheduled {
		return action.StandaloneTerminate(state)
	}
	return mustDefaultEdgePlan(NexusOperationType, state, NexusTerminate, umpire.Standalone)
}
func EmbeddedSyncSuccess() []umpire.Action {
	return mustDefaultEdgePlan(NexusOperationType, NexusScheduled, NexusSucceed, umpire.Embedded)
}
func EmbeddedOpFailure() []umpire.Action {
	return mustDefaultEdgePlan(NexusOperationType, NexusScheduled, NexusFail, umpire.Embedded)
}
func WorkflowRunPlan() []umpire.Action           { return action.WorkflowRunPlan() }
func WorkflowContinueAsNewPlan() []umpire.Action { return action.WorkflowContinueAsNewPlan() }
func AutoCoverPlans() [][]umpire.Action {
	compiled, err := DefaultProtocol()
	if err != nil {
		panic(err)
	}
	planned, err := compiled.planSettlingEdges(NexusOperationType, umpire.Embedded)
	if err != nil {
		panic(err)
	}
	result := make([][]umpire.Action, len(planned.Plans))
	for index, plan := range planned.Plans {
		result[index] = plan.Actions
	}
	return result
}
func RandomPlan(seed int64) ([]umpire.Action, string) {
	compiled, err := DefaultProtocol()
	if err != nil {
		return nil, "invalid-protocol"
	}
	plan, err := compiled.sampleSettlingPlan(NexusOperationType, umpire.Embedded, seed)
	if err != nil {
		return nil, "empty-model"
	}
	return plan.Actions, fmt.Sprintf("%s--%s-->settle (%s)", plan.From, plan.Event, plan.Hosting)
}
func FaultTargets(plan []umpire.Action, learned []string) []string {
	return action.FaultTargets(plan, learned)
}
func ReconcileFootprint(plan []umpire.Action, observed []string) []FootprintDrift {
	return action.ReconcileFootprint(plan, observed)
}
func ScheduleFaults(plans []PlanFootprint, budget int) (scheduled, skipped []FaultDrive) {
	return action.ScheduleFaults(plans, budget)
}
func ValidateKitchensinkMappings() error { return action.ValidateKitchensinkMappings() }
func ValidateMutationCoverage() error    { return action.ValidateMutationCoverage() }

func mustDefaultEdgePlan(entityType umpire.EntityType, from, event string, hosting umpire.Hosting) []umpire.Action {
	compiled, err := DefaultProtocol()
	if err != nil {
		panic(err)
	}
	plan, err := compiled.PlanEdge(entityType, from, event, hosting)
	if err != nil {
		panic(err)
	}
	return plan
}

// StartUnknownEndpoint is the canonical rejected-start action.
var StartUnknownEndpoint = action.StartUnknownEndpoint

// NewRegressionHarness creates the Temporal sparse-regression executor.
func NewRegressionHarness(factory RegressionEnvironmentFactory, sink coreregress.ArtifactSink) coreregress.Harness {
	return action.NewRegressionHarness(factory, sink)
}
