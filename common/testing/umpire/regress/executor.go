package regress

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Bindings contains concrete identities and values grounded while a path executes.
type Bindings map[string]any

// Cleanup reverses one installed resource, action, or policy.
type Cleanup func(context.Context) error

// Harness creates an isolated live environment for every compiled path.
type Harness interface {
	NewPath(context.Context, int, CompletedPath) (PathHarness, error)
}

// PathHarness realizes only capabilities already selected and validated by Compile.
type PathHarness interface {
	SetupResource(context.Context, CompletedResource) (Cleanup, error)
	InstallAction(context.Context, CompletedStep, Bindings) (Cleanup, error)
	ArmPolicy(context.Context, CompletedPolicy, Bindings) (Cleanup, error)
	Await(context.Context, []CompletedAtom, Bindings) error
	Fire(context.Context, CompletedStep, Bindings) error
	Reconcile(context.Context, CompletedStep, Bindings) error
	Observe(context.Context, CompletedMilestone, Bindings) error
	CheckSafety(context.Context, Checkpoint) error
	Quiesce(context.Context) error
	ResolveLiveness(context.Context) error
}

type closablePath interface {
	Close(context.Context) error
}

// Checkpoint identifies when the global Monitor safety rulebook is evaluated.
type Checkpoint uint8

const (
	ActionCheckpoint Checkpoint = iota
	ObservationCheckpoint
	QuiescenceCheckpoint
)

func (c Checkpoint) String() string {
	switch c {
	case ActionCheckpoint:
		return "action"
	case ObservationCheckpoint:
		return "observation"
	case QuiescenceCheckpoint:
		return "quiescence"
	default:
		return "unknown"
	}
}

type activeCleanup struct {
	name    string
	cleanup Cleanup
	active  bool
}

// Run executes every compiled path in a fresh harness environment.
func Run(ctx context.Context, suite Suite, harness Harness) error {
	return RunWithOptions(ctx, suite, harness, RunOptions{MaxParallel: 1})
}

// RunOptions bounds isolated path execution without changing suite semantics.
type RunOptions struct {
	MaxParallel int
}

// RunWithOptions executes every path, using at most MaxParallel isolated environments.
func RunWithOptions(ctx context.Context, suite Suite, harness Harness, options RunOptions) error {
	if harness == nil {
		return errors.New("regress run: harness is nil")
	}
	if options.MaxParallel < 1 {
		return errors.New("regress run: MaxParallel must be positive")
	}
	if err := ValidateSuite(suite); err != nil {
		return fmt.Errorf("regress run: invalid completed suite: %w", err)
	}
	var sink ArtifactSink
	if provider, ok := harness.(ArtifactHarness); ok {
		sink = provider.ArtifactSink()
	}
	recorder := newArtifactRecorder(suite, sink)
	if err := recorder.flush(ctx); err != nil {
		return fmt.Errorf("write initial artifact: %w", err)
	}
	results := make([]error, len(suite.Paths))
	indices := make(chan int)
	var workers sync.WaitGroup
	workerCount := min(options.MaxParallel, max(1, len(suite.Paths)))
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range indices {
				path := suite.Paths[index]
				pathHarness, err := harness.NewPath(ctx, index, path)
				if err != nil {
					results[index] = fmt.Errorf("create path %d environment: %w", index, err)
					continue
				}
				bindings, err := runPath(ctx, pathHarness, path, recorder, index)
				if finishErr := recorder.finish(ctx, index, bindings, err); finishErr != nil {
					err = errors.Join(err, fmt.Errorf("write path artifact: %w", finishErr))
				}
				if err != nil {
					results[index] = fmt.Errorf("run path %d: %w", index, err)
				}
			}
		}()
	}
	for index := range suite.Paths {
		indices <- index
	}
	close(indices)
	workers.Wait()
	if err := errors.Join(results...); err != nil {
		return err
	}
	if err := recorder.complete(ctx); err != nil {
		return fmt.Errorf("write completed artifact: %w", err)
	}
	return nil
}

// ValidateSuite checks the completed structure before any environment is created.
func ValidateSuite(suite Suite) error {
	if len(suite.Paths) == 0 {
		return errors.New("suite contains no completed paths")
	}
	if suite.PathCount != len(suite.Paths) {
		return fmt.Errorf("path count is %d, but suite contains %d paths", suite.PathCount, len(suite.Paths))
	}
	if hasCycle(len(suite.IR.Nodes), suite.IR.Edges) {
		return errors.New("completed sparse DAG contains a cycle")
	}
	for pathIndex, path := range suite.Paths {
		stepCount := len(path.Steps)
		if stepCount == 0 {
			stepCount = len(path.Actions)
		}
		if len(path.Actions) > 0 && len(path.Steps) > 0 && len(path.Actions) != len(path.Steps) {
			return fmt.Errorf("path %d has %d actions and %d execution steps", pathIndex, len(path.Actions), len(path.Steps))
		}
		resources := map[string]bool{}
		for _, resource := range path.Resources {
			if resource.Name == "" || resources[resource.Name] {
				return fmt.Errorf("path %d has an empty or duplicate resource %q", pathIndex, resource.Name)
			}
			resources[resource.Name] = true
		}
		for _, policy := range path.Policies {
			if policy.Start < 0 || policy.End < policy.Start || policy.End > stepCount {
				return fmt.Errorf("path %d policy %q has invalid interval [%d,%d)", pathIndex, policy.Name, policy.Start, policy.End)
			}
		}
		for _, milestone := range path.Milestones {
			if milestone.BeforeAction < 0 || milestone.AfterAction < milestone.BeforeAction || milestone.AfterAction > stepCount {
				return fmt.Errorf("path %d milestone %q has invalid action interval [%d,%d]", pathIndex, milestone.Name, milestone.BeforeAction, milestone.AfterAction)
			}
		}
	}
	return nil
}

func runPath(ctx context.Context, harness PathHarness, path CompletedPath, recorder *artifactRecorder, pathIndex int) (bindings Bindings, resultErr error) {
	bindings = cloneRuntimeBindings(path.Bindings)
	var installed []*activeCleanup
	activePolicies := make([]*activeCleanup, len(path.Policies))
	cleanupContext := context.WithoutCancel(ctx)
	defer func() {
		if provider, ok := harness.(ArtifactFactProvider); ok {
			if err := recorder.facts(cleanupContext, pathIndex, provider); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("write fact artifact: %w", err))
			}
		}
		for index := len(activePolicies) - 1; index >= 0; index-- {
			entry := activePolicies[index]
			if entry == nil || !entry.active {
				continue
			}
			if err := runCleanup(cleanupContext, entry); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
		for index := len(installed) - 1; index >= 0; index-- {
			if err := runCleanup(cleanupContext, installed[index]); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
		if closer, ok := harness.(closablePath); ok {
			if err := closer.Close(cleanupContext); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("close path environment: %w", err))
			}
		}
	}()

	for _, resource := range path.Resources {
		cleanup, err := harness.SetupResource(ctx, resource)
		if err != nil {
			return bindings, fmt.Errorf("setup resource %s: %w", resource.Name, err)
		}
		installed = append(installed, &activeCleanup{name: "cleanup resource " + resource.Name, cleanup: cleanup, active: cleanup != nil})
	}
	steps := path.Steps
	if len(steps) == 0 && len(path.Actions) > 0 {
		steps = make([]CompletedStep, len(path.Actions))
		for index, action := range path.Actions {
			steps[index] = CompletedStep{Action: action, Mode: ProactiveAction}
		}
	}
	for _, step := range steps {
		cleanup, err := harness.InstallAction(ctx, step, bindings)
		if err != nil {
			return bindings, fmt.Errorf("install action %s: %w", step.Action.Name, err)
		}
		installed = append(installed, &activeCleanup{name: "uninstall action " + step.Action.Name, cleanup: cleanup, active: cleanup != nil})
	}

	if err := executeBoundary(ctx, harness, path, bindings, activePolicies, 0, ObservationCheckpoint, recorder, pathIndex); err != nil {
		return bindings, err
	}
	for index, step := range steps {
		if err := recorder.actionBegun(ctx, pathIndex, step.Action.Name, bindings); err != nil {
			return bindings, fmt.Errorf("write action artifact: %w", err)
		}
		if step.Mode == ProactiveAction {
			if len(step.Preconditions) > 0 {
				if err := harness.Await(ctx, step.Preconditions, bindings); err != nil {
					return bindings, fmt.Errorf("await %s preconditions: %w", step.Action.Name, err)
				}
			}
			if err := harness.Fire(ctx, step, bindings); err != nil {
				return bindings, fmt.Errorf("fire %s: %w", step.Action.Name, err)
			}
		}
		if err := harness.Reconcile(ctx, step, bindings); err != nil {
			return bindings, fmt.Errorf("reconcile %s: %w", step.Action.Name, err)
		}
		if provider, ok := harness.(ArtifactFactProvider); ok {
			if err := recorder.facts(ctx, pathIndex, provider); err != nil {
				return bindings, fmt.Errorf("write fact artifact: %w", err)
			}
		}
		if err := executeBoundary(ctx, harness, path, bindings, activePolicies, index+1, ActionCheckpoint, recorder, pathIndex); err != nil {
			return bindings, err
		}
	}
	if err := harness.Quiesce(ctx); err != nil {
		return bindings, fmt.Errorf("quiesce: %w", err)
	}
	if err := harness.CheckSafety(ctx, QuiescenceCheckpoint); err != nil {
		return bindings, fmt.Errorf("monitor safety at quiescence: %w", err)
	}
	if err := recorder.verdict(ctx, pathIndex, QuiescenceCheckpoint, bindings); err != nil {
		return bindings, fmt.Errorf("write Monitor artifact: %w", err)
	}
	if err := harness.ResolveLiveness(ctx); err != nil {
		return bindings, fmt.Errorf("resolve liveness: %w", err)
	}
	return bindings, nil
}

func executeBoundary(
	ctx context.Context,
	harness PathHarness,
	path CompletedPath,
	bindings Bindings,
	activePolicies []*activeCleanup,
	boundary int,
	checkpoint Checkpoint,
	recorder *artifactRecorder,
	pathIndex int,
) error {
	for index, policy := range path.Policies {
		if policy.Start != boundary || activePolicies[index] != nil {
			continue
		}
		cleanup, err := harness.ArmPolicy(ctx, policy, bindings)
		if err != nil {
			return fmt.Errorf("arm policy %s: %w", policy.Name, err)
		}
		activePolicies[index] = &activeCleanup{name: "disarm policy " + policy.Name, cleanup: cleanup, active: cleanup != nil}
		if err := recorder.policy(ctx, pathIndex, policy.Name, true, bindings); err != nil {
			return fmt.Errorf("write policy artifact: %w", err)
		}
	}
	observed := false
	for _, milestone := range path.Milestones {
		if milestone.AfterAction != boundary || (milestone.Kind != OutcomeKind && milestone.Kind != RelationKind && milestone.Kind != BindingKind) {
			continue
		}
		if err := harness.Observe(ctx, milestone, bindings); err != nil {
			return fmt.Errorf("observe %s: %w", milestone.Name, err)
		}
		if provider, ok := harness.(ArtifactFactProvider); ok {
			if err := recorder.facts(ctx, pathIndex, provider); err != nil {
				return fmt.Errorf("write fact artifact: %w", err)
			}
		}
		observed = true
		if err := recorder.observation(ctx, pathIndex, milestone.Name, bindings); err != nil {
			return fmt.Errorf("write observation artifact: %w", err)
		}
	}
	if boundary > 0 || observed {
		if err := harness.CheckSafety(ctx, checkpoint); err != nil {
			return fmt.Errorf("monitor safety at %s: %w", checkpoint, err)
		}
		if err := recorder.verdict(ctx, pathIndex, checkpoint, bindings); err != nil {
			return fmt.Errorf("write Monitor artifact: %w", err)
		}
	}
	for index := len(path.Policies) - 1; index >= 0; index-- {
		policy := path.Policies[index]
		entry := activePolicies[index]
		if policy.End != boundary || entry == nil || !entry.active {
			continue
		}
		if err := runCleanup(context.WithoutCancel(ctx), entry); err != nil {
			return err
		}
		if err := recorder.policy(ctx, pathIndex, policy.Name, false, bindings); err != nil {
			return fmt.Errorf("write policy artifact: %w", err)
		}
	}
	return nil
}

func runCleanup(ctx context.Context, entry *activeCleanup) error {
	if entry == nil || !entry.active {
		return nil
	}
	entry.active = false
	if entry.cleanup == nil {
		return nil
	}
	if err := entry.cleanup(ctx); err != nil {
		return fmt.Errorf("%s: %w", entry.name, err)
	}
	return nil
}
