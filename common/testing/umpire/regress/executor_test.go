package regress_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
)

func TestRunEmitsNeutralActionWindowsAndVerdicts(t *testing.T) {
	observer := &recordingExecutionObserver{}
	harness := &recordingHarness{observer: observer}
	suite := regress.Suite{
		Paths: []regress.CompletedPath{{Steps: []regress.CompletedStep{
			{Action: regress.CompletedAction{Name: "proactive"}, Mode: regress.ProactiveAction},
			{Action: regress.CompletedAction{Name: "reactive"}, Mode: regress.ReactiveAction},
		}}},
		PathCount: 1,
	}

	require.NoError(t, regress.Run(t.Context(), suite, harness))
	require.Equal(t, []umpire.ExecutionObservation{
		{Kind: umpire.ExecutionActionStart, Action: "proactive", Phase: "install", Outcome: umpire.ExecutionOutcomeStarted},
		{Kind: umpire.ExecutionActionStart, Action: "reactive", Phase: "install", Outcome: umpire.ExecutionOutcomeStarted},
		{Kind: umpire.ExecutionActionFinish, Action: "proactive", Phase: "reconcile", Outcome: umpire.ExecutionOutcomeSucceeded},
		{Kind: umpire.ExecutionVerdict, Checkpoint: "action", Property: umpire.MonitorSafetyProperty("action"), Pass: true},
		{Kind: umpire.ExecutionActionFinish, Action: "reactive", Phase: "reconcile", Outcome: umpire.ExecutionOutcomeSucceeded},
		{Kind: umpire.ExecutionVerdict, Checkpoint: "action", Property: umpire.MonitorSafetyProperty("action"), Pass: true},
		{Kind: umpire.ExecutionVerdict, Checkpoint: "quiescence", Property: umpire.MonitorSafetyProperty("quiescence"), Pass: true},
	}, observer.observations)
}

func TestRunPropagatesExecutionObserverError(t *testing.T) {
	observerErr := errors.New("observer failed")
	harness := &recordingHarness{observer: &recordingExecutionObserver{err: observerErr}}
	suite := regress.Suite{
		Paths:     []regress.CompletedPath{{Steps: []regress.CompletedStep{{Action: regress.CompletedAction{Name: "action"}, Mode: regress.ProactiveAction}}}},
		PathCount: 1,
	}

	err := regress.Run(t.Context(), suite, harness)
	require.ErrorIs(t, err, observerErr)
}

func TestRunExecutesPathAndCleansUpInReverseOrder(t *testing.T) {
	harness := &recordingHarness{}
	suite := regress.Suite{
		ModelVersion: "fake/v1",
		Profile:      regress.Profile{Name: "local"},
		Paths: []regress.CompletedPath{{
			Resources: []regress.CompletedResource{{Name: "namespace"}, {Name: "worker"}},
			Actions:   []regress.CompletedAction{{Name: "task.finish"}},
			Steps: []regress.CompletedStep{{
				Action: regress.CompletedAction{Name: "task.finish"},
				Mode:   regress.ProactiveAction,
				Preconditions: []regress.CompletedAtom{{
					Predicate: "task.state",
					Arguments: []regress.Argument{regress.Symbol("job"), regress.Literal("ready")},
				}},
			}},
			Policies: []regress.CompletedPolicy{{Name: "rpc.fail-next", Start: 0, End: 1}},
			Milestones: []regress.CompletedMilestone{{
				Node:        0,
				Kind:        regress.OutcomeKind,
				Name:        "task.state",
				Arguments:   []regress.Argument{regress.Symbol("job"), regress.Literal("done")},
				AfterAction: 1,
			}},
		}},
		PathCount: 1,
	}

	err := regress.Run(context.Background(), suite, harness)
	require.NoError(t, err)
	require.Equal(t, []string{
		"new-path:0",
		"setup:namespace",
		"setup:worker",
		"install:task.finish",
		"arm:rpc.fail-next",
		"await:task.state",
		"fire:task.finish",
		"reconcile:task.finish",
		"observe:task.state",
		"safety:action",
		"disarm:rpc.fail-next",
		"quiesce",
		"safety:quiescence",
		"liveness",
		"uninstall:task.finish",
		"cleanup:worker",
		"cleanup:namespace",
	}, harness.events)
}

func TestRunDisarmsPoliciesAndCleansResourcesAfterDriveFailure(t *testing.T) {
	driveErr := errors.New("injected drive failure")
	harness := &recordingHarness{fireErr: driveErr}
	suite := regress.Suite{
		Paths: []regress.CompletedPath{{
			Resources: []regress.CompletedResource{{Name: "namespace"}},
			Actions:   []regress.CompletedAction{{Name: "task.finish"}},
			Steps: []regress.CompletedStep{{
				Action: regress.CompletedAction{Name: "task.finish"},
				Mode:   regress.ProactiveAction,
			}},
			Policies: []regress.CompletedPolicy{{Name: "rpc.fail-next", Start: 0, End: 1}},
		}},
		PathCount: 1,
	}

	err := regress.Run(context.Background(), suite, harness)
	require.ErrorIs(t, err, driveErr)
	require.Equal(t, []string{
		"new-path:0",
		"setup:namespace",
		"install:task.finish",
		"arm:rpc.fail-next",
		"fire:task.finish",
		"disarm:rpc.fail-next",
		"uninstall:task.finish",
		"cleanup:namespace",
	}, harness.events)
}

func TestRunWithOptionsBoundsParallelPathExecution(t *testing.T) {
	harness := &parallelHarness{release: make(chan struct{})}
	suite := regress.Suite{Paths: make([]regress.CompletedPath, 3), PathCount: 3}
	for index := range suite.Paths {
		suite.Paths[index].Steps = []regress.CompletedStep{{Action: regress.CompletedAction{Name: "block"}, Mode: regress.ProactiveAction}}
	}
	done := make(chan error, 1)
	go func() {
		done <- regress.RunWithOptions(context.Background(), suite, harness, regress.RunOptions{MaxParallel: 2})
	}()
	await.RequireTrue(t, func() bool { return harness.peak.Load() == 2 }, time.Second, time.Millisecond)
	close(harness.release)
	require.NoError(t, <-done)
	require.EqualValues(t, 2, harness.peak.Load())
}

func TestRunRejectsInvalidCompletedSuiteBeforeCreatingEnvironment(t *testing.T) {
	harness := &recordingHarness{}
	err := regress.Run(context.Background(), regress.Suite{
		Paths:     []regress.CompletedPath{{}},
		PathCount: 2,
	}, harness)
	require.ErrorContains(t, err, "path count is 2")
	require.Empty(t, harness.events)
}

func TestRunRejectsHarnessPreflightBeforeCreatingEnvironment(t *testing.T) {
	preflightErr := errors.New("unsupported realization")
	harness := &preflightHarness{
		recordingHarness: &recordingHarness{},
		err:              preflightErr,
	}

	err := regress.Run(context.Background(), regress.Suite{
		Paths:     []regress.CompletedPath{{}},
		PathCount: 1,
	}, harness)

	require.ErrorIs(t, err, preflightErr)
	require.Empty(t, harness.events)
}

func TestRunCleansUpAfterCancellationOrTimeout(t *testing.T) {
	for _, driveErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(driveErr.Error(), func(t *testing.T) {
			harness := &recordingHarness{fireErr: driveErr}
			suite := regress.Suite{
				Paths: []regress.CompletedPath{{
					Resources: []regress.CompletedResource{{Name: "namespace"}},
					Steps:     []regress.CompletedStep{{Action: regress.CompletedAction{Name: "drive"}, Mode: regress.ProactiveAction}},
					Policies:  []regress.CompletedPolicy{{Name: "fault", Start: 0, End: 1}},
				}},
				PathCount: 1,
			}

			err := regress.Run(context.Background(), suite, harness)
			require.ErrorIs(t, err, driveErr)
			require.Contains(t, harness.events, "disarm:fault")
			require.Contains(t, harness.events, "cleanup:namespace")
		})
	}
}

func TestRunFailsOnEffectDriftAndGlobalSafetyViolation(t *testing.T) {
	for name, harness := range map[string]*recordingHarness{
		"effect drift":     {reconcileErr: errors.New("effect drift")},
		"safety violation": {safetyErr: errors.New("safety violation")},
	} {
		t.Run(name, func(t *testing.T) {
			suite := regress.Suite{
				Paths:     []regress.CompletedPath{{Steps: []regress.CompletedStep{{Action: regress.CompletedAction{Name: "drive"}, Mode: regress.ProactiveAction}}}},
				PathCount: 1,
			}

			err := regress.Run(context.Background(), suite, harness)
			require.Error(t, err)
			require.ErrorContains(t, err, name)
		})
	}
}

type recordingHarness struct {
	events       []string
	fireErr      error
	reconcileErr error
	safetyErr    error
	observer     *recordingExecutionObserver
}

type recordingExecutionObserver struct {
	observations []umpire.ExecutionObservation
	err          error
}

type preflightHarness struct {
	*recordingHarness
	err error
}

func (h *preflightHarness) Preflight(regress.Suite) error {
	return h.err
}

func (o *recordingExecutionObserver) ObserveExecution(_ context.Context, observed umpire.ExecutionObservation) error {
	if o.err != nil {
		return o.err
	}
	o.observations = append(o.observations, observed)
	return nil
}

type parallelHarness struct {
	active  atomic.Int32
	peak    atomic.Int32
	release chan struct{}
}

func (h *parallelHarness) NewPath(context.Context, int, regress.CompletedPath) (regress.PathHarness, error) {
	return &parallelPath{parent: h}, nil
}

type parallelPath struct{ parent *parallelHarness }

func (p *parallelPath) SetupResource(context.Context, regress.CompletedResource) (regress.Cleanup, error) {
	return nil, nil
}
func (p *parallelPath) InstallAction(context.Context, regress.CompletedStep, regress.Bindings) (regress.Cleanup, error) {
	return nil, nil
}
func (p *parallelPath) ArmPolicy(context.Context, regress.CompletedPolicy, regress.Bindings) (regress.Cleanup, error) {
	return nil, nil
}
func (p *parallelPath) Await(context.Context, []regress.CompletedAtom, regress.Bindings) error {
	return nil
}
func (p *parallelPath) Fire(context.Context, regress.CompletedStep, regress.Bindings) error {
	active := p.parent.active.Add(1)
	for peak := p.parent.peak.Load(); active > peak && !p.parent.peak.CompareAndSwap(peak, active); peak = p.parent.peak.Load() {
	}
	<-p.parent.release
	p.parent.active.Add(-1)
	return nil
}
func (p *parallelPath) Reconcile(context.Context, regress.CompletedStep, regress.Bindings) error {
	return nil
}
func (p *parallelPath) Observe(context.Context, regress.CompletedMilestone, regress.Bindings) error {
	return nil
}
func (p *parallelPath) CheckSafety(context.Context, regress.Checkpoint) error { return nil }
func (p *parallelPath) Quiesce(context.Context) error                         { return nil }
func (p *parallelPath) ResolveLiveness(context.Context) error                 { return nil }

func (h *recordingHarness) NewPath(_ context.Context, index int, _ regress.CompletedPath) (regress.PathHarness, error) {
	h.events = append(h.events, fmt.Sprintf("new-path:%d", index))
	return &recordingPath{parent: h}, nil
}

type recordingPath struct {
	parent *recordingHarness
}

func (p *recordingPath) ExecutionObserver() umpire.ExecutionObserver {
	if p.parent.observer == nil {
		return nil
	}
	return p.parent.observer
}

func (p *recordingPath) SetupResource(_ context.Context, resource regress.CompletedResource) (regress.Cleanup, error) {
	p.parent.events = append(p.parent.events, "setup:"+resource.Name)
	return func(context.Context) error {
		p.parent.events = append(p.parent.events, "cleanup:"+resource.Name)
		return nil
	}, nil
}

func (p *recordingPath) InstallAction(_ context.Context, step regress.CompletedStep, _ regress.Bindings) (regress.Cleanup, error) {
	p.parent.events = append(p.parent.events, "install:"+step.Action.Name)
	return func(context.Context) error {
		p.parent.events = append(p.parent.events, "uninstall:"+step.Action.Name)
		return nil
	}, nil
}

func (p *recordingPath) ArmPolicy(_ context.Context, policy regress.CompletedPolicy, _ regress.Bindings) (regress.Cleanup, error) {
	p.parent.events = append(p.parent.events, "arm:"+policy.Name)
	return func(context.Context) error {
		p.parent.events = append(p.parent.events, "disarm:"+policy.Name)
		return nil
	}, nil
}

func (p *recordingPath) Await(_ context.Context, atoms []regress.CompletedAtom, _ regress.Bindings) error {
	if len(atoms) > 0 {
		p.parent.events = append(p.parent.events, "await:"+atoms[0].Predicate)
	}
	return nil
}

func (p *recordingPath) Fire(_ context.Context, step regress.CompletedStep, bindings regress.Bindings) error {
	p.parent.events = append(p.parent.events, "fire:"+step.Action.Name)
	bindings["job"] = "job-1"
	return p.parent.fireErr
}

func (p *recordingPath) Reconcile(_ context.Context, step regress.CompletedStep, _ regress.Bindings) error {
	p.parent.events = append(p.parent.events, "reconcile:"+step.Action.Name)
	return p.parent.reconcileErr
}

func (p *recordingPath) Observe(_ context.Context, milestone regress.CompletedMilestone, _ regress.Bindings) error {
	p.parent.events = append(p.parent.events, "observe:"+milestone.Name)
	return nil
}

func (p *recordingPath) CheckSafety(_ context.Context, checkpoint regress.Checkpoint) error {
	p.parent.events = append(p.parent.events, "safety:"+checkpoint.String())
	return p.parent.safetyErr
}

func (p *recordingPath) Quiesce(context.Context) error {
	p.parent.events = append(p.parent.events, "quiesce")
	return nil
}

func (p *recordingPath) ResolveLiveness(context.Context) error {
	p.parent.events = append(p.parent.events, "liveness")
	return nil
}
