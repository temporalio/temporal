package umpiretest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/campaign"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2"
)

// CampaignScenarioTransform applies environment-specific matrix, route, or fault selections.
type CampaignScenarioTransform func(campaign.Scenario) (coreregress.CompletedPath, error)

// CampaignExecutorOptions contains phase-specific execution and evidence bounds.
type CampaignExecutorOptions struct {
	RunOptions         coreregress.RunOptions
	TraceOptions       umpire.TraceOptions
	Transform          CampaignScenarioTransform
	ReplayCommand      func(campaign.Scenario) []string
	AdditionalArtifact coreregress.ArtifactSink
}

// TemporalCampaignExecutor realizes completed campaign paths in isolated Temporal environments.
type TemporalCampaignExecutor struct {
	factory umpire2.RegressionEnvironmentFactory
	profile coreregress.Profile
	options CampaignExecutorOptions
}

// NewCampaignExecutor constructs the Umpire2 adapter for the generic campaign engine.
func NewCampaignExecutor(
	factory umpire2.RegressionEnvironmentFactory,
	profile coreregress.Profile,
	options CampaignExecutorOptions,
) (*TemporalCampaignExecutor, error) {
	if factory == nil {
		return nil, errors.New("umpiretest campaign: environment factory is nil")
	}
	if options.RunOptions.MaxParallel == 0 {
		options.RunOptions.MaxParallel = 1
	}
	if options.RunOptions.MaxParallel != 1 {
		return nil, errors.New("umpiretest campaign: one scenario must execute serially")
	}
	if err := umpire.ValidateEnvironmentProfile(profile.Environment); err != nil {
		return nil, fmt.Errorf("umpiretest campaign: %w", err)
	}
	if options.Transform == nil {
		options.Transform = exactCampaignPath
	}
	return &TemporalCampaignExecutor{factory: factory, profile: profile, options: options}, nil
}

// Execute realizes one campaign scenario and preserves semantic failures as qualified claims.
func (e *TemporalCampaignExecutor) Execute(ctx context.Context, scenario campaign.Scenario) campaign.Execution {
	path, err := e.options.Transform(scenario)
	if err != nil {
		return unsupportedCampaignExecution(scenario, e.profile.Environment, err)
	}
	suite := coreregress.Suite{
		Name: scenario.Name, ModelVersion: scenario.ModelVersion, Profile: e.profile,
		Paths: []coreregress.CompletedPath{path}, PathCount: 1,
	}
	if err := coreregress.ValidateSuite(suite); err != nil {
		return failedCampaignExecution(scenario, e.profile.Environment, err)
	}

	recorder := umpire.NewTraceRecorder(e.options.TraceOptions)
	coverage, err := umpire.NewCoverage(true)
	if err != nil {
		return failedCampaignExecution(scenario, e.profile.Environment, err)
	}
	cleanup := &campaignCleanup{}
	factory := instrumentCampaignFactory(e.factory, recorder, coverage, cleanup)
	sink := &capturingArtifactSink{delegate: e.options.AdditionalArtifact}
	harness := umpire2.NewRegressionHarness(factory, sink)
	runErr := coreregress.RunWithOptions(ctx, suite, harness, e.options.RunOptions)
	artifact := sink.Snapshot()
	trace := recorder.Snapshot()
	trace.Complete = ctx.Err() == nil && cleanup.Complete()
	execution := campaign.Execution{
		Trace:            trace,
		Artifact:         artifact,
		ObservedCoverage: coverage.Snapshot(),
		CleanupComplete:  cleanup.Complete(),
		TimedOut:         errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(runErr, context.DeadlineExceeded),
	}
	if e.options.ReplayCommand != nil {
		execution.ReplayCommand = slices.Clone(e.options.ReplayCommand(scenario))
	}
	execution.Claim = campaignClaim(scenario, e.profile.Environment, artifact, runErr)
	if runErr != nil && execution.Claim.Status != umpire.ClaimViolated {
		execution.Error = umpire.ExecutionErrorClass(runErr)
	}
	return execution
}

func exactCampaignPath(scenario campaign.Scenario) (coreregress.CompletedPath, error) {
	if len(scenario.Matrix.Values) != 0 {
		return coreregress.CompletedPath{}, errors.New("matrix selections require an explicit campaign scenario transformer")
	}
	if len(scenario.ExplorationRoute) != 0 {
		return coreregress.CompletedPath{}, errors.New("exploration routes require an explicit campaign scenario transformer")
	}
	if len(scenario.Faults) != 0 {
		return coreregress.CompletedPath{}, errors.New("fault selections require an explicit campaign scenario transformer")
	}
	return scenario.Path, nil
}

type campaignMonitorInstrumentation interface {
	SetCoverage(*umpire.Coverage)
	SetTraceRecorder(*umpire.TraceRecorder)
}

type campaignCleanup struct {
	mu       sync.Mutex
	called   bool
	failed   bool
	prepared bool
}

func (c *campaignCleanup) Complete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepared && c.called && !c.failed
}

func instrumentCampaignFactory(
	factory umpire2.RegressionEnvironmentFactory,
	recorder *umpire.TraceRecorder,
	coverage *umpire.Coverage,
	state *campaignCleanup,
) umpire2.RegressionEnvironmentFactory {
	return func(ctx context.Context, index int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
		environment, cleanup, err := factory(ctx, index)
		if err != nil {
			return environment, cleanup, err
		}
		instrumentation, ok := environment.GetMonitor().(campaignMonitorInstrumentation)
		if !ok {
			if cleanup != nil {
				_ = cleanup(context.WithoutCancel(ctx))
			}
			return nil, nil, errors.New("Umpire2 monitor does not support campaign trace and coverage instrumentation")
		}
		instrumentation.SetTraceRecorder(recorder)
		instrumentation.SetCoverage(coverage)
		state.mu.Lock()
		state.prepared = true
		state.mu.Unlock()
		return environment, func(cleanupContext context.Context) error {
			var cleanupErr error
			if cleanup != nil {
				cleanupErr = cleanup(cleanupContext)
			}
			state.mu.Lock()
			state.called = true
			state.failed = cleanupErr != nil
			state.mu.Unlock()
			return cleanupErr
		}, nil
	}
}

func campaignClaim(
	scenario campaign.Scenario,
	profile umpire.EnvironmentProfile,
	artifact *coreregress.Artifact,
	runErr error,
) umpire.QualifiedClaim {
	if artifact != nil && len(artifact.Paths) == 1 {
		claims := artifact.Paths[0].Claims
		for _, claim := range claims {
			if claim.Status == umpire.ClaimViolated {
				return claim
			}
		}
		if len(claims) > 0 {
			return claims[len(claims)-1]
		}
	}
	claim := umpire.QualifyEvidence(
		scenario.ModelVersion,
		"live-regression",
		profile,
		umpire.EvidenceRequirement{Property: umpire.MonitorSafetyProperty("quiescence"), Sources: []umpire.EvidenceSource{umpire.InProcessEvidence}},
		umpire.ObservedEvidence{},
		runErr != nil,
	)
	claim.Status = umpire.ClaimInconclusive
	claim.Diagnostic = "regression execution produced no evidence-qualified checkpoint"
	return claim
}

func unsupportedCampaignExecution(
	scenario campaign.Scenario,
	profile umpire.EnvironmentProfile,
	err error,
) campaign.Execution {
	claim := umpire.QualifiedClaim{
		ModelVersion: scenario.ModelVersion,
		Target:       "live-regression",
		Property:     umpire.MonitorSafetyProperty("quiescence"),
		Environment:  profile.Name,
		Status:       umpire.ClaimUnsupported,
		Diagnostic:   err.Error(),
	}
	return campaign.Execution{Claim: claim}
}

func failedCampaignExecution(
	scenario campaign.Scenario,
	profile umpire.EnvironmentProfile,
	err error,
) campaign.Execution {
	execution := unsupportedCampaignExecution(scenario, profile, err)
	execution.Claim.Status = umpire.ClaimInconclusive
	execution.Error = umpire.ExecutionErrorClass(err)
	return execution
}

var _ campaign.Executor = (*TemporalCampaignExecutor)(nil)
