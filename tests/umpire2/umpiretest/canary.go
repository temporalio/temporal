package umpiretest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/canary"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2"
)

// CanaryActionPlan resolves one allowlisted canary operation into completed Temporal intent.
type CanaryActionPlan func(canary.Action) (coreregress.CompletedPath, error)

// CanaryDriverOptions configures the Umpire2 adapter without weakening the common safety envelope.
type CanaryDriverOptions struct {
	ModelVersion string
	Profile      coreregress.Profile
	Plan         CanaryActionPlan
	Resources    func(umpire2.RegressionEnvironment) []canary.Resource
}

// TemporalCanaryDriver executes approved completed paths in one prepared isolated environment.
type TemporalCanaryDriver struct {
	factory umpire2.RegressionEnvironmentFactory
	options CanaryDriverOptions
	mu      sync.Mutex
	active  *preparedCanary
	next    atomic.Uint64
	runMu   sync.Mutex
}

type preparedCanary struct {
	token       string
	environment umpire2.RegressionEnvironment
	cleanup     coreregress.Cleanup
}

// NewCanaryDriver constructs a context-compliant driver for common/testing/umpire/canary.
func NewCanaryDriver(
	factory umpire2.RegressionEnvironmentFactory,
	options CanaryDriverOptions,
) (*TemporalCanaryDriver, error) {
	if factory == nil {
		return nil, errors.New("umpiretest canary: environment factory is nil")
	}
	if options.ModelVersion == "" {
		return nil, errors.New("umpiretest canary: model version is empty")
	}
	if options.Plan == nil {
		return nil, errors.New("umpiretest canary: action plan resolver is nil")
	}
	if err := umpire.ValidateEnvironmentProfile(options.Profile.Environment); err != nil {
		return nil, fmt.Errorf("umpiretest canary: %w", err)
	}
	if options.Profile.Environment.Kind != umpire.CanaryEnvironment {
		return nil, errors.New("umpiretest canary: evidence profile kind must be canary")
	}
	return &TemporalCanaryDriver{factory: factory, options: options}, nil
}

// Prepare allocates one isolated Umpire2 regression environment before any canary action starts.
func (d *TemporalCanaryDriver) Prepare(ctx context.Context, request canary.PreparationRequest) (canary.Preparation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active != nil {
		return canary.Preparation{}, errors.New("umpiretest canary: a prepared campaign is already active")
	}
	environment, cleanup, err := d.factory(ctx, 0)
	token := request.CampaignID + "/" + strconv.FormatUint(d.next.Add(1), 10)
	prepared := &preparedCanary{token: token, environment: environment, cleanup: cleanup}
	d.active = prepared
	preparation := canary.Preparation{Scope: request, CleanupToken: token}
	if environment != nil && d.options.Resources != nil {
		preparation.Resources = d.options.Resources(environment)
	}
	if err != nil {
		return preparation, err
	}
	if environment == nil {
		return preparation, errors.New("umpiretest canary: environment factory returned nil")
	}
	if actual := environment.Namespace().String(); actual != request.Namespace {
		preparation.Scope.Namespace = actual
	}
	return preparation, nil
}

// Execute serially realizes one allowlisted action and reports only secret-safe semantic fields.
func (d *TemporalCanaryDriver) Execute(ctx context.Context, action canary.Action) canary.Observation {
	d.runMu.Lock()
	defer d.runMu.Unlock()
	d.mu.Lock()
	prepared := d.active
	d.mu.Unlock()
	if prepared == nil || prepared.environment == nil {
		return canary.Observation{Error: errors.New("umpiretest canary: no prepared environment")}
	}
	path, err := d.options.Plan(action)
	if err != nil {
		return canary.Observation{Error: fmt.Errorf("umpiretest canary: resolve action %q: %w", action.Name, err)}
	}
	suite := coreregress.Suite{
		Name:         action.Name,
		ModelVersion: d.options.ModelVersion,
		Profile:      d.options.Profile,
		Paths:        []coreregress.CompletedPath{path},
		PathCount:    1,
	}
	if err := coreregress.ValidateSuite(suite); err != nil {
		return canary.Observation{Error: fmt.Errorf("umpiretest canary: invalid completed action %q: %w", action.Name, err)}
	}
	sink := &capturingArtifactSink{}
	harness := umpire2.NewRegressionHarness(
		func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
			return prepared.environment, nil, nil
		},
		sink,
	)
	runErr := coreregress.Run(ctx, suite, harness)
	claim, ok := finalArtifactClaim(sink.Snapshot())
	observation := canary.Observation{Fields: map[string]string{
		"action":        action.Name,
		"model_version": d.options.ModelVersion,
	}}
	if ok {
		observation.Fields["property"] = claim.Property
		observation.Fields["claim_status"] = string(claim.Status)
	}
	switch {
	case ok && claim.Status == umpire.ClaimViolated:
		observation.InvariantViolated = true
		observation.Error = runErr
	case !ok || claim.Status == umpire.ClaimUnsupported || claim.Status == umpire.ClaimInconclusive:
		observation.ObservationLost = true
		if runErr != nil {
			observation.Error = runErr
		} else {
			observation.Error = errors.New("umpiretest canary: no conclusive evidence-qualified checkpoint")
		}
	case runErr != nil:
		observation.Error = runErr
	}
	return observation
}

// Cleanup releases exactly the resources returned by the active preparation.
func (d *TemporalCanaryDriver) Cleanup(ctx context.Context, preparation canary.Preparation) error {
	d.runMu.Lock()
	defer d.runMu.Unlock()
	d.mu.Lock()
	prepared := d.active
	if prepared == nil {
		d.mu.Unlock()
		return nil
	}
	if preparation.CleanupToken != prepared.token {
		d.mu.Unlock()
		return errors.New("umpiretest canary: cleanup token does not match the active campaign")
	}
	d.active = nil
	d.mu.Unlock()
	if prepared.cleanup == nil {
		return nil
	}
	return prepared.cleanup(ctx)
}

func finalArtifactClaim(artifact *coreregress.Artifact) (umpire.QualifiedClaim, bool) {
	if artifact == nil || len(artifact.Paths) != 1 || len(artifact.Paths[0].Claims) == 0 {
		return umpire.QualifiedClaim{}, false
	}
	claims := artifact.Paths[0].Claims
	for _, claim := range claims {
		if claim.Status == umpire.ClaimViolated {
			return claim, true
		}
	}
	return claims[len(claims)-1], true
}

var _ canary.Driver = (*TemporalCanaryDriver)(nil)
