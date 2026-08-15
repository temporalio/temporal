// Package umpiretest provides Temporal test sessions and high-level Umpire runners.
package umpiretest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	chasmactivity "go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire2"
	"go.temporal.io/server/tests/umpire2/regress/capability"
)

// RegressionRequest is the complete input to one Temporal sparse-regression run.
type RegressionRequest struct {
	Protocol     *umpire2.Protocol
	Plan         coreregress.Plan
	Profile      coreregress.Profile
	Environment  umpire2.RegressionEnvironmentFactory
	RunOptions   coreregress.RunOptions
	ArtifactSink coreregress.ArtifactSink
}

// RegressionResult retains the compiled intent and latest incremental artifact.
type RegressionResult struct {
	ProtocolVersion string
	Profile         coreregress.Profile
	RunOptions      coreregress.RunOptions
	Suite           coreregress.Suite
	Artifact        *coreregress.Artifact
}

// RunRegression validates and compiles before allocating an environment, then runs every path.
func RunRegression(ctx context.Context, request RegressionRequest) (RegressionResult, error) {
	if request.Protocol == nil {
		return RegressionResult{}, errors.New("umpiretest regression: protocol is nil")
	}
	if request.Environment == nil {
		return RegressionResult{}, errors.New("umpiretest regression: environment factory is nil")
	}
	if request.RunOptions.MaxParallel < 1 {
		return RegressionResult{}, errors.New("umpiretest regression: MaxParallel must be positive")
	}
	suite, err := request.Protocol.CompileRegression(request.Plan, request.Profile)
	result := RegressionResult{
		ProtocolVersion: suite.ModelVersion,
		Profile:         request.Profile,
		RunOptions:      request.RunOptions,
		Suite:           suite,
	}
	if err != nil {
		return result, fmt.Errorf("umpiretest regression: compile: %w", err)
	}
	sink := &capturingArtifactSink{delegate: request.ArtifactSink}
	harness := umpire2.NewRegressionHarness(request.Environment, sink)
	err = coreregress.RunWithOptions(ctx, suite, harness, request.RunOptions)
	result.Artifact = sink.Snapshot()
	if err != nil {
		return result, fmt.Errorf("umpiretest regression: execute: %w", err)
	}
	return result, nil
}

type capturingArtifactSink struct {
	delegate coreregress.ArtifactSink
	mu       sync.Mutex
	latest   *coreregress.Artifact
}

func (s *capturingArtifactSink) WriteArtifact(ctx context.Context, artifact coreregress.Artifact) error {
	s.mu.Lock()
	copy := artifact
	s.latest = &copy
	s.mu.Unlock()
	if s.delegate == nil {
		return nil
	}
	return s.delegate.WriteArtifact(ctx, artifact)
}

func (s *capturingArtifactSink) Snapshot() *coreregress.Artifact {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		return nil
	}
	copy := *s.latest
	return &copy
}

type regressionConfig struct {
	chasm       bool
	timeout     time.Duration
	runOptions  coreregress.RunOptions
	artifact    coreregress.ArtifactSink
	environment umpire2.RegressionEnvironmentFactory
}

// RegressionOption customizes the local regression affordance.
type RegressionOption func(*regressionConfig)

// WithCHASM selects the CHASM or HSM local environment preset.
func WithCHASM(enabled bool) RegressionOption {
	return func(config *regressionConfig) { config.chasm = enabled }
}

// WithRegressionTimeout changes the total regression deadline.
func WithRegressionTimeout(timeout time.Duration) RegressionOption {
	return func(config *regressionConfig) { config.timeout = timeout }
}

// WithRegressionRunOptions changes path execution concurrency.
func WithRegressionRunOptions(options coreregress.RunOptions) RegressionOption {
	return func(config *regressionConfig) { config.runOptions = options }
}

// WithRegressionArtifactSink retains incremental artifacts in an additional sink.
func WithRegressionArtifactSink(sink coreregress.ArtifactSink) RegressionOption {
	return func(config *regressionConfig) { config.artifact = sink }
}

// WithRegressionEnvironment uses an explicit environment factory instead of the local preset.
func WithRegressionEnvironment(factory umpire2.RegressionEnvironmentFactory) RegressionOption {
	return func(config *regressionConfig) { config.environment = factory }
}

// RequireRegression runs sparse intent with the isolated local preset and requires conformance.
func RequireRegression(t *testing.T, plan coreregress.Plan, options ...RegressionOption) RegressionResult {
	t.Helper()
	if err := ConfigureProcessInstrumentation(); err != nil {
		t.Fatalf("umpiretest regression: configure process instrumentation: %v", err)
	}
	config := regressionConfig{chasm: true, timeout: time.Minute, runOptions: coreregress.RunOptions{MaxParallel: 1}}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	if config.timeout <= 0 {
		t.Fatalf("umpiretest regression: timeout must be positive")
	}
	protocol, err := CanonicalProtocol()
	if err != nil {
		t.Fatalf("umpiretest regression: compile default protocol: %v", err)
	}
	profile := localRegressionProfile(config.chasm)
	factory := config.environment
	if factory == nil {
		factory = localRegressionFactory(t, config.chasm)
	}
	ctx, cancel := context.WithTimeout(t.Context(), config.timeout)
	defer cancel()
	result, err := RunRegression(ctx, RegressionRequest{
		Protocol: protocol, Plan: plan, Profile: profile, Environment: factory,
		RunOptions: config.runOptions, ArtifactSink: config.artifact,
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	return result
}

func localRegressionProfile(chasmEnabled bool) coreregress.Profile {
	capabilities := []string{capability.Faults.Name}
	if chasmEnabled {
		capabilities = append(capabilities, capability.CHASM.Name, capability.ActivityCallbacks.Name)
	}
	return coreregress.Profile{
		Name:         "local",
		Capabilities: capabilities,
		Environment:  umpire.InProcessProfile(),
	}
}

func localRegressionFactory(t *testing.T, chasmEnabled bool) umpire2.RegressionEnvironmentFactory {
	return func(context.Context, int) (umpire2.RegressionEnvironment, coreregress.Cleanup, error) {
		environment := testcore.NewEnv(t,
			testcore.WithUmpireMonitorFactory(umpire2.NewMonitor),
			testcore.WithDynamicConfig(dynamicconfig.EnableChasm, chasmEnabled),
			testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, chasmEnabled),
			testcore.WithDynamicConfig(chasmnexus.Enabled, chasmEnabled),
			testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, chasmEnabled),
			testcore.WithDynamicConfig(chasmactivity.Enabled, chasmEnabled),
			testcore.WithDynamicConfig(chasmactivity.EnableCallbacks, chasmEnabled),
			testcore.WithDynamicConfig(callback.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		)
		return &localRegressionEnvironment{TestEnv: environment, t: t}, nil, nil
	}
}

type localRegressionEnvironment struct {
	*testcore.TestEnv
	t *testing.T
}

func (e *localRegressionEnvironment) StartNexusServer(listenAddress string, handler nexus.Handler) {
	nexustest.NewNexusServer(e.t, listenAddress, handler)
}
