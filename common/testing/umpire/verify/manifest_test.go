package verify

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManifestRecordsAssuranceBoundaryDeterministically(t *testing.T) {
	model := Model{
		Version:    "test/v1",
		Entities:   []EntityType{{Name: "job", IDs: []string{"job#0", "job#1"}, Initial: "ready", States: []State{{Name: "ready"}}}},
		Actions:    []Action{{Name: "schedule"}},
		Properties: []Property{{Name: "safe", Kind: SafetyProperty, Expr: Expr{Op: TrueExpr}}},
	}
	model.Actions[0].Unrealized = true
	model.Actions[0].Capabilities = []string{"rpc", "faults"}
	model.Abstractions = []Abstraction{{Name: "schedule", Reason: "model-only"}}
	model.Properties[0].Fairness = []string{"weak-schedule"}

	manifest, err := NewManifest(model, ManifestOptions{
		GeneratorVersion:    "umpire-genmodels/v1",
		Target:              "feature-workflow",
		TargetOwners:        []CapabilityOwner{"workflow", "delivery"},
		TargetModules:       []string{"workflow", "delivery"},
		TargetCompositions:  []string{"workflow-delivery"},
		TargetProperties:    []string{"safe"},
		ModelFamilyVersion:  "test-family/v1",
		ModelFamilyHash:     "sha256:family",
		BackendRequirements: []string{"tla", "ivy"},
		MinimumBounds:       map[string]int{"job": 2},
		FailurePolicy:       []string{"ambiguous-persistence"},
		Interfaces: []ManifestInterface{{
			Name:        "delivery",
			Provider:    ManifestModuleRef{Module: "delivery", Owner: "delivery"},
			Consumers:   []ManifestModuleRef{{Module: "workflow", Owner: "workflow"}},
			Obligations: []string{"accepted"},
		}},
		Guarantee: FiniteExhaustive,
		Tools: []ToolVersion{
			{Name: "tlc", Version: "1.7.4", SHA256: "abc"},
			{Name: "p", Version: "3.1.0"},
		},
		Unsupported: []Unsupported{{Backend: "ivy", Construct: "property progresses", Reason: "not inductive safety"}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, manifest.ModelHash)
	require.Equal(t, map[string]int{"job": 2}, manifest.Bounds)
	require.Equal(t, []string{"schedule"}, manifest.Actions)
	require.Equal(t, []string{"safe"}, manifest.Properties)
	require.Equal(t, []string{"weak-schedule"}, manifest.Fairness)
	require.Equal(t, []Abstraction{{Name: "schedule", Reason: "model-only"}}, manifest.Abstractions)
	require.Equal(t, "feature-workflow", manifest.Target)
	require.Equal(t, []CapabilityOwner{"delivery", "workflow"}, manifest.TargetOwners)
	require.Equal(t, []string{"delivery", "workflow"}, manifest.TargetModules)
	require.Equal(t, map[string]int{"job": 2}, manifest.MinimumBounds)
	require.Equal(t, []string{"ambiguous-persistence"}, manifest.FailurePolicy)
	require.Equal(t, "delivery", manifest.Interfaces[0].Name)

	first, err := MarshalManifest(manifest)
	require.NoError(t, err)
	second, err := MarshalManifest(manifest)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestResultRejectsSuccessWhenExplorationHitLimit(t *testing.T) {
	for _, termination := range []TerminationReason{
		Timeout,
		DepthLimit,
		StateLimit,
		StepLimit,
		MemoryLimit,
		ScheduleLimit,
		ToolLimit,
		Interrupted,
		EvidenceFailure,
	} {
		t.Run(string(termination), func(t *testing.T) {
			err := ValidateResult(Result{Status: FiniteExhaustive, Termination: termination})
			require.ErrorContains(t, err, "cannot claim")
		})
	}
}

func TestResultRejectsSuccessWithUnsupportedSemantics(t *testing.T) {
	err := ValidateResult(Result{
		Status: InvariantProved, Termination: Completed,
		Unsupported: []Unsupported{{Backend: "ivy", Construct: "property delivery.progress", Reason: "not inductive"}},
	})
	require.ErrorContains(t, err, "cannot claim")
}

func TestResultRejectsCounterexampleWithoutNativeTrace(t *testing.T) {
	err := ValidateResult(Result{
		Status: Counterexample, Termination: Completed, FailedProperty: "safe",
	})
	require.ErrorContains(t, err, "native trace")
}

func TestResultRejectsCounterexampleWithIncompleteTermination(t *testing.T) {
	err := ValidateResult(Result{
		Status: Counterexample, Termination: ToolError, FailedProperty: "safe", NativeTrace: "trace",
	})
	require.ErrorContains(t, err, "completed")
}

func TestResultRejectsEvidenceFailureWithoutDiagnostic(t *testing.T) {
	err := ValidateResult(Result{Status: Inconclusive, Termination: EvidenceFailure})
	require.ErrorContains(t, err, "diagnostic")
}
