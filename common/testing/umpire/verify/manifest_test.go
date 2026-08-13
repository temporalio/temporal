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
		GeneratorVersion: "umpire-genmodels/v1",
		Guarantee:        FiniteExhaustive,
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

	first, err := MarshalManifest(manifest)
	require.NoError(t, err)
	second, err := MarshalManifest(manifest)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestResultRejectsSuccessWhenExplorationHitLimit(t *testing.T) {
	err := ValidateResult(Result{Status: FiniteExhaustive, Termination: StateLimit})
	require.ErrorContains(t, err, "cannot claim")
}
