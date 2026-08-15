package fizz

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

func TestGeneratedSourceChecksWithFizzBee(t *testing.T) {
	tool := os.Getenv("UMPIRE_FIZZ_TOOL")
	if tool == "" {
		t.Skip("UMPIRE_FIZZ_TOOL is not set")
	}
	files, diagnostics, err := Generate(testModel())
	require.NoError(t, err)
	require.Empty(t, diagnostics)
	directory := t.TempDir()
	for name, contents := range files {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), contents, 0o600))
	}
	config, err := RenderConfig(verify.Bounds{MaxDepth: 4})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "fizz.yaml"), config, 0o600))
	command := exec.Command(tool, "--test", "--copy-ast", "--output-dir", filepath.Join(directory, "native"), "Umpire.fizz")
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "PASSED: Model checker completed successfully")
}

func TestGeneratedFaultProducesFizzBeeCounterexample(t *testing.T) {
	tool := os.Getenv("UMPIRE_FIZZ_TOOL")
	if tool == "" {
		t.Skip("UMPIRE_FIZZ_TOOL is not set")
	}
	model := testModel()
	model.Properties[0].Expr = verify.Expr{Op: verify.FalseExpr}
	files, diagnostics, err := Generate(model)
	require.NoError(t, err)
	require.Empty(t, diagnostics)
	directory := t.TempDir()
	for name, contents := range files {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), contents, 0o600))
	}
	config, err := RenderConfig(verify.Bounds{MaxDepth: 4})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "fizz.yaml"), config, 0o600))
	nativeDirectory := filepath.Join(directory, "native")
	command := exec.Command(tool, "--test", "--copy-ast", "--output-dir", nativeDirectory, "Umpire.fizz")
	command.Dir = directory
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "FAILED: Model checker failed. Invariant:  Property_reciprocal_link")
	require.FileExists(t, filepath.Join(nativeDirectory, "error-graph.json"))
}

func TestGenerateLowersAtomicActionsWithBindingsBranchesAndFrames(t *testing.T) {
	files, diagnostics, err := Generate(testModel())
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	source := string(files["Umpire.fizz"])
	require.Contains(t, source, "atomic action Action_schedule:")
	require.Contains(t, source, "operation = oneof IDs_NexusOperation")
	require.Contains(t, source, "require operation not in exists_NexusOperation")
	require.Contains(t, source, "require state_Workflow[caller] == \"started\"")
	require.Contains(t, source, "next_exists_NexusOperation = set(exists_NexusOperation)")
	require.Contains(t, source, "next_state_Workflow = dict(state_Workflow)")
	require.Contains(t, source, "next_relation_nexus_child_of.add((operation, caller))")
	require.Contains(t, source, "exists_Workflow = next_exists_Workflow")
	require.Contains(t, source, "branch = oneof [0, 1]")
	require.Contains(t, source, "if branch == 0:")
}

func TestGenerateEmitsCardinalitySafetyCanStepAndQuiescence(t *testing.T) {
	files, diagnostics, err := Generate(testModel())
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	source := string(files["Umpire.fizz"])
	require.Contains(t, source, "# CanStep =")
	require.Contains(t, source, "always assertion Relation_nexus_child_of_endpoints:")
	require.Contains(t, source, "always assertion Cardinality_nexus_child_of_source:")
	require.Contains(t, source, "always assertion Property_reciprocal_link:")
	require.Contains(t, source, "always assertion Property_scheduled_quiescent_progress:")
	require.Contains(t, source, "return ((")
	require.Equal(t, strings.TrimRight(source, "\n")+"\n", source)
}

func TestGenerateRequiresFreshBindingsToBeDistinct(t *testing.T) {
	model := testModel()
	model.Actions = append(model.Actions, verify.Action{
		Name: "create-pair",
		Parameters: []verify.Parameter{
			{Name: "left", Type: "NexusOperation", Binding: verify.FreshBinding},
			{Name: "right", Type: "NexusOperation", Binding: verify.FreshBinding},
		},
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: "NexusOperation", Ref: "left", State: "scheduled"},
			{Kind: verify.CreateEffect, Entity: "NexusOperation", Ref: "right", State: "scheduled"},
		},
	})

	files, _, err := Generate(model)
	require.NoError(t, err)
	require.Contains(t, string(files["Umpire.fizz"]), "require left != right")
}

func TestGenerateReportsUnsupportedProgress(t *testing.T) {
	model := testModel()
	model.Properties = append(model.Properties, verify.Property{
		Name: "eventually-finished", Kind: verify.ProgressProperty,
		Expr: verify.Expr{Op: verify.TrueExpr}, Fairness: []string{"weak-finish"},
		Source: verify.Provenance{Path: "protocol.go", Symbol: "EventuallyFinished"},
	})

	files, diagnostics, err := Generate(model)
	require.NoError(t, err)
	require.Equal(t, []Diagnostic{{
		Construct: "property eventually-finished",
		Reason:    "FizzBee semantic generation does not support temporal progress properties",
		Source:    verify.Provenance{Path: "protocol.go", Symbol: "EventuallyFinished"},
	}}, diagnostics)
	require.Contains(t, string(files["Umpire.fizz"]), "# unsupported property eventually-finished")
}

func TestGenerateIsDeterministicAcrossDeclarationOrder(t *testing.T) {
	left := testModel()
	right := testModel()
	right.Entities[0], right.Entities[1] = right.Entities[1], right.Entities[0]

	leftFiles, leftDiagnostics, err := Generate(left)
	require.NoError(t, err)
	rightFiles, rightDiagnostics, err := Generate(right)
	require.NoError(t, err)
	require.Equal(t, leftFiles, rightFiles)
	require.Equal(t, leftDiagnostics, rightDiagnostics)
}

func TestGenerateRejectsIdentifierCollisions(t *testing.T) {
	model := testModel()
	model.Actions = append(model.Actions, verify.Action{Name: "schedule-"}, verify.Action{Name: "schedule!"})

	_, _, err := Generate(model)
	require.ErrorContains(t, err, "normalize to identifier")
}

func TestRenderConfigRequiresExplicitDepthAndDisablesNativeSemantics(t *testing.T) {
	_, err := RenderConfig(verify.Bounds{})
	require.ErrorContains(t, err, "positive max depth")

	config, err := RenderConfig(verify.Bounds{MaxDepth: 7})
	require.NoError(t, err)
	require.Equal(t, "options:\n  max_actions: 7\n  max_concurrent_actions: 1\n  crash_on_yield: false\ndeadlock_detection: false\nliveness: \"false\"\n", string(config))
}

func TestTraceVocabularyMapsGeneratedNames(t *testing.T) {
	vocabulary, err := TraceVocabulary(testModel())
	require.NoError(t, err)
	require.Equal(t, "schedule", vocabulary.Actions["Action_schedule"])
	require.Equal(t, []string{"reciprocal-link"}, vocabulary.Properties["Property_reciprocal_link"])
	require.Equal(t, "NexusOperation", vocabulary.EntityExists["exists_NexusOperation"])
	require.Equal(t, "NexusOperation", vocabulary.EntityStates["state_NexusOperation"])
	require.Equal(t, "nexus-child-of", vocabulary.Relations["relation_nexus_child_of"])
}

func testModel() verify.Model {
	return verify.Model{
		Version: "fizz-test/v1",
		Entities: []verify.EntityType{
			{Name: "Workflow", IDs: []string{"workflow-0"}, InitiallyExists: []string{"workflow-0"}, Initial: "started", States: []verify.State{{Name: "started"}, {Name: "completed", Terminal: true}}},
			{Name: "NexusOperation", IDs: []string{"operation-0", "operation-1"}, Initial: "unused", States: []verify.State{{Name: "unused"}, {Name: "scheduled"}, {Name: "succeeded", Terminal: true}}},
		},
		Relations: []verify.Relation{{Name: "nexus-child-of", Source: "NexusOperation", Target: "Workflow", SourceCardinality: verify.One, TargetCardinality: verify.Many}},
		Actions: []verify.Action{
			{
				Name:       "schedule",
				Parameters: []verify.Parameter{{Name: "operation", Type: "NexusOperation", Binding: verify.FreshBinding}, {Name: "caller", Type: "Workflow", Binding: verify.InputBinding}},
				Guard:      verify.StateIs("Workflow", "caller", "started"),
				Effects:    []verify.Effect{{Kind: verify.CreateEffect, Entity: "NexusOperation", Ref: "operation", State: "scheduled"}, {Kind: verify.AddRelationEffect, Relation: "nexus-child-of", Source: "operation", Target: "caller"}},
				Branches:   []verify.Branch{{Name: "first"}, {Name: "second"}},
			},
		},
		Properties: []verify.Property{
			{Name: "reciprocal-link", Kind: verify.SafetyProperty, Expr: verify.Expr{Op: verify.TrueExpr}},
			{Name: "scheduled-quiescent-progress", Kind: verify.QuiescentProperty, Expr: verify.Expr{Op: verify.TrueExpr}},
		},
	}
}
