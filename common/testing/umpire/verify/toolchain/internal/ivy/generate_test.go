package ivy

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

func TestGeneratedSourceChecksWithIvy(t *testing.T) {
	tool := os.Getenv("UMPIRE_IVY_TOOL")
	if tool == "" {
		t.Skip("UMPIRE_IVY_TOOL is not set")
	}
	model := testModel()
	model.Properties = append(model.Properties, verify.Property{
		Name: "existential-syntax",
		Kind: verify.SafetyProperty,
		Expr: verify.Expr{Op: verify.ImpliesExpr, Args: []verify.Expr{
			{Op: verify.ExistsExpr, Entity: "operation", Var: "candidate", Args: []verify.Expr{{Op: verify.EntityExistsExpr, Entity: "operation", Ref: "candidate"}}},
			{Op: verify.TrueExpr},
		}},
	})
	files, diagnostics, err := Generate(model)
	require.NoError(t, err)
	require.Empty(t, diagnostics)
	path := filepath.Join(t.TempDir(), "Umpire.ivy")
	require.NoError(t, os.WriteFile(path, files["Umpire.ivy"], 0o600))
	command := exec.Command(tool, path)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestGenerateProducesTypedRelationalModel(t *testing.T) {
	files, diagnostics, err := Generate(testModel())
	require.NoError(t, err)
	require.Empty(t, diagnostics)

	source := string(files["Umpire.ivy"])
	require.Contains(t, source, "#lang ivy1.7")
	require.Contains(t, source, "type operation")
	require.Contains(t, source, "type operation_state = {operation_state_done,operation_state_scheduled,operation_state_unspecified}")
	require.Contains(t, source, "relation relation_owns(X:workflow, Y:operation)")
	require.Contains(t, source, "action create(operation:operation,workflow:workflow) = {")
	require.Contains(t, source, "require ~exists_operation(operation)")
	require.Contains(t, source, "relation_owns(workflow,operation) := true")
	require.Contains(t, source, "invariant [cardinality_owns_target]")
	require.Contains(t, source, "invariant [owned_operations_exist]")
	require.Contains(t, source, "export create")
}

func TestGenerateReportsUnsupportedPropertyKinds(t *testing.T) {
	model := testModel()
	model.Properties = append(model.Properties, verify.Property{
		Name: "quiescent-progress",
		Kind: verify.QuiescentProperty,
		Expr: verify.Expr{Op: verify.TrueExpr},
	})

	files, diagnostics, err := Generate(model)
	require.NoError(t, err)
	require.Equal(t, []Diagnostic{{Construct: "property quiescent-progress", Reason: "Ivy generation supports inductive safety properties only"}}, diagnostics)
	require.Contains(t, string(files["Umpire.ivy"]), "# unsupported property quiescent-progress")
}

func TestGenerateReportsUnsupportedProgressProperties(t *testing.T) {
	model := testModel()
	model.Properties = append(model.Properties, verify.Property{
		Name:     "eventual-progress",
		Kind:     verify.ProgressProperty,
		Expr:     verify.Expr{Op: verify.TrueExpr},
		Fairness: []string{"weak-create"},
	})

	files, diagnostics, err := Generate(model)
	require.NoError(t, err)
	require.Equal(t, []Diagnostic{{Construct: "property eventual-progress", Reason: "Ivy generation supports inductive safety properties only"}}, diagnostics)
	require.Contains(t, string(files["Umpire.ivy"]), "# unsupported property eventual-progress")
}

func TestGeneratePreservesNondeterministicBranches(t *testing.T) {
	model := testModel()
	model.Actions[0].Branches = []verify.Branch{
		{Name: "scheduled", Effects: nil},
		{Name: "done", Effects: []verify.Effect{{Kind: verify.SetStateEffect, Entity: "operation", Ref: "operation", State: "done"}}},
	}

	files, _, err := Generate(model)
	require.NoError(t, err)
	require.Contains(t, string(files["Umpire.ivy"]), "if * {")
}

func TestGenerateRequiresSameTypeFreshParametersToBeDistinct(t *testing.T) {
	model := testModel()
	model.Entities[1].IDs = append(model.Entities[1].IDs, "operation#1")
	model.Actions = append(model.Actions, verify.Action{
		Name: "create-pair",
		Parameters: []verify.Parameter{
			{Name: "left", Type: "operation", Binding: verify.FreshBinding},
			{Name: "right", Type: "operation", Binding: verify.FreshBinding},
		},
		Effects: []verify.Effect{
			{Kind: verify.CreateEffect, Entity: "operation", Ref: "left", State: "scheduled"},
			{Kind: verify.CreateEffect, Entity: "operation", Ref: "right", State: "scheduled"},
		},
	})

	files, _, err := Generate(model)
	require.NoError(t, err)
	require.Contains(t, string(files["Umpire.ivy"]), "require left ~= right;")
}

func TestTraceVocabularyMapsGeneratedNamesToCanonicalModel(t *testing.T) {
	vocabulary, err := TraceVocabulary(testModel())
	require.NoError(t, err)

	require.Equal(t, "create", vocabulary.Actions["create"])
	require.Equal(t, []string{"owned-operations-exist"}, vocabulary.Properties["owned_operations_exist"])
	require.Equal(t, []string{"relation owns target cardinality"}, vocabulary.Properties["cardinality_owns_target"])
	require.Equal(t, "operation", vocabulary.EntityExists["exists_operation"])
	require.Equal(t, "operation", vocabulary.EntityStates["state_operation"])
	require.Equal(t, "owns", vocabulary.Relations["relation_owns"])
	require.Equal(t, "operation#0", vocabulary.Identities["operation_0"])
	require.Equal(t, "scheduled", vocabulary.States["operation_state_scheduled"])
	require.Equal(t, map[string]string{
		"operation": "operation",
		"workflow":  "workflow",
	}, vocabulary.Bindings["create"])
}

func TestGenerateAndTraceVocabularyRejectIdentityCollisions(t *testing.T) {
	model := testModel()
	model.Entities[1].IDs = []string{"operation-0", "operation_0"}

	_, _, generateErr := Generate(model)
	require.ErrorContains(t, generateErr, "normalize to identifier")
	_, vocabularyErr := TraceVocabulary(model)
	require.ErrorContains(t, vocabularyErr, "normalize to identifier")
}

func TestGenerateAndTraceVocabularyRejectParameterIdentifierCollisions(t *testing.T) {
	model := testModel()
	model.Actions = append(model.Actions, verify.Action{
		Name: "collide",
		Parameters: []verify.Parameter{
			{Name: "workflow-ref", Type: "workflow", Binding: verify.InputBinding},
			{Name: "workflow_ref", Type: "workflow", Binding: verify.InputBinding},
		},
	})

	_, _, generateErr := Generate(model)
	require.ErrorContains(t, generateErr, "normalize to parameter identifier")
	_, vocabularyErr := TraceVocabulary(model)
	require.ErrorContains(t, vocabularyErr, "normalize to parameter identifier")
}

func testModel() verify.Model {
	return verify.Model{
		Version: "test/v1",
		Entities: []verify.EntityType{
			{Name: "workflow", IDs: []string{"workflow#0"}, InitiallyExists: []string{"workflow#0"}, Initial: "running", States: []verify.State{{Name: "running"}}},
			{Name: "operation", IDs: []string{"operation#0"}, Initial: "unspecified", States: []verify.State{{Name: "unspecified"}, {Name: "scheduled"}, {Name: "done"}}},
		},
		Relations: []verify.Relation{{Name: "owns", Source: "workflow", Target: "operation", SourceCardinality: verify.Many, TargetCardinality: verify.One}},
		Actions: []verify.Action{{
			Name: "create",
			Parameters: []verify.Parameter{
				{Name: "operation", Type: "operation", Binding: verify.FreshBinding},
				{Name: "workflow", Type: "workflow", Binding: verify.InputBinding},
			},
			Guard: verify.StateIs("workflow", "workflow", "running"),
			Effects: []verify.Effect{
				{Kind: verify.CreateEffect, Entity: "operation", Ref: "operation", State: "scheduled"},
				{Kind: verify.AddRelationEffect, Relation: "owns", Source: "workflow", Target: "operation"},
			},
		}},
		Properties: []verify.Property{{
			Name: "owned-operations-exist",
			Kind: verify.SafetyProperty,
			Expr: verify.Expr{Op: verify.ForAllExpr, Entity: "workflow", Var: "w", Args: []verify.Expr{{
				Op: verify.ForAllExpr, Entity: "operation", Var: "o", Args: []verify.Expr{{
					Op: verify.ImpliesExpr,
					Args: []verify.Expr{
						{Op: verify.RelationHasExpr, Relation: "owns", Source: "w", Target: "o"},
						{Op: verify.EntityExistsExpr, Entity: "operation", Ref: "o"},
					},
				}},
			}}},
		}},
	}
}
