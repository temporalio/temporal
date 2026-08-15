package p

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

func TestGeneratedSourceCompilesWithP(t *testing.T) {
	tool := os.Getenv("UMPIRE_P_TOOL")
	if tool == "" {
		t.Skip("UMPIRE_P_TOOL is not set")
	}
	files, err := Generate(testModel())
	require.NoError(t, err)
	directory := t.TempDir()
	for name, contents := range files {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), contents, 0o600))
	}
	for _, mode := range []string{"bugfinding", "pex"} {
		t.Run(mode, func(t *testing.T) {
			command := exec.Command(tool, "compile", "--pproj", filepath.Join(directory, "Umpire.pproj"), "--mode", mode)
			command.Dir = directory
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}
}

func TestGeneratePreservesAtomicKernel(t *testing.T) {
	files, err := Generate(testModel())
	require.NoError(t, err)

	source := string(files["Umpire.p"])
	require.Contains(t, source, "machine UmpireWorld")
	require.Contains(t, source, "var checkerStep: int;")
	require.Contains(t, source, "fun Apply_create_operation_0_workflow_0()")
	require.Contains(t, source, "exists_operation += (operation_0);")
	require.Contains(t, source, "relation_owns += ((source = workflow_0, target = operation_0));")
	require.Contains(t, source, "assert !((source = workflow_0, target = operation_0) in relation_owns")
	require.Contains(t, source, "|| (workflow_0 in exists_workflow && operation_0 in exists_operation)")
	require.Contains(t, source, "send this, eStep;")
	require.Contains(t, source, "checkerStep = checkerStep + 1;")
	require.NotContains(t, source, "raise eStep;")
	require.NotContains(t, source, "choose(enabled)")
	require.Contains(t, source, "type tSelection = (chosen: int, remaining: set[int]);")
	require.Contains(t, source, "fun EnabledChunk_0(enabled: set[int]): set[int]")
	require.Contains(t, source, "fun SelectChunk_0(enabled: set[int]): tSelection")
	require.Contains(t, source, "fun ApplyChunk_0(selected: int)")
	require.Contains(t, source, "test tcUmpire [main=UmpireWorld]")
	require.Contains(t, string(files["Umpire.pproj"]), "<Target>PChecker,PEx</Target>")
}

func TestGenerateExpandsBranchesAndQuiescentProperties(t *testing.T) {
	model := testModel()
	model.Properties = append(model.Properties, verify.Property{
		Name: "operation-safety", Kind: verify.SafetyProperty, Expr: verify.Expr{Op: verify.TrueExpr},
	})
	model.Actions[0].Branches = []verify.Branch{
		{Name: "success", Effects: []verify.Effect{{Kind: verify.SetStateEffect, Entity: "operation", Ref: "operation", State: "done"}}},
		{Name: "failure", Effects: []verify.Effect{{Kind: verify.SetStateEffect, Entity: "operation", Ref: "operation", State: "failed"}}},
	}
	files, err := Generate(model)
	require.NoError(t, err)

	source := string(files["Umpire.p"])
	require.Contains(t, source, "if ($) {")
	require.Contains(t, source, "fun CheckRelation_0()")
	require.Contains(t, source, "fun CheckProperty_0()")
	require.Contains(t, source, "fun CheckQuiescent()")
	require.Contains(t, source, "fun CheckQuiescentProperty_0()")
	require.Contains(t, source, "operation_0 in exists_operation")
}

func TestGenerateRequiresSameTypeFreshParametersToBeDistinct(t *testing.T) {
	model := testModel()
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

	files, err := Generate(model)
	require.NoError(t, err)
	require.Contains(t, string(files["Umpire.p"]), "operation_0 != operation_1")
}

func TestGenerateIsDeterministic(t *testing.T) {
	model := testModel()
	reversed := model
	reversed.Entities = []verify.EntityType{model.Entities[1], model.Entities[0]}

	left, err := Generate(model)
	require.NoError(t, err)
	right, err := Generate(reversed)
	require.NoError(t, err)
	require.Equal(t, left, right)
}

func testModel() verify.Model {
	return verify.Model{
		Version: "test/v1",
		Entities: []verify.EntityType{
			{Name: "workflow", IDs: []string{"workflow#0"}, InitiallyExists: []string{"workflow#0"}, Initial: "running", States: []verify.State{{Name: "running"}}},
			{Name: "operation", IDs: []string{"operation#0", "operation#1"}, Initial: "unspecified", States: []verify.State{{Name: "unspecified"}, {Name: "scheduled"}, {Name: "done"}, {Name: "failed"}}},
		},
		Relations: []verify.Relation{{Name: "owns", Source: "workflow", Target: "operation", SourceCardinality: verify.One, TargetCardinality: verify.One}},
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
			Name: "operation-eventually-leaves-scheduled",
			Kind: verify.QuiescentProperty,
			Expr: verify.Expr{Op: verify.ForAllExpr, Entity: "operation", Var: "operation", Args: []verify.Expr{verify.Not(verify.StateIs("operation", "operation", "scheduled"))}},
		}},
	}
}
