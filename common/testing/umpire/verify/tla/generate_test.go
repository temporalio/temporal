package tla

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

func TestGeneratedSourceParsesAndChecksWithTLC(t *testing.T) {
	jar := os.Getenv("UMPIRE_TLA_JAR")
	if jar == "" {
		t.Skip("UMPIRE_TLA_JAR is not set")
	}
	java := os.Getenv("UMPIRE_JAVA_TOOL")
	if java == "" {
		java = "java"
	}
	files, err := Generate(testModel())
	require.NoError(t, err)
	directory := t.TempDir()
	for name, contents := range files {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), contents, 0o600))
	}
	parse := exec.Command(java, "-cp", jar, "tla2sany.SANY", "Umpire.tla")
	parse.Dir = directory
	output, err := parse.CombinedOutput()
	require.NoError(t, err, string(output))

	check := exec.Command(java, "-cp", jar, "tlc2.TLC", "-workers", "1", "-config", "Umpire-smoke.cfg", "Umpire.tla")
	check.Dir = directory
	output, err = check.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Model checking completed. No error has been found")
}

func TestGeneratedSourceChecksWithApalache(t *testing.T) {
	tool := os.Getenv("UMPIRE_APALACHE_TOOL")
	if tool == "" {
		t.Skip("UMPIRE_APALACHE_TOOL is not set")
	}
	files, err := Generate(testModel())
	require.NoError(t, err)
	directory := t.TempDir()
	for name, contents := range files {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), contents, 0o600))
	}
	command := exec.Command(tool, "check", "--config=Umpire-smoke.cfg", "--inv=Safety,QuiescentSafety", "--no-deadlock", "--length=5", "Umpire.tla")
	command.Dir = directory
	command.Env = append(os.Environ(), "JAVA_HOME=/opt/homebrew/opt/openjdk@21")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "Checker reports no error")
}

func TestGenerateLowersAtomicActionAndFramesUnchangedState(t *testing.T) {
	files, err := Generate(testModel())
	require.NoError(t, err)

	module := string(files["Umpire.tla"])
	require.Contains(t, module, "CONSTANTS\n    \\* @type: Set(Str);\n    NexusOperationIDs,")
	require.Contains(t, module, "Schedule(operation, caller) ==")
	require.Contains(t, module, `/\ operation \notin exists_NexusOperation`)
	require.Contains(t, module, `/\ state_Workflow[caller] = "started"`)
	require.Contains(t, module, `/\ exists_NexusOperation' = exists_NexusOperation \union {operation}`)
	require.Contains(t, module, `/\ state_NexusOperation' = [state_NexusOperation EXCEPT ![operation] = "scheduled"]`)
	require.Contains(t, module, `/\ relation_nexus_child_of' = relation_nexus_child_of \union {<<operation, caller>>}`)
	require.Contains(t, module, `/\ UNCHANGED <<exists_Workflow, state_Workflow>>`)
}

func TestGenerateEmitsTypeCardinalityAndQuiescentChecks(t *testing.T) {
	model := testModel()
	model.Properties = append(model.Properties,
		verify.Property{Name: "declared-safety", Kind: verify.SafetyProperty, Expr: verify.Expr{Op: verify.TrueExpr}},
		verify.Property{Name: "strengthening-safety", Kind: verify.SafetyProperty, Strengthening: true, Expr: verify.Expr{Op: verify.TrueExpr}},
	)
	files, err := Generate(model)
	require.NoError(t, err)

	module := string(files["Umpire.tla"])
	require.Contains(t, module, `relation_nexus_child_of \in SUBSET (NexusOperationIDs \X WorkflowIDs)`)
	require.Contains(t, module, `Cardinality_nexus_child_of ==`)
	require.Contains(t, module, `tuple[1] \in exists_NexusOperation /\ tuple[2] \in exists_Workflow`)
	require.Contains(t, module, `\A source \in NexusOperationIDs: Cardinality({target \in WorkflowIDs: <<source, target>> \in relation_nexus_child_of}) <= 1`)
	require.Contains(t, module, `NexusOperation_scheduled_quiescent_progress ==`)
	require.Contains(t, module, "InductiveInvariant ==")
	require.Contains(t, module, "    /\\ declared_safety")
	require.Contains(t, module, "    /\\ strengthening_safety")
	require.Contains(t, module, "DeclaredSafety ==")
	require.Contains(t, module, "Safety == InductiveInvariant /\\ DeclaredSafety")
	require.Contains(t, module, `QuiescentSafety == CanStep \/ NexusOperation_scheduled_quiescent_progress`)

	config := string(files["Umpire-smoke.cfg"])
	require.Contains(t, config, `NexusOperationIDs = {"operation-0", "operation-1"}`)
	require.Contains(t, config, "INVARIANT TypeOK")
	require.Contains(t, config, "INVARIANT Cardinality_nexus_child_of")
	require.Contains(t, config, "CHECK_DEADLOCK FALSE")
	require.Contains(t, config, "INVARIANT QuiescentSafety")
	require.Equal(t, config, string(files["Umpire-nightly.cfg"]))
}

func TestGenerateIsDeterministicAcrossDeclarationOrder(t *testing.T) {
	left := testModel()
	right := testModel()
	right.Entities[0], right.Entities[1] = right.Entities[1], right.Entities[0]

	leftFiles, err := Generate(left)
	require.NoError(t, err)
	rightFiles, err := Generate(right)
	require.NoError(t, err)
	require.Equal(t, leftFiles, rightFiles)
}

func TestTraceVocabularyMapsGeneratedNamesToCanonicalModel(t *testing.T) {
	vocabulary, err := TraceVocabulary(testModel())
	require.NoError(t, err)

	require.Equal(t, "schedule", vocabulary.Actions["Schedule"])
	require.Equal(t, []string{"NexusOperation.scheduled.quiescent-progress"}, vocabulary.Properties["NexusOperation_scheduled_quiescent_progress"])
	require.Equal(t, []string{
		"relation nexus-child-of endpoints",
		"relation nexus-child-of source cardinality",
	}, vocabulary.Properties["Cardinality_nexus_child_of"])
	require.Equal(t, "NexusOperation", vocabulary.EntityExists["exists_NexusOperation"])
	require.Equal(t, "NexusOperation", vocabulary.EntityStates["state_NexusOperation"])
	require.Equal(t, "nexus-child-of", vocabulary.Relations["relation_nexus_child_of"])
	require.Equal(t, map[string]string{
		"operation": "operation",
		"caller":    "caller",
	}, vocabulary.Bindings["Schedule"])
	require.Equal(t, []string{
		"relation nexus-child-of endpoints",
		"relation nexus-child-of source cardinality",
	}, vocabulary.Properties["Safety"])
	require.Equal(t, []string{"NexusOperation.scheduled.quiescent-progress"}, vocabulary.Properties["QuiescentSafety"])
}

func TestTraceVocabularyDoesNotDuplicateAggregateSafetyProperties(t *testing.T) {
	model := testModel()
	model.Properties[0].Kind = verify.SafetyProperty

	vocabulary, err := TraceVocabulary(model)
	require.NoError(t, err)
	require.Equal(t, []string{
		"relation nexus-child-of endpoints",
		"relation nexus-child-of source cardinality",
		"NexusOperation.scheduled.quiescent-progress",
	}, vocabulary.Properties["Safety"])
}

func TestGenerateAndTraceVocabularyRejectParameterIdentifierCollisions(t *testing.T) {
	model := testModel()
	model.Actions = append(model.Actions, verify.Action{
		Name: "collide",
		Parameters: []verify.Parameter{
			{Name: "workflow-ref", Type: "Workflow", Binding: verify.InputBinding},
			{Name: "workflow_ref", Type: "Workflow", Binding: verify.InputBinding},
		},
	})

	_, generateErr := Generate(model)
	require.ErrorContains(t, generateErr, "normalize to parameter identifier")
	_, vocabularyErr := TraceVocabulary(model)
	require.ErrorContains(t, vocabularyErr, "normalize to parameter identifier")
}

func testModel() verify.Model {
	return verify.Model{
		Version: "test/v1",
		Entities: []verify.EntityType{
			{
				Name:            "Workflow",
				IDs:             []string{"workflow-0"},
				InitiallyExists: []string{"workflow-0"},
				Initial:         "started",
				States:          []verify.State{{Name: "started"}, {Name: "completed", Terminal: true}},
			},
			{
				Name:    "NexusOperation",
				IDs:     []string{"operation-0", "operation-1"},
				Initial: "unspecified",
				States:  []verify.State{{Name: "unspecified"}, {Name: "scheduled", MustProgress: true}, {Name: "succeeded", Terminal: true}},
			},
		},
		Relations: []verify.Relation{{
			Name:              "nexus-child-of",
			Source:            "NexusOperation",
			Target:            "Workflow",
			SourceCardinality: verify.One,
			TargetCardinality: verify.Many,
		}},
		Actions: []verify.Action{
			{
				Name: "schedule",
				Parameters: []verify.Parameter{
					{Name: "operation", Type: "NexusOperation", Binding: verify.FreshBinding},
					{Name: "caller", Type: "Workflow", Binding: verify.InputBinding},
				},
				Guard: verify.StateIs("Workflow", "caller", "started"),
				Effects: []verify.Effect{
					{Kind: verify.CreateEffect, Entity: "NexusOperation", Ref: "operation", State: "scheduled"},
					{Kind: verify.AddRelationEffect, Relation: "nexus-child-of", Source: "operation", Target: "caller"},
				},
			},
			{
				Name:       "finish",
				Parameters: []verify.Parameter{{Name: "operation", Type: "NexusOperation", Binding: verify.InputBinding}},
				Guard:      verify.StateIs("NexusOperation", "operation", "scheduled"),
				Effects:    []verify.Effect{{Kind: verify.SetStateEffect, Entity: "NexusOperation", Ref: "operation", State: "succeeded"}},
			},
		},
		Properties: []verify.Property{{
			Name: "NexusOperation.scheduled.quiescent-progress",
			Kind: verify.QuiescentProperty,
			Expr: verify.Expr{
				Op:     verify.ForAllExpr,
				Entity: "NexusOperation",
				Var:    "operation",
				Args:   []verify.Expr{verify.Not(verify.StateIs("NexusOperation", "operation", "scheduled"))},
			},
		}},
	}
}
