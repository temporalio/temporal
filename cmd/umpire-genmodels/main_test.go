package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/common/testing/umpire/verify/ivy"
	pgenerator "go.temporal.io/server/common/testing/umpire/verify/p"
	"go.temporal.io/server/common/testing/umpire/verify/runner"
	"go.temporal.io/server/common/testing/umpire/verify/tla"
)

func TestGenerateWritesCompleteDeterministicArtifactSet(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, generate(directory, 1))

	names := []string{
		"manifest.json",
		"model.ir.json",
		"tla/Umpire.tla",
		"tla/Umpire-smoke.cfg",
		"tla/Umpire-nightly.cfg",
		"p/Umpire.p",
		"p/Umpire.pproj",
		"ivy/Umpire.ivy",
	}
	first := map[string][]byte{}
	for _, name := range names {
		contents, err := os.ReadFile(filepath.Join(directory, name))
		require.NoError(t, err, name)
		require.NotEmpty(t, contents, name)
		first[name] = contents
	}
	require.NoError(t, generate(directory, 1))
	for _, name := range names {
		contents, err := os.ReadFile(filepath.Join(directory, name))
		require.NoError(t, err, name)
		require.Equal(t, first[name], contents, name)
	}
}

func TestCheckGeneratedRejectsUnexpectedArtifact(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, generate(directory, 1))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "stale.txt"), []byte("stale"), 0o600))

	err := checkGenerated(directory, 1)
	require.ErrorContains(t, err, "unexpected stale.txt")
}

func TestRunnerRequestUsesBackendSpecificApalacheDepth(t *testing.T) {
	request, err := runnerRequest(runner.Apalache, checkOptions{
		output: "/models", artifacts: "/artifacts", profile: "smoke", apalacheTool: "/tools/apalache",
	}, verify.Bounds{MaxDepth: 100, Schedules: 100}, verify.Model{})
	require.NoError(t, err)
	require.Equal(t, uint64(5), request.Bounds.MaxDepth)
}

func TestRunnerRequestCapsPExDepthAtNativeChoiceLimit(t *testing.T) {
	request, err := runnerRequest(runner.PEx, checkOptions{
		output: "/models", artifacts: "/artifacts", profile: "nightly", pTool: "/tools/p",
	}, verify.Bounds{MaxDepth: 1_000, Schedules: 10_000}, verify.Model{})
	require.NoError(t, err)
	require.Equal(t, uint64(100), request.Bounds.MaxDepth)
	require.Equal(t, uint64(10_000), request.Bounds.Schedules)
}

func TestRequestedBackendsRunsInductiveProofOnlyForNightlyAll(t *testing.T) {
	smoke, err := requestedBackends("all", "smoke")
	require.NoError(t, err)
	require.NotContains(t, smoke, runner.ApalacheProof)

	nightly, err := requestedBackends("all", "nightly")
	require.NoError(t, err)
	require.Contains(t, nightly, runner.ApalacheProof)

	explicit, err := requestedBackends("apalache-proof", "smoke")
	require.NoError(t, err)
	require.Equal(t, []runner.Backend{runner.ApalacheProof}, explicit)
}

func TestGeneratorsCoverEverySourceActionAndProperty(t *testing.T) {
	model, err := verificationModel(1)
	require.NoError(t, err)
	tlaFiles, err := tla.Generate(model)
	require.NoError(t, err)
	pFiles, err := pgenerator.Generate(model)
	require.NoError(t, err)
	ivyFiles, diagnostics, err := ivy.Generate(model)
	require.NoError(t, err)

	tlaSource := string(tlaFiles["Umpire.tla"])
	pSource := string(pFiles["Umpire.p"])
	ivySource := string(ivyFiles["Umpire.ivy"])
	for _, action := range model.Actions {
		require.Contains(t, tlaSource, tla.ActionIdentifier(action.Name)+"Enabled", action.Name)
		require.Equal(t, 1, strings.Count(pSource, "UMPIRE_ACTION "+action.Name+" "), action.Name)
		require.Contains(t, ivySource, "action "+ivy.ActionIdentifier(action.Name)+"(", action.Name)
	}
	for _, property := range model.Properties {
		require.Contains(t, tlaSource, tla.PropertyIdentifier(property.Name)+" ==", property.Name)
		if property.Kind == verify.SafetyProperty {
			require.Contains(t, pSource, "property "+property.Name+" failed", property.Name)
			require.Contains(t, ivySource, "["+ivy.PropertyIdentifier(property.Name)+"]", property.Name)
		} else {
			require.Contains(t, ivySource, "# unsupported property "+property.Name, property.Name)
		}
	}
	require.Len(t, diagnostics, len(model.Properties)-countSafetyProperties(model))
}

func countSafetyProperties(model verify.Model) int {
	count := 0
	for _, property := range model.Properties {
		if property.Kind == verify.SafetyProperty {
			count++
		}
	}
	return count
}

func TestSeededSafetyBugIsFoundByInterpreter(t *testing.T) {
	interpreter, err := verify.NewInterpreter(seededBugModel())
	require.NoError(t, err)
	exploration, err := interpreter.Explore(1)
	require.NoError(t, err)
	require.True(t, exploration.Complete)
	require.Equal(t, []verify.PropertyViolation{{State: 1, Property: "reciprocal-link"}}, exploration.Violations)

	states, err := interpreter.Replay([]verify.TraceStep{{Action: "seed.bug", Bindings: verify.Bindings{"target": "target#0", "source": "source#0"}}})
	require.NoError(t, err)
	require.Len(t, states, 1)
}

func TestSeededSafetyBugIsFoundByTLC(t *testing.T) {
	tool := os.Getenv("UMPIRE_TLA_JAR")
	if tool == "" {
		t.Skip("UMPIRE_TLA_JAR is not set")
	}
	directory := writeSeededArtifacts(t)
	result, err := runner.Check(context.Background(), runner.Request{
		Backend: runner.TLC, ToolPath: tool, JavaPath: os.Getenv("UMPIRE_JAVA_TOOL"), ToolVersion: "1.7.4",
		ModelDir: filepath.Join(directory, "tla"), ArtifactDir: filepath.Join(directory, "results"), Timeout: 2 * time.Minute,
		ActionNames: map[string]string{tla.ActionIdentifier("seed.bug"): "seed.bug"},
	})
	require.NoError(t, err)
	requireSeededCounterexample(t, result)
}

func TestInterpreterAndTLCReachSameBoundedStateCount(t *testing.T) {
	tool := os.Getenv("UMPIRE_TLA_JAR")
	if tool == "" {
		t.Skip("UMPIRE_TLA_JAR is not set")
	}
	model := seededBugModel()
	model.Actions[0].Effects = append(model.Actions[0].Effects, verify.Effect{
		Kind: verify.AddRelationEffect, Relation: "reverse", Source: "target", Target: "source",
	})
	interpreter, err := verify.NewInterpreter(model)
	require.NoError(t, err)
	exploration, err := interpreter.Explore(2)
	require.NoError(t, err)
	require.True(t, exploration.Complete)
	require.Empty(t, exploration.Violations)

	directory := t.TempDir()
	files, err := tla.Generate(model)
	require.NoError(t, err)
	require.NoError(t, writeFiles(directory, files))
	result, err := runner.Check(context.Background(), runner.Request{
		Backend: runner.TLC, ToolPath: tool, JavaPath: os.Getenv("UMPIRE_JAVA_TOOL"), ToolVersion: "1.7.4",
		ModelDir: directory, ArtifactDir: filepath.Join(directory, "results"), Timeout: 2 * time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, verify.FiniteExhaustive, result.Status, result.StandardOutput+result.StandardError)
	require.Equal(t, uint64(len(exploration.States)), result.DistinctStates)
}

func TestSeededSafetyBugIsFoundByPAndPEx(t *testing.T) {
	tool := os.Getenv("UMPIRE_P_TOOL")
	if tool == "" {
		t.Skip("UMPIRE_P_TOOL is not set")
	}
	for _, backend := range []runner.Backend{runner.P, runner.PEx} {
		t.Run(string(backend), func(t *testing.T) {
			directory := writeSeededArtifacts(t)
			result, err := runner.Check(context.Background(), runner.Request{
				Backend: backend, ToolPath: tool, JavaPath: os.Getenv("UMPIRE_JAVA_TOOL"), ToolVersion: "3.1.0",
				ModelDir: filepath.Join(directory, "p"), ArtifactDir: filepath.Join(directory, "results"), Timeout: 2 * time.Minute,
				Bounds: verify.Bounds{MaxDepth: 20, Schedules: 20},
			})
			require.NoError(t, err)
			requireSeededCounterexample(t, result)
		})
	}
}

func TestSeededSafetyBugIsRejectedByIvy(t *testing.T) {
	tool := os.Getenv("UMPIRE_IVY_TOOL")
	if tool == "" {
		t.Skip("UMPIRE_IVY_TOOL is not set")
	}
	directory := writeSeededArtifacts(t)
	result, err := runner.Check(context.Background(), runner.Request{
		Backend: runner.Ivy, ToolPath: tool, ToolVersion: "1.8.26",
		ModelDir: filepath.Join(directory, "ivy"), ArtifactDir: filepath.Join(directory, "results"), Timeout: 2 * time.Minute,
		ActionNames: map[string]string{ivy.ActionIdentifier("seed.bug"): "seed.bug"},
	})
	require.NoError(t, err)
	requireSeededCounterexample(t, result)
}

func writeSeededArtifacts(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	model := seededBugModel()
	files := map[string][]byte{}
	tlaFiles, err := tla.Generate(model)
	require.NoError(t, err)
	mergeFiles(files, "tla", tlaFiles)
	pFiles, err := pgenerator.Generate(model)
	require.NoError(t, err)
	mergeFiles(files, "p", pFiles)
	ivyFiles, diagnostics, err := ivy.Generate(model)
	require.NoError(t, err)
	require.Empty(t, diagnostics)
	mergeFiles(files, "ivy", ivyFiles)
	require.NoError(t, writeFiles(directory, files))
	return directory
}

func requireSeededCounterexample(t *testing.T, result verify.Result) {
	t.Helper()
	require.Equal(t, verify.Counterexample, result.Status, result.StandardOutput+result.StandardError)
	require.Contains(t, result.FailedProperty, "reciprocal")
	require.NotEmpty(t, result.Trace, result.StandardOutput+result.StandardError)
	require.Equal(t, "seed.bug", result.Trace[len(result.Trace)-1].Action)
}

func seededBugModel() verify.Model {
	return verify.Model{
		Version: "umpire-verification-seeded-bug/v1",
		Entities: []verify.EntityType{
			{Name: "source", IDs: []string{"source#0"}, InitiallyExists: []string{"source#0"}, Initial: "available", States: []verify.State{{Name: "available"}}},
			{Name: "target", IDs: []string{"target#0"}, Initial: "unused", States: []verify.State{{Name: "unused"}, {Name: "created"}}},
		},
		Relations: []verify.Relation{
			{Name: "forward", Source: "source", Target: "target", SourceCardinality: verify.Many, TargetCardinality: verify.Many},
			{Name: "reverse", Source: "target", Target: "source", SourceCardinality: verify.Many, TargetCardinality: verify.Many},
		},
		Actions: []verify.Action{{
			Name:       "seed.bug",
			Parameters: []verify.Parameter{{Name: "source", Type: "source", Binding: verify.InputBinding}, {Name: "target", Type: "target", Binding: verify.FreshBinding}},
			Effects: []verify.Effect{
				{Kind: verify.CreateEffect, Entity: "target", Ref: "target", State: "created"},
				{Kind: verify.AddRelationEffect, Relation: "forward", Source: "source", Target: "target"},
			},
		}},
		Properties: []verify.Property{{
			Name: "reciprocal-link",
			Kind: verify.SafetyProperty,
			Expr: verify.Expr{Op: verify.ForAllExpr, Entity: "source", Var: "source", Args: []verify.Expr{{
				Op: verify.ForAllExpr, Entity: "target", Var: "target", Args: []verify.Expr{{
					Op: verify.ImpliesExpr,
					Args: []verify.Expr{
						{Op: verify.RelationHasExpr, Relation: "forward", Source: "source", Target: "target"},
						{Op: verify.RelationHasExpr, Relation: "reverse", Source: "target", Target: "source"},
					},
				}},
			}}},
		}},
	}
}
