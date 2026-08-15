package runner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/common/testing/umpire/verify/fizz"
	"go.temporal.io/server/common/testing/umpire/verify/ivy"
	"go.temporal.io/server/common/testing/umpire/verify/tla"
)

func TestToolchainPlanSelectsCompatibleBackends(t *testing.T) {
	requests, err := testToolchain().Plan(verify.Model{}, PlanOptions{
		ModelRoot: "/models", ArtifactRoot: "/artifacts", Target: "delivery",
		Backends: "all", Profile: "nightly", Requirements: []string{"tla", "ivy"},
	})
	require.NoError(t, err)
	require.Equal(t, []Backend{SANY, TLC, Apalache, Ivy, ApalacheProof}, requestBackends(requests))

	_, err = testToolchain().Plan(verify.Model{}, PlanOptions{
		Backends: "p", Profile: "smoke", Requirements: []string{"tla"},
	})
	require.ErrorContains(t, err, "none of the requested backends satisfy target requirements")
}

func TestToolchainPlanUsesPinnedVersionsAndBackendDirectories(t *testing.T) {
	requests, err := testToolchain().Plan(verify.Model{}, PlanOptions{
		ModelRoot: "/models", ArtifactRoot: "/artifacts", Target: "protocol-atomic",
		Backends: "sany,tlc,apalache,apalache-proof,p,pex,ivy", Profile: "smoke", Timeout: time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, []Request{
		{Backend: SANY, Target: "protocol-atomic", Profile: "smoke", ToolPath: "/tools/tla.jar", JavaPath: "/tools/java", ToolVersion: "1.7.4", ModelDir: "/models/protocol-atomic/tla", ArtifactDir: "/artifacts/protocol-atomic/smoke", Timeout: time.Minute, Bounds: verify.Bounds{MaxDepth: 100, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{}},
		{Backend: TLC, Target: "protocol-atomic", Profile: "smoke", ToolPath: "/tools/tla.jar", JavaPath: "/tools/java", ToolVersion: "1.7.4", ModelDir: "/models/protocol-atomic/tla", ArtifactDir: "/artifacts/protocol-atomic/smoke", Timeout: time.Minute, Bounds: verify.Bounds{MaxDepth: 100, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{}},
		{Backend: Apalache, Target: "protocol-atomic", Profile: "smoke", ToolPath: "/tools/apalache", JavaPath: "/tools/java", ToolVersion: "0.61.0", ModelDir: "/models/protocol-atomic/tla", ArtifactDir: "/artifacts/protocol-atomic/smoke", Timeout: time.Minute, Bounds: verify.Bounds{MaxDepth: 5, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{}},
		{Backend: ApalacheProof, Target: "protocol-atomic", Profile: "smoke", ToolPath: "/tools/apalache", JavaPath: "/tools/java", ToolVersion: "0.61.0", ModelDir: "/models/protocol-atomic/tla", ArtifactDir: "/artifacts/protocol-atomic/smoke", Timeout: time.Minute, Bounds: verify.Bounds{MaxDepth: 100, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{}},
		{Backend: P, Target: "protocol-atomic", Profile: "smoke", ToolPath: "/tools/p", JavaPath: "/tools/java", ToolVersion: "3.1.0", ModelDir: "/models/protocol-atomic/p", ArtifactDir: "/artifacts/protocol-atomic/smoke", Timeout: time.Minute, Bounds: verify.Bounds{MaxDepth: 100, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{}},
		{Backend: PEx, Target: "protocol-atomic", Profile: "smoke", ToolPath: "/tools/p", JavaPath: "/tools/java", ToolVersion: "3.1.0", ModelDir: "/models/protocol-atomic/p", ArtifactDir: "/artifacts/protocol-atomic/smoke", Timeout: time.Minute, Bounds: verify.Bounds{MaxDepth: 100, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{}},
		{Backend: Ivy, Target: "protocol-atomic", Profile: "smoke", ToolPath: "/tools/ivy", JavaPath: "/tools/java", ToolVersion: "1.8.26", ModelDir: "/models/protocol-atomic/ivy", ArtifactDir: "/artifacts/protocol-atomic/smoke", Timeout: time.Minute, Bounds: verify.Bounds{MaxDepth: 100, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{}},
	}, requests)
}

func TestToolchainPlanScopesUnsupportedSemanticsToBackend(t *testing.T) {
	model := verify.Model{
		Actions: []verify.Action{{Name: "delivery.schedule"}},
		Properties: []verify.Property{{
			Name: "delivery.progress", Kind: verify.ProgressProperty,
			Fairness: []string{"weak-schedule"}, Source: verify.Provenance{Path: "delivery.go"},
		}},
		Abstractions: []verify.Abstraction{{Name: "environment", Reason: "unrealized"}},
	}
	requests, err := testToolchain().Plan(model, PlanOptions{Backends: "tlc,p,ivy", Profile: "smoke"})
	require.NoError(t, err)
	require.Len(t, requests, 3)
	require.Equal(t, map[string]string{
		tla.ActionIdentifier("delivery.schedule"):  "delivery.schedule",
		ivy.ActionIdentifier("delivery.schedule"):  "delivery.schedule",
		fizz.ActionIdentifier("delivery.schedule"): "delivery.schedule",
	}, requests[0].ActionNames)
	require.Equal(t, map[string]string{
		tla.PropertyIdentifier("delivery.progress"):  "delivery.progress",
		ivy.PropertyIdentifier("delivery.progress"):  "delivery.progress",
		fizz.PropertyIdentifier("delivery.progress"): "delivery.progress",
	}, requests[0].PropertyNames)
	require.Equal(t, []string{"weak-schedule"}, requests[0].Fairness)
	require.Equal(t, model.Abstractions, requests[0].Abstractions)
	for _, index := range []int{0, 1, 2} {
		require.Equal(t, []verify.Unsupported{{
			Backend: string(requests[index].Backend), Construct: "property delivery.progress",
			Reason: "backend generation does not support temporal progress properties", Source: verify.Provenance{Path: "delivery.go"},
		}}, requests[index].Unsupported)
	}
}

func TestToolchainPlanCarriesCounterexampleModelAndVocabulary(t *testing.T) {
	model := verify.Model{
		Version: "trace-plan-test/v1",
		Entities: []verify.EntityType{{
			Name: "job", IDs: []string{"job#0"}, InitiallyExists: []string{"job#0"}, Initial: "ready",
			States: []verify.State{{Name: "ready"}, {Name: "done"}},
		}},
		Actions: []verify.Action{{
			Name: "complete", Parameters: []verify.Parameter{{Name: "job", Type: "job", Binding: verify.InputBinding}},
			Effects: []verify.Effect{{Kind: verify.SetStateEffect, Entity: "job", Ref: "job", State: "done"}},
		}},
		Properties: []verify.Property{{Name: "safe", Kind: verify.SafetyProperty, Expr: verify.Expr{Op: verify.TrueExpr}}},
	}

	requests, err := testToolchain().Plan(model, PlanOptions{Backends: "tlc,p,ivy", Profile: "smoke"})
	require.NoError(t, err)
	require.Equal(t, model, requests[0].Model)
	require.Equal(t, model, requests[1].Model)
	require.Equal(t, model, requests[2].Model)
	require.Equal(t, "job", requests[0].TraceVocabulary.EntityExists["exists_job"])
	require.Empty(t, requests[1].TraceVocabulary.EntityExists)
	require.Equal(t, "job#0", requests[2].TraceVocabulary.Identities["job_0"])
}

func TestToolchainPlanAppliesNightlyDepthCaps(t *testing.T) {
	requests, err := testToolchain().Plan(verify.Model{}, PlanOptions{
		Backends: "tlc,apalache,pex", Profile: "nightly",
	})
	require.NoError(t, err)
	require.Equal(t, []verify.Bounds{
		{MaxDepth: 1_000, Schedules: 10_000},
		{MaxDepth: 20, Schedules: 10_000},
		{MaxDepth: 100, Schedules: 10_000},
	}, []verify.Bounds{requests[0].Bounds, requests[1].Bounds, requests[2].Bounds})
	require.Equal(t, "Umpire-nightly.cfg", requests[0].Config)
}

func TestToolchainPlanSelectsExperimentalFizzOnlyExplicitlyForSmoke(t *testing.T) {
	all, err := testToolchain().Plan(verify.Model{}, PlanOptions{Backends: "all", Profile: "smoke"})
	require.NoError(t, err)
	require.NotContains(t, requestBackends(all), Fizz)

	requests, err := testToolchain().Plan(verify.Model{}, PlanOptions{
		ModelRoot: "/models", ArtifactRoot: "/artifacts", Target: "delivery",
		Backends: "fizz", Profile: "smoke", Requirements: []string{"fizz"},
	})
	require.NoError(t, err)
	require.Equal(t, []Request{{
		Backend: Fizz, Target: "delivery", Profile: "smoke", ToolPath: "/tools/fizz", JavaPath: "/tools/java", ToolVersion: "0.5.2",
		ModelDir: "/models/delivery/fizz", ArtifactDir: "/artifacts/delivery/smoke",
		Bounds: verify.Bounds{MaxDepth: 5, Schedules: 100}, ActionNames: map[string]string{}, PropertyNames: map[string]string{},
	}}, requests)

	_, err = testToolchain().Plan(verify.Model{}, PlanOptions{Backends: "fizz", Profile: "nightly"})
	require.ErrorContains(t, err, "available only in the smoke")
}

func TestToolchainPlanRejectsInvalidConfiguration(t *testing.T) {
	_, err := Toolchain{}.Plan(verify.Model{}, PlanOptions{Backends: "tlc", Profile: "smoke"})
	require.ErrorContains(t, err, "TLA+ verification requires -tla-jar or UMPIRE_TLA_JAR")

	_, err = testToolchain().Plan(verify.Model{}, PlanOptions{Backends: "missing", Profile: "smoke"})
	require.ErrorContains(t, err, `unknown verification backend "missing"`)

	_, err = testToolchain().Plan(verify.Model{}, PlanOptions{Backends: "tlc", Profile: "weekend"})
	require.ErrorContains(t, err, `unknown verification profile "weekend"`)
}

func TestToolVersionsReturnsDefensiveCopies(t *testing.T) {
	first := ToolVersions()
	require.NotEmpty(t, first)
	require.NotEmpty(t, first[1].Artifacts)
	first[0].Version = "changed"
	first[1].Artifacts[0].SHA256 = "changed"

	second := ToolVersions()
	require.Equal(t, "0.61.0", second[0].Version)
	require.NotEqual(t, "changed", second[1].Artifacts[0].SHA256)
}

func TestToolVersionsContainCompleteArtifacts(t *testing.T) {
	for _, tool := range ToolVersions() {
		for _, artifact := range tool.Artifacts {
			require.NotEmpty(t, artifact.Platform, tool.Name)
			require.NotEmpty(t, artifact.URL, tool.Name+" "+artifact.Platform)
			require.NotEmpty(t, artifact.SHA256, tool.Name+" "+artifact.Platform)
			require.NotEmpty(t, artifact.Archive, tool.Name+" "+artifact.Platform)
			require.NotEmpty(t, artifact.ArchiveType, tool.Name+" "+artifact.Platform)
			require.NotEmpty(t, artifact.ExtractRoot, tool.Name+" "+artifact.Platform)
			require.NotEmpty(t, artifact.Executable, tool.Name+" "+artifact.Platform)
		}
	}
}

func testToolchain() Toolchain {
	return Toolchain{
		TLAJarPath: "/tools/tla.jar", JavaPath: "/tools/java", PPath: "/tools/p",
		ApalachePath: "/tools/apalache", IvyPath: "/tools/ivy", FizzPath: "/tools/fizz",
	}
}

func requestBackends(requests []Request) []Backend {
	result := make([]Backend, len(requests))
	for index, request := range requests {
		result[index] = request.Backend
	}
	return result
}
