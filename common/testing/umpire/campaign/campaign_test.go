package campaign

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
)

func TestCampaignFindsMinimizesReplaysAndPromotesSeededUnknownFailure(t *testing.T) {
	coverage, err := umpire.NewCoverage(true,
		umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: "safe"},
		umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: "noise"},
		umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: "seeded.fail"},
	)
	require.NoError(t, err)
	executor := ExecuteFunc(func(_ context.Context, scenario Scenario) Execution {
		violated := pathHasAction(scenario.Path, "seeded.fail")
		status := umpire.ClaimEstablished
		if violated {
			status = umpire.ClaimViolated
		}
		var trace umpire.Trace
		if violated {
			trace = umpire.Trace{Complete: true, Events: []umpire.TraceEvent{{
				Key: "failure", Kind: umpire.TraceVerdict, Name: "job-never-closes", Source: umpire.PublicAPIEvidence,
				Fields: map[string]string{"pass": "false", "violations": "1"},
			}}}
		}
		key, keyErr := ScenarioKey(scenario)
		require.NoError(t, keyErr)
		return Execution{
			Claim: umpire.QualifiedClaim{
				ModelVersion: scenario.ModelVersion,
				Property:     "job-never-closes",
				Environment:  "public-api",
				Status:       status,
				Observed:     []umpire.EvidenceSource{umpire.PublicAPIEvidence},
			},
			Trace:            trace,
			ObservedCoverage: scenarioCoverage(scenario),
			ReplayCommand:    []string{"umpire-campaign", "replay", key},
			CleanupComplete:  true,
		}
	})
	request := Request{
		Template:    seededTemplate(),
		RiskFocus:   []string{"seeded.fail"},
		Bounds:      Bounds{MaxCandidates: 2, MaxExecutions: 2, MaxMinimizationAttempts: 5},
		Seed:        42,
		Environment: umpire.PublicAPIProfile(),
		Coverage:    coverage,
		Corpus:      NewCorpus(10),
		Executor:    executor,
	}

	result, err := Run(context.Background(), request)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Len(t, result.Selected, 2)
	require.Contains(t, result.Selected[0].Reason, "declared-risk=1")
	require.Len(t, result.Discoveries, 1)
	discovery := result.Discoveries[0]
	require.Equal(t, ReplayMatched, discovery.ReplayStatus)
	require.Equal(t, 5, discovery.MinimizationAttempts)
	require.True(t, discovery.MinimizationComplete)
	require.Empty(t, discovery.PromotionBlock)
	require.Equal(t, []regress.CompletedAction{{Name: "seeded.fail"}}, discovery.Minimized.Path.Actions)
	require.Empty(t, discovery.Minimized.Path.Policies)
	require.Empty(t, discovery.Minimized.Path.Resources)
	require.Empty(t, discovery.Minimized.Path.Bindings)
	require.NotEmpty(t, discovery.Reductions)
	require.Len(t, result.Candidates, 1)
	require.NotEmpty(t, result.Candidates[0].ID)
	require.Equal(t, "job-never-closes", result.Candidates[0].Property)
	require.Equal(t, []regress.Node{{ID: 0, Kind: regress.ActionKind, Name: "seeded.fail"}}, result.Candidates[0].SparseIR.Nodes)
	require.Contains(t, result.Summary(), "discovered job-never-closes")
	require.Contains(t, result.Summary(), "replay=matched")
	require.Contains(t, result.CoverageDelta, umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: "seeded.fail"})

	repeatRequest := request
	repeatRequest.Corpus = NewCorpus(10)
	repeatCoverage, coverageErr := umpire.NewCoverage(true,
		umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: "safe"},
		umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: "noise"},
		umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: "seeded.fail"},
	)
	require.NoError(t, coverageErr)
	repeatRequest.Coverage = repeatCoverage
	repeated, err := Run(context.Background(), repeatRequest)
	require.NoError(t, err)
	require.Equal(t, result.Candidates[0].ID, repeated.Candidates[0].ID)
}

func TestCampaignCorpusDeduplicatesRuntimeIdentityAndRealizationDrift(t *testing.T) {
	left := Scenario{ModelVersion: "model/v1", Path: regress.CompletedPath{
		Actions:  []regress.CompletedAction{{Name: "complete", Realization: "local"}},
		Bindings: regress.Bindings{"run": "runtime-one"},
	}}
	right := cloneScenario(left)
	right.Path.Actions[0].Realization = "deployment"
	right.Path.Bindings["run"] = "runtime-two"
	leftKey, err := ScenarioKey(left)
	require.NoError(t, err)
	rightKey, err := ScenarioKey(right)
	require.NoError(t, err)
	require.Equal(t, leftKey, rightKey)

	corpus := NewCorpus(1)
	require.True(t, corpus.Add(leftKey))
	require.False(t, corpus.Add(rightKey))
	require.True(t, corpus.Contains(rightKey))
	require.Len(t, corpus.Keys(), 1)
}

func TestCampaignExpandsPairwiseExplorationAndNovelFaultTargetsDeterministically(t *testing.T) {
	lifecycle := umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial:     "open",
		States:      umpire.States{"open": {}, "closed": {}},
		Transitions: []umpire.Transition{{Event: "close", From: []string{"open"}, To: "closed"}},
	})
	require.NoError(t, lifecycle.Validate())
	request := Request{
		Template:     seededTemplate(),
		Seed:         7,
		Dimensions:   []umpire.MatrixDimension{{Name: "route", Values: []string{"sync", "backlog"}}},
		Exploration:  &Exploration{Lifecycle: lifecycle, Constraints: umpire.Constraints{MaxDepth: 1}},
		FaultTargets: []string{"history", "matching"},
	}

	first, err := expandScenarios(request)
	require.NoError(t, err)
	second, err := expandScenarios(request)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Len(t, first, 12)
	for _, scenario := range first {
		require.Equal(t, []string{"close"}, scenario.ExplorationRoute)
	}
}

func TestCampaignBlocksPromotionOnReplayObservationDrift(t *testing.T) {
	expected := Execution{
		Claim:           umpire.QualifiedClaim{Property: "safety", Status: umpire.ClaimViolated},
		CleanupComplete: true,
		Trace: umpire.Trace{Complete: true, Events: []umpire.TraceEvent{
			{Kind: umpire.TraceVerdict, Name: "safety", Fields: map[string]string{"pass": "false"}},
			{Kind: umpire.TraceFact, Name: "one"},
		}},
	}
	actual := expected
	actual.Trace.Events = append([]umpire.TraceEvent(nil), expected.Trace.Events...)
	actual.Trace.Events[1].Name = "different"

	status, err := compareReplay(expected, actual)
	require.NoError(t, err)
	require.Equal(t, ReplayObservationDrift, status)
}

func TestCampaignBlocksPromotionOnReplayCausalEvidenceDrift(t *testing.T) {
	expected := Execution{
		Claim:           umpire.QualifiedClaim{Property: "safety", Status: umpire.ClaimViolated},
		CleanupComplete: true,
		Trace: umpire.Trace{Complete: true, Events: []umpire.TraceEvent{
			{Key: "violation", Kind: umpire.TraceVerdict, Name: "safety", Fields: map[string]string{"pass": "false"}},
			{Key: "fact", Kind: umpire.TraceFact, Name: "observed", Causes: []string{"violation"}},
		}},
	}
	actual := expected
	actual.Trace.Events = slices.Clone(expected.Trace.Events)
	actual.Trace.Events[1].Causes = nil

	status, err := compareReplay(expected, actual)
	require.NoError(t, err)
	require.Equal(t, ReplayObservationDrift, status)
}

func TestCampaignRejectsReplayWithDifferentQualifiedViolationEvidence(t *testing.T) {
	expected := Execution{
		Claim:           umpire.QualifiedClaim{ModelVersion: "model/v1", Property: "safety", Environment: "history", Status: umpire.ClaimViolated, Observed: []umpire.EvidenceSource{umpire.HistoryEvidence}},
		CleanupComplete: true,
		Trace:           umpire.Trace{Complete: true, Events: []umpire.TraceEvent{{Kind: umpire.TraceVerdict, Name: "safety", Fields: map[string]string{"pass": "false"}}}},
	}
	actual := expected
	actual.Claim.Observed = []umpire.EvidenceSource{umpire.PublicAPIEvidence}

	status, err := compareReplay(expected, actual)
	require.NoError(t, err)
	require.Equal(t, ReplayViolationDrift, status)
}

func TestCampaignRejectsUnencodableSemanticIdentity(t *testing.T) {
	coverage, err := umpire.NewCoverage(true)
	require.NoError(t, err)
	template := seededTemplate()
	template.Paths[0].Actions[0].Arguments = []regress.Argument{regress.Literal(make(chan int))}
	template.Paths[0].Steps[0].Action.Arguments = slices.Clone(template.Paths[0].Actions[0].Arguments)

	_, err = Run(context.Background(), Request{
		Template:    template,
		Bounds:      Bounds{MaxCandidates: 1, MaxExecutions: 1, MaxMinimizationAttempts: 1},
		Environment: umpire.PublicAPIProfile(),
		Coverage:    coverage,
		Executor:    ExecuteFunc(func(context.Context, Scenario) Execution { return Execution{} }),
	})
	require.ErrorContains(t, err, "encode semantic scenario")
}

func TestCampaignReportsCandidateAndExecutionBudgetOmissions(t *testing.T) {
	coverage, err := umpire.NewCoverage(true)
	require.NoError(t, err)
	request := Request{
		Template:    seededTemplate(),
		Bounds:      Bounds{MaxCandidates: 2, MaxExecutions: 1, MaxMinimizationAttempts: 1},
		Environment: umpire.PublicAPIProfile(),
		Coverage:    coverage,
		Corpus:      NewCorpus(10),
		Executor: ExecuteFunc(func(_ context.Context, _ Scenario) Execution {
			return Execution{Claim: umpire.QualifiedClaim{Property: "safety", Status: umpire.ClaimEstablished}, CleanupComplete: true}
		}),
	}

	result, err := Run(context.Background(), request)
	require.NoError(t, err)
	require.False(t, result.Complete)
	require.Equal(t, "execution budget exhausted", result.StopReason)
	require.Len(t, result.Executions, 1)
}

func TestCampaignRejectsUnqualifiedExecutorSuccess(t *testing.T) {
	coverage, err := umpire.NewCoverage(true)
	require.NoError(t, err)
	request := Request{
		Template:    seededTemplate(),
		Bounds:      Bounds{MaxCandidates: 1, MaxExecutions: 1, MaxMinimizationAttempts: 1},
		Environment: umpire.PublicAPIProfile(),
		Coverage:    coverage,
		Executor: ExecuteFunc(func(_ context.Context, _ Scenario) Execution {
			return Execution{Claim: umpire.QualifiedClaim{Property: "safety", Status: umpire.ClaimEstablished}, CleanupComplete: true}
		}),
	}

	result, err := Run(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, umpire.ClaimInconclusive, result.Executions[0].Claim.Status)
	require.Contains(t, result.Executions[0].Claim.Diagnostic, "mismatched")
}

func TestCampaignRejectsProfileInvalidAndInconsistentExecutorEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Execution)
	}{
		{name: "unavailable source", mutate: func(execution *Execution) {
			execution.Trace.Events[0].Source = umpire.HistoryEvidence
		}},
		{name: "dangling cause", mutate: func(execution *Execution) {
			execution.Trace.Events[0].Causes = []string{"missing"}
		}},
		{name: "qualified violation with omissions", mutate: func(execution *Execution) {
			execution.Claim.Omissions = []string{"lost:public-api"}
		}},
		{name: "trace source omitted from claim", mutate: func(execution *Execution) {
			execution.Claim.Observed = nil
		}},
		{name: "verdict differs from claim", mutate: func(execution *Execution) {
			execution.Trace.Events[0].Name = "different-property"
		}},
		{name: "passing verdict", mutate: func(execution *Execution) {
			execution.Trace.Events[0].Fields["pass"] = "true"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coverage, err := umpire.NewCoverage(true)
			require.NoError(t, err)
			request := Request{
				Template:    seededTemplate(),
				Bounds:      Bounds{MaxCandidates: 1, MaxExecutions: 1, MaxMinimizationAttempts: 1},
				Environment: umpire.PublicAPIProfile(),
				Coverage:    coverage,
				Executor: ExecuteFunc(func(_ context.Context, scenario Scenario) Execution {
					execution := Execution{
						Claim: umpire.QualifiedClaim{
							ModelVersion: scenario.ModelVersion,
							Property:     "safety",
							Environment:  "public-api",
							Status:       umpire.ClaimViolated,
							Observed:     []umpire.EvidenceSource{umpire.PublicAPIEvidence},
						},
						Trace: umpire.Trace{Complete: true, Events: []umpire.TraceEvent{{
							Key: "violation", Kind: umpire.TraceVerdict, Name: "safety", Source: umpire.PublicAPIEvidence,
							Fields: map[string]string{"pass": "false", "violations": "1"},
						}}},
						CleanupComplete: true,
					}
					test.mutate(&execution)
					return execution
				}),
			}

			result, err := Run(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, umpire.ClaimInconclusive, result.Executions[0].Claim.Status)
			require.Empty(t, result.Discoveries)
			require.Empty(t, result.Candidates)
		})
	}
}

func seededTemplate() regress.Suite {
	path := func(actions ...string) regress.CompletedPath {
		result := regress.CompletedPath{
			Resources: []regress.CompletedResource{{Name: "worker"}},
			Policies:  []regress.CompletedPolicy{{Name: "drop", Start: 0, End: len(actions)}},
			Bindings:  regress.Bindings{"unused": "runtime-id"},
		}
		for _, action := range actions {
			result.Actions = append(result.Actions, regress.CompletedAction{Name: action})
			result.Steps = append(result.Steps, regress.CompletedStep{Action: regress.CompletedAction{Name: action}, Mode: regress.ProactiveAction})
		}
		return result
	}
	return regress.Suite{
		Name:         "seeded-unknown",
		ModelVersion: "model/v1",
		Profile:      regress.Profile{Name: "public-api", Environment: umpire.PublicAPIProfile()},
		IR:           regress.IR{Mode: regress.AllPathsMode, Symbols: regress.Symbols{}},
		Paths: []regress.CompletedPath{
			path("safe"),
			path("noise", "seeded.fail"),
		},
		PathCount: 2,
	}
}

func pathHasAction(path regress.CompletedPath, name string) bool {
	for _, action := range path.Actions {
		if action.Name == name {
			return true
		}
	}
	return false
}
