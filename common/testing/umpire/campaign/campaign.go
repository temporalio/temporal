// Package campaign runs bounded behavioral discovery through promotion-ready replay.
package campaign

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
)

// Bounds are hard exploration and reduction limits.
type Bounds struct {
	MaxCandidates           int           `json:"maxCandidates"`
	MaxExecutions           int           `json:"maxExecutions"`
	MaxMinimizationAttempts int           `json:"maxMinimizationAttempts"`
	MaxDuration             time.Duration `json:"maxDuration,omitempty"`
}

// Exploration asks the generic lifecycle planner for one deterministic bounded route.
type Exploration struct {
	Lifecycle   *umpire.Lifecycle
	Constraints umpire.Constraints
}

// Request is the complete input to one bounded discovery campaign.
type Request struct {
	Template         regress.Suite
	RiskFocus        []string
	Bounds           Bounds
	Seed             int64
	Environment      umpire.EnvironmentProfile
	Coverage         *umpire.Coverage
	Corpus           *Corpus
	Dimensions       []umpire.MatrixDimension
	MatrixConstraint umpire.MatrixConstraint
	Exploration      *Exploration
	FaultTargets     []string
	Executor         Executor
}

// Scenario is one environment-independent semantic experiment.
type Scenario struct {
	Name             string                `json:"name"`
	ModelVersion     string                `json:"modelVersion"`
	Path             regress.CompletedPath `json:"path"`
	Matrix           umpire.MatrixCase     `json:"matrix,omitempty"`
	ExplorationRoute []string              `json:"explorationRoute,omitempty"`
	Faults           []string              `json:"faults,omitempty"`
}

// Selection records why a scenario was selected or omitted.
type Selection struct {
	Key      string   `json:"key"`
	Scenario Scenario `json:"scenario"`
	Reason   string   `json:"reason"`
}

// Execution is the stable output of one isolated scenario execution.
type Execution struct {
	ScenarioKey      string                 `json:"scenarioKey"`
	Claim            umpire.QualifiedClaim  `json:"claim"`
	Trace            umpire.Trace           `json:"trace,omitempty"`
	Artifact         *regress.Artifact      `json:"artifact,omitempty"`
	ObservedCoverage []umpire.CoveragePoint `json:"observedCoverage,omitempty"`
	ReplayCommand    []string               `json:"replayCommand,omitempty"`
	CleanupComplete  bool                   `json:"cleanupComplete"`
	TimedOut         bool                   `json:"timedOut,omitempty"`
	Error            string                 `json:"error,omitempty"`
}

// Executor realizes one scenario in a fresh environment.
type Executor interface {
	Execute(context.Context, Scenario) Execution
}

// ExecuteFunc adapts a function to Executor.
type ExecuteFunc func(context.Context, Scenario) Execution

func (f ExecuteFunc) Execute(ctx context.Context, scenario Scenario) Execution {
	return f(ctx, scenario)
}

// Reduction records one accepted monotonic simplification.
type Reduction struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// ReplayStatus distinguishes semantic agreement from distributed scheduling and evidence drift.
type ReplayStatus string

const (
	ReplayMatched          ReplayStatus = "matched"
	ReplayScheduleDrift    ReplayStatus = "schedule-drift"
	ReplayObservationDrift ReplayStatus = "observation-drift"
	ReplayViolationDrift   ReplayStatus = "violation-drift"
)

// Discovery records one qualified failure, its reduction, and semantic replay.
type Discovery struct {
	Original             Execution    `json:"original"`
	Minimized            Scenario     `json:"minimized"`
	MinimizedRun         Execution    `json:"minimizedRun"`
	Reductions           []Reduction  `json:"reductions,omitempty"`
	MinimizationAttempts int          `json:"minimizationAttempts"`
	MinimizationComplete bool         `json:"minimizationComplete"`
	Replay               Execution    `json:"replay"`
	ReplayStatus         ReplayStatus `json:"replayStatus"`
	PromotionBlock       string       `json:"promotionBlock,omitempty"`
}

// RegressionCandidate is stable behavioral intent ready for human-reviewed promotion.
type RegressionCandidate struct {
	ID             string                    `json:"id"`
	Name           string                    `json:"name"`
	ModelVersion   string                    `json:"modelVersion"`
	Environment    umpire.EnvironmentProfile `json:"environment"`
	Property       string                    `json:"property"`
	SparseIR       regress.IR                `json:"sparseIR"`
	Scenario       Scenario                  `json:"scenario"`
	ReplayCommands [][]string                `json:"replayCommands,omitempty"`
}

// Result is the complete bounded campaign record.
type Result struct {
	Selected       []Selection            `json:"selected,omitempty"`
	Omitted        []Selection            `json:"omitted,omitempty"`
	Executions     []Execution            `json:"executions,omitempty"`
	Discoveries    []Discovery            `json:"discoveries,omitempty"`
	Candidates     []RegressionCandidate  `json:"candidates,omitempty"`
	CoverageBefore []umpire.CoveragePoint `json:"coverageBefore,omitempty"`
	CoverageAfter  []umpire.CoveragePoint `json:"coverageAfter,omitempty"`
	CoverageDelta  []umpire.CoveragePoint `json:"coverageDelta,omitempty"`
	Complete       bool                   `json:"complete"`
	StopReason     string                 `json:"stopReason,omitempty"`
}

// Summary returns a concise, secret-free diagnosis of the strongest campaign outcome.
func (r Result) Summary() string {
	if len(r.Candidates) > 0 {
		candidate := r.Candidates[0]
		return fmt.Sprintf("discovered %s; candidate=%s replay=%s omitted=%d", candidate.Property, candidate.ID, r.Discoveries[0].ReplayStatus, len(r.Omitted))
	}
	if len(r.Discoveries) > 0 {
		discovery := r.Discoveries[0]
		return fmt.Sprintf("discovered %s; promotion blocked: %s; replay=%s", discovery.Original.Claim.Property, discovery.PromotionBlock, discovery.ReplayStatus)
	}
	for _, execution := range r.Executions {
		if execution.Claim.Status == umpire.ClaimUnsupported || execution.Claim.Status == umpire.ClaimInconclusive {
			return fmt.Sprintf("%s: %s (%s); omitted=%d", execution.Claim.Property, execution.Claim.Status, execution.Claim.Diagnostic, len(r.Omitted))
		}
	}
	if r.StopReason != "" {
		return fmt.Sprintf("campaign incomplete: %s; selected=%d omitted=%d", r.StopReason, len(r.Selected), len(r.Omitted))
	}
	return fmt.Sprintf("campaign complete: executions=%d discoveries=0 omitted=%d", len(r.Executions), len(r.Omitted))
}

var ErrInvalidRequest = errors.New("invalid campaign request")

// Run validates, selects, executes, minimizes, replays, and promotes a bounded campaign.
func Run(ctx context.Context, request Request) (Result, error) {
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	if request.Bounds.MaxDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.Bounds.MaxDuration)
		defer cancel()
	}
	result := Result{CoverageBefore: request.Coverage.Snapshot()}
	scenarios, err := expandScenarios(request)
	if err != nil {
		return Result{}, err
	}
	corpus := request.Corpus
	if corpus == nil {
		corpus = NewCorpus(max(request.Bounds.MaxCandidates, request.Bounds.MaxExecutions))
	}
	ranked, err := rankScenarios(scenarios, request.RiskFocus, request.Coverage.Unmet(), request.Seed)
	if err != nil {
		return Result{}, err
	}
	selectedKeys := map[string]struct{}{}
	for _, rankedScenario := range ranked {
		selection := Selection{Key: rankedScenario.key, Scenario: rankedScenario.scenario, Reason: rankedScenario.reason}
		switch {
		case corpus.Contains(rankedScenario.key):
			selection.Reason = "duplicate semantic scenario already retained in corpus"
			result.Omitted = append(result.Omitted, selection)
		case containsKey(selectedKeys, rankedScenario.key):
			selection.Reason = "duplicate semantic scenario in campaign request"
			result.Omitted = append(result.Omitted, selection)
		case len(result.Selected) >= request.Bounds.MaxCandidates:
			selection.Reason = "candidate budget exhausted"
			result.Omitted = append(result.Omitted, selection)
		default:
			selectedKeys[rankedScenario.key] = struct{}{}
			result.Selected = append(result.Selected, selection)
		}
	}
	for _, selected := range result.Selected {
		if len(result.Executions) >= request.Bounds.MaxExecutions {
			result.StopReason = "execution budget exhausted"
			break
		}
		if err := ctx.Err(); err != nil {
			result.StopReason = err.Error()
			break
		}
		if !corpus.Add(selected.Key) {
			result.Omitted = append(result.Omitted, Selection{Key: selected.Key, Scenario: selected.Scenario, Reason: "semantic scenario was claimed concurrently or corpus capacity exhausted"})
			continue
		}
		execution := execute(ctx, request.Executor, request.Environment, selected.Key, selected.Scenario)
		result.Executions = append(result.Executions, execution)
		for _, point := range execution.ObservedCoverage {
			request.Coverage.Record(point)
		}
		if !promotableViolation(execution) {
			continue
		}
		discovery, err := minimizeAndReplay(ctx, request, selected.Scenario, execution)
		if err != nil {
			return result, err
		}
		result.Discoveries = append(result.Discoveries, discovery)
		if discovery.PromotionBlock == "" {
			candidate, err := promote(request, discovery)
			if err != nil {
				return result, err
			}
			result.Candidates = append(result.Candidates, candidate)
		}
	}
	result.CoverageAfter = request.Coverage.Snapshot()
	result.CoverageDelta = coverageDifference(result.CoverageBefore, result.CoverageAfter)
	if result.StopReason == "" {
		for _, omitted := range result.Omitted {
			if omitted.Reason == "candidate budget exhausted" || strings.Contains(omitted.Reason, "corpus capacity exhausted") {
				result.StopReason = omitted.Reason
				break
			}
		}
	}
	result.Complete = result.StopReason == "" && len(result.Selected) <= request.Bounds.MaxExecutions
	if !result.Complete && result.StopReason == "" {
		result.StopReason = "execution budget exhausted"
	}
	return result, nil
}

func validateRequest(request Request) error {
	if err := regress.ValidateSuite(request.Template); err != nil {
		return fmt.Errorf("%w: behavioral template: %v", ErrInvalidRequest, err)
	}
	if request.Executor == nil {
		return fmt.Errorf("%w: executor is nil", ErrInvalidRequest)
	}
	if request.Coverage == nil {
		return fmt.Errorf("%w: coverage collector is nil", ErrInvalidRequest)
	}
	if request.Bounds.MaxCandidates < 1 || request.Bounds.MaxExecutions < 1 || request.Bounds.MaxMinimizationAttempts < 1 || request.Bounds.MaxDuration < 0 {
		return fmt.Errorf("%w: bounds must be positive", ErrInvalidRequest)
	}
	if err := umpire.ValidateEnvironmentProfile(request.Environment); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return nil
}

func expandScenarios(request Request) ([]Scenario, error) {
	matrix := []umpire.MatrixCase{{}}
	if len(request.Dimensions) > 0 {
		var err error
		matrix, err = umpire.GeneratePairwise(request.Dimensions, request.MatrixConstraint)
		if err != nil {
			return nil, fmt.Errorf("%w: pairwise matrix: %v", ErrInvalidRequest, err)
		}
	}
	var route []string
	if request.Exploration != nil {
		if request.Exploration.Lifecycle == nil {
			return nil, fmt.Errorf("%w: exploration lifecycle is nil", ErrInvalidRequest)
		}
		plan := umpire.Explore(request.Exploration.Lifecycle, request.Exploration.Constraints, umpire.WithSeed(request.Seed))
		if len(plan.Routes) > 0 {
			route = slices.Clone(plan.Routes[0])
		}
	}
	var base []Scenario
	for pathIndex, path := range request.Template.Paths {
		for _, matrixCase := range matrix {
			base = append(base, Scenario{
				Name:             fmt.Sprintf("%s/path-%d", request.Template.Name, pathIndex),
				ModelVersion:     request.Template.ModelVersion,
				Path:             clonePath(path),
				Matrix:           matrixCase,
				ExplorationRoute: slices.Clone(route),
			})
		}
	}
	result := slices.Clone(base)
	seenFaults := map[string]struct{}{}
	var breadth, depth []Scenario
	for _, fault := range request.FaultTargets {
		if fault == "" {
			return nil, fmt.Errorf("%w: fault target is empty", ErrInvalidRequest)
		}
		for _, scenario := range base {
			faulted := cloneScenario(scenario)
			faulted.Faults = []string{fault}
			faulted.Name += "/fault-" + fault
			if _, exists := seenFaults[fault]; !exists {
				seenFaults[fault] = struct{}{}
				breadth = append(breadth, faulted)
			} else {
				depth = append(depth, faulted)
			}
		}
	}
	return append(result, append(breadth, depth...)...), nil
}
