// Package campaign runs bounded behavioral discovery through promotion-ready replay.
package campaign

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
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

type rankedScenario struct {
	scenario Scenario
	key      string
	reason   string
	score    int
	tie      string
}

func rankScenarios(scenarios []Scenario, risks []string, unmet []umpire.CoveragePoint, seed int64) ([]rankedScenario, error) {
	unmetSet := make(map[umpire.CoveragePoint]struct{}, len(unmet))
	for _, point := range unmet {
		unmetSet[point] = struct{}{}
	}
	result := make([]rankedScenario, 0, len(scenarios))
	for _, scenario := range scenarios {
		key, err := ScenarioKey(scenario)
		if err != nil {
			return nil, err
		}
		semantic := scenarioSemantics(scenario)
		riskScore := 0
		for _, risk := range risks {
			if slices.Contains(semantic, risk) {
				riskScore++
			}
		}
		novelty := 0
		for _, point := range scenarioCoverage(scenario) {
			if _, exists := unmetSet[point]; exists {
				novelty++
			}
		}
		tieHash := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", seed, key)))
		result = append(result, rankedScenario{
			scenario: scenario,
			key:      key,
			reason:   fmt.Sprintf("declared-risk=%d semantic-novelty=%d", riskScore, novelty),
			score:    riskScore*1_000 + novelty,
			tie:      hex.EncodeToString(tieHash[:]),
		})
	}
	slices.SortFunc(result, func(left, right rankedScenario) int {
		if order := cmp.Compare(right.score, left.score); order != 0 {
			return order
		}
		if order := cmp.Compare(left.tie, right.tie); order != 0 {
			return order
		}
		return cmp.Compare(left.key, right.key)
	})
	return result, nil
}

func execute(ctx context.Context, executor Executor, environment umpire.EnvironmentProfile, key string, scenario Scenario) Execution {
	execution := executor.Execute(ctx, cloneScenario(scenario))
	execution.ScenarioKey = key
	invalid := execution.Claim.Property == "" || execution.Claim.Environment != environment.Name || execution.Claim.ModelVersion != scenario.ModelVersion
	if err := umpire.ValidateQualifiedClaim(environment, execution.Claim); err != nil {
		invalid = true
	}
	traceRequired := execution.Claim.Status == umpire.ClaimViolated || execution.Trace.Complete || len(execution.Trace.Events) > 0
	if traceRequired {
		if err := umpire.ValidateTraceEvidence(execution.Trace, environment); err != nil {
			invalid = true
		} else if err := validateClaimTrace(execution.Claim, execution.Trace); err != nil {
			invalid = true
		}
	}
	if execution.Error != "" || execution.TimedOut {
		invalid = true
	}
	if invalid {
		execution.Claim.Status = umpire.ClaimInconclusive
		execution.Claim.Diagnostic = "executor returned incomplete, mismatched, or failed qualified evidence"
	}
	return execution
}

func validateClaimTrace(claim umpire.QualifiedClaim, trace umpire.Trace) error {
	retained := make([]umpire.EvidenceSource, 0, len(trace.Events))
	matchedViolation := false
	for _, event := range trace.Events {
		retained = append(retained, event.Source)
		if failingVerdict(event, claim.Property) {
			matchedViolation = true
		}
	}
	slices.SortFunc(retained, func(left, right umpire.EvidenceSource) int {
		return cmp.Compare(string(left), string(right))
	})
	retained = slices.Compact(retained)
	observed := slices.Clone(claim.Observed)
	slices.SortFunc(observed, func(left, right umpire.EvidenceSource) int {
		return cmp.Compare(string(left), string(right))
	})
	observed = slices.Compact(observed)
	if !slices.Equal(retained, observed) {
		return errors.New("qualified claim does not identify the complete retained evidence sources")
	}
	if claim.Status == umpire.ClaimViolated && !matchedViolation {
		return fmt.Errorf("violated claim has no matching verdict for %q", claim.Property)
	}
	return nil
}

func minimizeAndReplay(ctx context.Context, request Request, original Scenario, originalRun Execution) (Discovery, error) {
	current := cloneScenario(original)
	currentRun := originalRun
	var reductions []Reduction
	attempts := 0
	minimizationComplete := true
	accept := func(kind, detail string, candidate Scenario) (bool, error) {
		if attempts >= request.Bounds.MaxMinimizationAttempts || ctx.Err() != nil {
			return false, nil
		}
		attempts++
		key, err := ScenarioKey(candidate)
		if err != nil {
			return false, err
		}
		trial := execute(ctx, request.Executor, request.Environment, key, candidate)
		same, err := sameViolation(originalRun, trial)
		if err != nil {
			return false, err
		}
		if !same {
			return false, nil
		}
		current = candidate
		currentRun = trial
		reductions = append(reductions, Reduction{Kind: kind, Detail: detail})
		return true, nil
	}
	for index := len(current.Path.Actions) - 1; index >= 0; index-- {
		if attempts >= request.Bounds.MaxMinimizationAttempts || ctx.Err() != nil {
			minimizationComplete = false
			break
		}
		candidate := cloneScenario(current)
		detail := candidate.Path.Actions[index].Name
		candidate.Path = removeAction(candidate.Path, index)
		if _, err := accept("action", detail, candidate); err != nil {
			return Discovery{}, err
		}
	}
	for index := len(current.Path.Policies) - 1; index >= 0; index-- {
		if attempts >= request.Bounds.MaxMinimizationAttempts || ctx.Err() != nil {
			minimizationComplete = false
			break
		}
		candidate := cloneScenario(current)
		detail := candidate.Path.Policies[index].Name
		candidate.Path.Policies = slices.Delete(candidate.Path.Policies, index, index+1)
		if _, err := accept("policy", detail, candidate); err != nil {
			return Discovery{}, err
		}
	}
	for index := len(current.Faults) - 1; index >= 0; index-- {
		if attempts >= request.Bounds.MaxMinimizationAttempts || ctx.Err() != nil {
			minimizationComplete = false
			break
		}
		candidate := cloneScenario(current)
		detail := candidate.Faults[index]
		candidate.Faults = slices.Delete(candidate.Faults, index, index+1)
		if _, err := accept("fault", detail, candidate); err != nil {
			return Discovery{}, err
		}
	}
	for index := len(current.Path.Resources) - 1; index >= 0; index-- {
		if attempts >= request.Bounds.MaxMinimizationAttempts || ctx.Err() != nil {
			minimizationComplete = false
			break
		}
		candidate := cloneScenario(current)
		detail := candidate.Path.Resources[index].Name
		candidate.Path.Resources = slices.Delete(candidate.Path.Resources, index, index+1)
		if _, err := accept("resource", detail, candidate); err != nil {
			return Discovery{}, err
		}
	}
	for _, binding := range unusedBindings(current.Path) {
		if attempts >= request.Bounds.MaxMinimizationAttempts || ctx.Err() != nil {
			minimizationComplete = false
			break
		}
		candidate := cloneScenario(current)
		delete(candidate.Path.Bindings, binding)
		if _, err := accept("binding", binding, candidate); err != nil {
			return Discovery{}, err
		}
	}
	replayKey, err := ScenarioKey(current)
	if err != nil {
		return Discovery{}, err
	}
	replay := execute(ctx, request.Executor, request.Environment, replayKey, current)
	status, err := compareReplay(currentRun, replay)
	if err != nil {
		return Discovery{}, err
	}
	discovery := Discovery{
		Original:             originalRun,
		Minimized:            current,
		MinimizedRun:         currentRun,
		Reductions:           reductions,
		MinimizationAttempts: attempts,
		MinimizationComplete: minimizationComplete && ctx.Err() == nil,
		Replay:               replay,
		ReplayStatus:         status,
	}
	discovery.PromotionBlock = promotionBlock(discovery.MinimizationComplete, currentRun, replay, status)
	return discovery, nil
}

func promotionBlock(minimizationComplete bool, currentRun, replay Execution, status ReplayStatus) string {
	switch {
	case !minimizationComplete:
		return "minimization budget exhausted or canceled"
	case !currentRun.CleanupComplete || !replay.CleanupComplete:
		return "cleanup did not complete"
	case currentRun.TimedOut || replay.TimedOut:
		return "execution timed out"
	case currentRun.Claim.Status == umpire.ClaimInconclusive || replay.Claim.Status == umpire.ClaimInconclusive:
		return "evidence is inconclusive"
	case status == ReplayObservationDrift:
		return "replay observation drift"
	case status == ReplayViolationDrift:
		return "replay violated a different behavior"
	default:
		return ""
	}
}

func removeAction(path regress.CompletedPath, index int) regress.CompletedPath {
	path.Actions = slices.Delete(path.Actions, index, index+1)
	if index < len(path.Steps) {
		path.Steps = slices.Delete(path.Steps, index, index+1)
	}
	for milestoneIndex := range path.Milestones {
		milestone := &path.Milestones[milestoneIndex]
		if milestone.BeforeAction > index {
			milestone.BeforeAction--
		}
		if milestone.AfterAction > index {
			milestone.AfterAction--
		}
	}
	for policyIndex := range path.Policies {
		policy := &path.Policies[policyIndex]
		if policy.Start > index {
			policy.Start--
		}
		if policy.End > index {
			policy.End--
		}
	}
	return path
}

func unusedBindings(path regress.CompletedPath) []string {
	used := map[string]struct{}{}
	collect := func(arguments []regress.Argument) {
		for _, argument := range arguments {
			if !argument.Literal && argument.SymbolName != "" {
				used[argument.SymbolName] = struct{}{}
			}
		}
	}
	for _, action := range path.Actions {
		collect(action.Arguments)
	}
	for _, milestone := range path.Milestones {
		collect(milestone.Arguments)
		if milestone.Binding != "" {
			used[milestone.Binding] = struct{}{}
		}
	}
	for _, policy := range path.Policies {
		collect(policy.Arguments)
	}
	var result []string
	for binding := range path.Bindings {
		if _, exists := used[binding]; !exists {
			result = append(result, binding)
		}
	}
	slices.Sort(result)
	return result
}

func sameViolation(expected, actual Execution) (bool, error) {
	if expected.Claim.Status != umpire.ClaimViolated || actual.Claim.Status != umpire.ClaimViolated ||
		!sameQualifiedClaim(expected.Claim, actual.Claim) || !expected.Trace.Complete || !actual.Trace.Complete ||
		!actual.CleanupComplete || actual.TimedOut || actual.Error != "" {
		return false, nil
	}
	expectedEvidence, err := violationEvidence(expected.Trace, expected.Claim.Property)
	if err != nil {
		return false, err
	}
	actualEvidence, err := violationEvidence(actual.Trace, actual.Claim.Property)
	if err != nil {
		return false, err
	}
	return len(expectedEvidence) > 0 && slices.Equal(expectedEvidence, actualEvidence), nil
}

func promotableViolation(execution Execution) bool {
	return execution.Claim.Status == umpire.ClaimViolated && execution.Trace.Complete && hasViolationEvidence(execution.Trace, execution.Claim.Property) &&
		execution.CleanupComplete && !execution.TimedOut && execution.Error == ""
}

func compareReplay(expected, actual Execution) (ReplayStatus, error) {
	same, err := sameViolation(expected, actual)
	if err != nil {
		return "", err
	}
	if !same {
		return ReplayViolationDrift, nil
	}
	expectedSemantic, err := semanticTrace(expected.Trace)
	if err != nil {
		return "", err
	}
	actualSemantic, err := semanticTrace(actual.Trace)
	if err != nil {
		return "", err
	}
	if !slices.Equal(expectedSemantic, actualSemantic) {
		expectedSet := slices.Clone(expectedSemantic)
		actualSet := slices.Clone(actualSemantic)
		slices.Sort(expectedSet)
		slices.Sort(actualSet)
		if slices.Equal(expectedSet, actualSet) {
			return ReplayScheduleDrift, nil
		}
		return ReplayObservationDrift, nil
	}
	return ReplayMatched, nil
}

func sameQualifiedClaim(expected, actual umpire.QualifiedClaim) bool {
	if expected.ModelVersion != actual.ModelVersion || expected.Target != actual.Target || expected.Property != actual.Property ||
		expected.Environment != actual.Environment || expected.Status != actual.Status {
		return false
	}
	expectedObserved := slices.Clone(expected.Observed)
	actualObserved := slices.Clone(actual.Observed)
	slices.Sort(expectedObserved)
	slices.Sort(actualObserved)
	expectedOmissions := slices.Clone(expected.Omissions)
	actualOmissions := slices.Clone(actual.Omissions)
	slices.Sort(expectedOmissions)
	slices.Sort(actualOmissions)
	return slices.Equal(expectedObserved, actualObserved) && slices.Equal(expectedOmissions, actualOmissions)
}

func violationEvidence(trace umpire.Trace, property string) ([]string, error) {
	var result []string
	for _, event := range trace.Events {
		if !failingVerdict(event, property) {
			continue
		}
		encoded, err := encodeTraceEvent(event)
		if err != nil {
			return nil, fmt.Errorf("%w: encode violation evidence: %v", ErrInvalidRequest, err)
		}
		result = append(result, encoded)
	}
	return result, nil
}

func hasViolationEvidence(trace umpire.Trace, property string) bool {
	for _, event := range trace.Events {
		if failingVerdict(event, property) {
			return true
		}
	}
	return false
}

func failingVerdict(event umpire.TraceEvent, property string) bool {
	return event.Kind == umpire.TraceVerdict && event.Name == property && event.Fields["pass"] == "false"
}

func semanticTrace(trace umpire.Trace) ([]string, error) {
	result := make([]string, 0, len(trace.Events))
	for _, event := range trace.Events {
		encoded, err := encodeTraceEvent(event)
		if err != nil {
			return nil, fmt.Errorf("%w: encode replay trace: %v", ErrInvalidRequest, err)
		}
		result = append(result, encoded)
	}
	return result, nil
}

func encodeTraceEvent(event umpire.TraceEvent) (string, error) {
	causes := slices.Clone(event.Causes)
	slices.Sort(causes)
	encoded, err := json.Marshal(struct {
		Key            string                `json:"key"`
		Kind           umpire.TraceKind      `json:"kind"`
		Name           string                `json:"name"`
		Source         umpire.EvidenceSource `json:"source"`
		ClockDomain    string                `json:"clockDomain"`
		SourceSequence uint64                `json:"sourceSequence"`
		Causes         []string              `json:"causes,omitempty"`
		Fields         map[string]string     `json:"fields,omitempty"`
	}{
		Key: event.Key, Kind: event.Kind, Name: event.Name, Source: event.Source,
		ClockDomain: event.ClockDomain, SourceSequence: event.SourceSequence,
		Causes: causes, Fields: event.Fields,
	})
	return string(encoded), err
}

func promote(request Request, discovery Discovery) (RegressionCandidate, error) {
	stableScenario := cloneScenario(discovery.Minimized)
	stableScenario.Path.Bindings = nil
	candidate := RegressionCandidate{
		Name:         discovery.Minimized.Name,
		ModelVersion: discovery.Minimized.ModelVersion,
		Environment:  request.Environment,
		Property:     discovery.MinimizedRun.Claim.Property,
		SparseIR:     sparseIR(request.Template.IR, stableScenario.Path),
		Scenario:     stableScenario,
		ReplayCommands: compactCommands([][]string{
			discovery.MinimizedRun.ReplayCommand,
			discovery.Replay.ReplayCommand,
		}),
	}
	scenarioKey, err := ScenarioKey(stableScenario)
	if err != nil {
		return RegressionCandidate{}, err
	}
	payload, err := json.Marshal(struct {
		ScenarioKey string     `json:"scenarioKey"`
		Property    string     `json:"property"`
		SparseIR    regress.IR `json:"sparseIR"`
		Environment string     `json:"environment"`
	}{ScenarioKey: scenarioKey, Property: candidate.Property, SparseIR: candidate.SparseIR, Environment: candidate.Environment.Name})
	if err != nil {
		return RegressionCandidate{}, fmt.Errorf("%w: encode regression candidate: %v", ErrInvalidRequest, err)
	}
	digest := sha256.Sum256(payload)
	candidate.ID = hex.EncodeToString(digest[:])
	return candidate, nil
}

func sparseIR(template regress.IR, path regress.CompletedPath) regress.IR {
	result := regress.IR{Mode: regress.OnePathMode, Symbols: cloneSymbols(template.Symbols)}
	actionNodes := make([]int, len(path.Actions))
	appendMilestones := func(boundary int) {
		for _, milestone := range path.Milestones {
			if milestone.AfterAction != boundary {
				continue
			}
			result.Nodes = append(result.Nodes, regress.Node{
				ID:        len(result.Nodes),
				Source:    milestone.Source,
				Kind:      milestone.Kind,
				Name:      milestone.Name,
				Arguments: slices.Clone(milestone.Arguments),
				Binding:   milestone.Binding,
			})
		}
	}
	appendMilestones(0)
	for index, action := range path.Actions {
		nodeID := len(result.Nodes)
		actionNodes[index] = nodeID
		result.Nodes = append(result.Nodes, regress.Node{
			ID:        nodeID,
			Source:    action.Source,
			Kind:      regress.ActionKind,
			Name:      action.Name,
			Arguments: slices.Clone(action.Arguments),
		})
		appendMilestones(index + 1)
	}
	for index := 1; index < len(result.Nodes); index++ {
		result.Edges = append(result.Edges, regress.Edge{From: result.Nodes[index-1].ID, To: result.Nodes[index].ID})
	}
	for index, policy := range path.Policies {
		body := slices.Clone(actionNodes[policy.Start:policy.End])
		result.Scopes = append(result.Scopes, regress.Scope{
			ID: index,
			Policy: regress.PolicyIR{
				Source:    policy.Source,
				Name:      policy.Name,
				Arguments: slices.Clone(policy.Arguments),
			},
			Body: body,
		})
	}
	result.Requirements = slices.Clone(template.Requirements)
	for index := range result.Requirements {
		result.Requirements[index].Arguments = slices.Clone(template.Requirements[index].Arguments)
	}
	return result
}

func cloneSymbols(symbols regress.Symbols) regress.Symbols {
	result := make(regress.Symbols, len(symbols))
	for name, symbol := range symbols {
		symbol.Uses = slices.Clone(symbol.Uses)
		result[name] = symbol
	}
	return result
}

func compactCommands(commands [][]string) [][]string {
	var result [][]string
	for _, command := range commands {
		if len(command) == 0 {
			continue
		}
		if len(result) == 0 || !slices.Equal(result[len(result)-1], command) {
			result = append(result, slices.Clone(command))
		}
	}
	return result
}

// ScenarioKey returns an environment-independent identity for completed semantic intent.
func ScenarioKey(scenario Scenario) (string, error) {
	type semanticAction struct {
		Name      string             `json:"name"`
		Arguments []regress.Argument `json:"arguments,omitempty"`
	}
	type semanticPolicy struct {
		Name      string             `json:"name"`
		Arguments []regress.Argument `json:"arguments,omitempty"`
		Start     int                `json:"start"`
		End       int                `json:"end"`
	}
	type semanticMilestone struct {
		Kind         regress.InstructionKind `json:"kind"`
		Name         string                  `json:"name"`
		Arguments    []regress.Argument      `json:"arguments,omitempty"`
		Binding      string                  `json:"binding,omitempty"`
		BeforeAction int                     `json:"beforeAction"`
		AfterAction  int                     `json:"afterAction"`
	}
	value := struct {
		ModelVersion     string              `json:"modelVersion"`
		Actions          []semanticAction    `json:"actions"`
		Policies         []semanticPolicy    `json:"policies,omitempty"`
		Milestones       []semanticMilestone `json:"milestones,omitempty"`
		Resources        []string            `json:"resources,omitempty"`
		Matrix           umpire.MatrixCase   `json:"matrix,omitempty"`
		ExplorationRoute []string            `json:"explorationRoute,omitempty"`
		Faults           []string            `json:"faults,omitempty"`
	}{
		ModelVersion:     scenario.ModelVersion,
		Matrix:           scenario.Matrix,
		ExplorationRoute: slices.Clone(scenario.ExplorationRoute),
		Faults:           slices.Clone(scenario.Faults),
	}
	for _, action := range scenario.Path.Actions {
		value.Actions = append(value.Actions, semanticAction{Name: action.Name, Arguments: slices.Clone(action.Arguments)})
	}
	for _, policy := range scenario.Path.Policies {
		value.Policies = append(value.Policies, semanticPolicy{Name: policy.Name, Arguments: slices.Clone(policy.Arguments), Start: policy.Start, End: policy.End})
	}
	for _, milestone := range scenario.Path.Milestones {
		value.Milestones = append(value.Milestones, semanticMilestone{
			Kind:         milestone.Kind,
			Name:         milestone.Name,
			Arguments:    slices.Clone(milestone.Arguments),
			Binding:      milestone.Binding,
			BeforeAction: milestone.BeforeAction,
			AfterAction:  milestone.AfterAction,
		})
	}
	for _, resource := range scenario.Path.Resources {
		value.Resources = append(value.Resources, resource.Name)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("%w: encode semantic scenario: %v", ErrInvalidRequest, err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func scenarioSemantics(scenario Scenario) []string {
	var result []string
	for _, action := range scenario.Path.Actions {
		result = append(result, action.Name)
	}
	for _, policy := range scenario.Path.Policies {
		result = append(result, policy.Name)
	}
	for _, resource := range scenario.Path.Resources {
		result = append(result, resource.Name)
	}
	result = append(result, scenario.Faults...)
	return result
}

func scenarioCoverage(scenario Scenario) []umpire.CoveragePoint {
	var result []umpire.CoveragePoint
	for _, action := range scenario.Path.Actions {
		result = append(result, umpire.CoveragePoint{Kind: umpire.CoverageAction, ID: action.Name})
	}
	for _, milestone := range scenario.Path.Milestones {
		kind := umpire.CoverageFact
		if milestone.Kind == regress.RelationKind {
			kind = umpire.CoverageRelation
		}
		result = append(result, umpire.CoveragePoint{Kind: kind, ID: milestone.Name})
	}
	return result
}

func coverageDifference(before, after []umpire.CoveragePoint) []umpire.CoveragePoint {
	seen := make(map[umpire.CoveragePoint]struct{}, len(before))
	for _, point := range before {
		seen[point] = struct{}{}
	}
	var result []umpire.CoveragePoint
	for _, point := range after {
		if _, exists := seen[point]; !exists {
			result = append(result, point)
		}
	}
	return result
}

func cloneScenario(scenario Scenario) Scenario {
	scenario.Path = clonePath(scenario.Path)
	scenario.Matrix = umpire.MatrixCase{Values: slices.Clone(scenario.Matrix.Values)}
	scenario.ExplorationRoute = slices.Clone(scenario.ExplorationRoute)
	scenario.Faults = slices.Clone(scenario.Faults)
	return scenario
}

func clonePath(path regress.CompletedPath) regress.CompletedPath {
	result := path
	result.Actions = make([]regress.CompletedAction, len(path.Actions))
	for index, action := range path.Actions {
		result.Actions[index] = action
		result.Actions[index].Arguments = slices.Clone(action.Arguments)
	}
	result.Steps = make([]regress.CompletedStep, len(path.Steps))
	for index, step := range path.Steps {
		result.Steps[index] = step
		result.Steps[index].Action.Arguments = slices.Clone(step.Action.Arguments)
		result.Steps[index].Preconditions = cloneAtoms(step.Preconditions)
		result.Steps[index].Effects = cloneAtoms(step.Effects)
	}
	result.Created = slices.Clone(path.Created)
	result.Resources = slices.Clone(path.Resources)
	result.Policies = make([]regress.CompletedPolicy, len(path.Policies))
	for index, policy := range path.Policies {
		result.Policies[index] = policy
		result.Policies[index].Arguments = slices.Clone(policy.Arguments)
	}
	result.Milestones = make([]regress.CompletedMilestone, len(path.Milestones))
	for index, milestone := range path.Milestones {
		result.Milestones[index] = milestone
		result.Milestones[index].Arguments = slices.Clone(milestone.Arguments)
	}
	result.Bindings = make(regress.Bindings, len(path.Bindings))
	for name, value := range path.Bindings {
		result.Bindings[name] = value
	}
	return result
}

func cloneAtoms(atoms []regress.CompletedAtom) []regress.CompletedAtom {
	result := make([]regress.CompletedAtom, len(atoms))
	for index, atom := range atoms {
		result[index] = atom
		result[index].Arguments = slices.Clone(atom.Arguments)
	}
	return result
}

func containsKey(keys map[string]struct{}, key string) bool {
	_, exists := keys[key]
	return exists
}

// Corpus is a bounded concurrency-safe set of canonical semantic scenario keys.
type Corpus struct {
	maxEntries int
	mu         sync.RWMutex
	keys       map[string]struct{}
}

// NewCorpus creates a bounded retained campaign corpus.
func NewCorpus(maxEntries int) *Corpus {
	return &Corpus{maxEntries: maxEntries, keys: map[string]struct{}{}}
}

// Contains reports whether equivalent semantic intent has already executed.
func (c *Corpus) Contains(key string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.keys[key]
	return exists
}

// Add retains a key unless the corpus is full.
func (c *Corpus) Add(key string) bool {
	if c == nil || key == "" || c.maxEntries < 1 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.keys[key]; exists {
		return false
	}
	if len(c.keys) >= c.maxEntries {
		return false
	}
	c.keys[key] = struct{}{}
	return true
}

// Keys returns the retained semantic keys in stable order.
func (c *Corpus) Keys() []string {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, 0, len(c.keys))
	for key := range c.keys {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}
