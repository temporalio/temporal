package campaign

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"go.temporal.io/server/common/testing/umpire"
)

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
