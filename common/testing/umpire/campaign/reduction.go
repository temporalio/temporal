package campaign

import (
	"context"
	"fmt"
	"slices"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/regress"
)

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
