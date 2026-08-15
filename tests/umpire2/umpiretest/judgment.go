package umpiretest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/server/common/testing/umpire"
)

// EntitySelector identifies a runtime entity by type and, optionally, symbolic or concrete identity.
type EntitySelector struct {
	Entity umpire.EntityType
	Ref    string
	ID     string
}

// Expectation identifies the selected entity state to judge.
type Expectation struct {
	Subject EntitySelector
	State   string
}

// MatchCardinality records how many runtime entities matched a selector.
type MatchCardinality string

const (
	MatchNone       MatchCardinality = "none"
	MatchExactlyOne MatchCardinality = "exactly-one"
	MatchMultiple   MatchCardinality = "multiple"
)

type judgmentMonitor interface {
	Snapshot(string) umpire.Snapshot
	CheckNamespace(context.Context, string) []umpire.Violation
	ObservationSummary(string) string
}

// JudgmentRequest contains the observation boundary and explicit runtime selector.
type JudgmentRequest struct {
	Monitor      judgmentMonitor
	NamespaceID  string
	Expectation  Expectation
	Bindings     map[string]string
	Profile      umpire.EnvironmentProfile
	ModelVersion string
	Timeout      time.Duration
	PollInterval time.Duration
}

// JudgmentResult retains the matched state, rule results, evidence qualification, and bound outcome.
type JudgmentResult struct {
	Expectation Expectation
	Matched     *umpire.EntitySnapshot
	Cardinality MatchCardinality
	Violations  []umpire.Violation
	Claim       umpire.QualifiedClaim
	TimedOut    bool
	Diagnostic  string
}

// Judge waits for one unambiguous runtime entity and qualifies its expected state.
func Judge(ctx context.Context, request JudgmentRequest) (JudgmentResult, error) {
	selector, err := validateJudgmentRequest(request)
	if err != nil {
		return JudgmentResult{}, err
	}
	request.Expectation.Subject = selector
	result := JudgmentResult{Expectation: request.Expectation}
	pollInterval := request.PollInterval
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, request.Timeout)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		result = inspectJudgment(ctx, request, result)
		if result.Cardinality == MatchMultiple || judgmentFinished(result) {
			return result, nil
		}
		select {
		case <-ctx.Done():
			result = inspectJudgment(context.WithoutCancel(ctx), request, result)
			result.TimedOut = true
			result.Diagnostic = judgmentTimeoutDiagnostic(request, result)
			result.Claim = qualifyJudgment(request, result, true)
			return result, nil
		case <-ticker.C:
		}
	}
}

func validateJudgmentRequest(request JudgmentRequest) (EntitySelector, error) {
	if request.Monitor == nil {
		return EntitySelector{}, errors.New("umpiretest judgment: monitor is nil")
	}
	if request.NamespaceID == "" {
		return EntitySelector{}, errors.New("umpiretest judgment: namespace ID is empty")
	}
	selector := request.Expectation.Subject
	if selector.Entity == "" {
		return EntitySelector{}, errors.New("umpiretest judgment: selector entity is empty")
	}
	if selector.Ref != "" && selector.ID != "" {
		return EntitySelector{}, errors.New("umpiretest judgment: selector cannot set both Ref and ID")
	}
	if selector.Ref != "" {
		id, ok := request.Bindings[selector.Ref]
		if !ok || id == "" {
			return EntitySelector{}, fmt.Errorf("umpiretest judgment: binding %q is missing", selector.Ref)
		}
		selector.ID = id
	}
	if request.Expectation.State == "" {
		return EntitySelector{}, errors.New("umpiretest judgment: expected state is empty")
	}
	if request.Timeout <= 0 {
		return EntitySelector{}, errors.New("umpiretest judgment: timeout must be positive")
	}
	if err := umpire.ValidateEnvironmentProfile(request.Profile); err != nil {
		return EntitySelector{}, fmt.Errorf("umpiretest judgment: %w", err)
	}
	return selector, nil
}

func inspectJudgment(ctx context.Context, request JudgmentRequest, previous JudgmentResult) JudgmentResult {
	result := JudgmentResult{Expectation: request.Expectation}
	matches := matchingEntities(request.Monitor.Snapshot(request.NamespaceID), request.Expectation.Subject)
	switch len(matches) {
	case 0:
		result.Cardinality = MatchNone
	case 1:
		result.Cardinality = MatchExactlyOne
		result.Matched = &matches[0]
	default:
		result.Cardinality = MatchMultiple
		result.Diagnostic = fmt.Sprintf("selector matched %d entities", len(matches))
		result.Claim = qualifyJudgment(request, result, false)
		return result
	}
	if result.Matched != nil && (result.Matched.Current == request.Expectation.State || result.Matched.Terminal) {
		result.Violations = request.Monitor.CheckNamespace(ctx, request.NamespaceID)
		violated := result.Matched.Current != request.Expectation.State || len(result.Violations) > 0
		result.Claim = qualifyJudgment(request, result, violated)
		if violated {
			result.Diagnostic = fmt.Sprintf("entity state is %q; expected %q", result.Matched.Current, request.Expectation.State)
		}
		return result
	}
	result.TimedOut = previous.TimedOut
	return result
}

func matchingEntities(snapshot umpire.Snapshot, selector EntitySelector) []umpire.EntitySnapshot {
	entities := snapshot.EntitiesOfType(selector.Entity)
	if selector.ID == "" {
		return entities
	}
	result := entities[:0]
	for _, entity := range entities {
		if entity.ID == selector.ID {
			result = append(result, entity)
		}
	}
	return result
}

func judgmentFinished(result JudgmentResult) bool {
	return result.Claim.Status != ""
}

func qualifyJudgment(request JudgmentRequest, result JudgmentResult, violated bool) umpire.QualifiedClaim {
	ambiguous := result.Cardinality == MatchMultiple
	return umpire.QualifyEvidence(
		request.ModelVersion,
		fmt.Sprintf("%s:%s", request.Expectation.Subject.Entity, request.Expectation.Subject.ID),
		request.Profile,
		umpire.EvidenceRequirement{
			Property:               fmt.Sprintf("entity-state:%s:%s", request.Expectation.Subject.Entity, request.Expectation.State),
			Sources:                []umpire.EvidenceSource{umpire.InProcessEvidence},
			RequireIdentityLineage: request.Expectation.Subject.ID != "",
		},
		umpire.ObservedEvidence{
			Sources:                    []umpire.EvidenceSource{umpire.InProcessEvidence},
			IdentityLineageEstablished: request.Expectation.Subject.ID != "" && !ambiguous,
			AmbiguousIdentity:          ambiguous,
		},
		violated,
	)
}

func judgmentTimeoutDiagnostic(request JudgmentRequest, result JudgmentResult) string {
	observation := request.Monitor.ObservationSummary(request.NamespaceID)
	if result.Matched == nil {
		return fmt.Sprintf("no matching entity reached %q before the observation bound; %s", request.Expectation.State, observation)
	}
	return fmt.Sprintf("entity remained in %q instead of %q before the observation bound; %s", result.Matched.Current, request.Expectation.State, observation)
}
