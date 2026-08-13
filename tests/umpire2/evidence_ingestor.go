package umpire2

import (
	"context"
	"errors"
	"fmt"
	"sync"

	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/protocol"
)

type evidenceIngestor struct {
	registry   *umpirefw.ModelState
	rulebook   *umpirefw.RuleRegistry
	factLog    *umpirefw.FactLog
	protocol   *protocol.Protocol
	relations  *umpirefw.RelationStore
	trace      *executionTrace
	coverageMu sync.RWMutex
	coverage   *umpirefw.Coverage
}

func newEvidenceIngestor(
	registry *umpirefw.ModelState,
	rulebook *umpirefw.RuleRegistry,
	factLog *umpirefw.FactLog,
	compiled *protocol.Protocol,
	relations *umpirefw.RelationStore,
	trace *executionTrace,
) *evidenceIngestor {
	return &evidenceIngestor{
		registry:  registry,
		rulebook:  rulebook,
		factLog:   factLog,
		protocol:  compiled,
		relations: relations,
		trace:     trace,
	}
}

func (i *evidenceIngestor) ingest(ctx context.Context, facts []umpirefw.Fact) error {
	if len(facts) == 0 {
		return nil
	}
	i.factLog.AddAll(facts)
	modelErr := i.registry.RouteFacts(ctx, facts)
	relationErrors := i.protocol.ApplyRelations(i.relations, facts)
	for _, relationErr := range relationErrors {
		i.recordRelationConflict(relationErr)
	}
	relationErr := errors.Join(relationErrors...)
	i.recordFactCoverage(facts)
	traceErr := i.trace.recordFacts(facts)
	return errors.Join(modelErr, relationErr, traceErr)
}

func (i *evidenceIngestor) recordRelationConflict(err error) {
	var relationErr *umpirefw.RelationError
	if !errors.As(err, &relationErr) || relationErr.Scope.Type == "" || relationErr.Scope.ID == "" {
		return
	}
	key := fmt.Sprintf("%s:%s:%s:%s", relationErr.Type, relationErr.Source, relationErr.Target, relationErr.Reason)
	i.rulebook.RecordConformance(relationErr.Scope, key, umpirefw.Violation{
		Rule:    "Conformance",
		Message: fmt.Sprintf("relation %s rejected: %s", relationErr.Type, relationErr.Reason),
		Tags: map[string]string{
			"relation": string(relationErr.Type),
			"source":   relationErr.Source.String(),
			"target":   relationErr.Target.String(),
		},
	})
}

func (i *evidenceIngestor) recordFactCoverage(facts []umpirefw.Fact) {
	i.coverageMu.RLock()
	coverage := i.coverage
	i.coverageMu.RUnlock()
	if coverage == nil {
		return
	}
	roots := map[umpirefw.EntityID]struct{}{}
	for _, observed := range facts {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageFact, ID: observed.Name()})
		if path := observed.TargetEntity(); path != nil {
			roots[path.Root()] = struct{}{}
		}
	}
	for _, edge := range i.relations.Snapshot() {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRelation, ID: string(edge.Type)})
	}
	for root := range roots {
		for _, entry := range i.registry.QueryAll(0, &root) {
			lifecycled, ok := entry.Entity.(umpirefw.Lifecycled)
			if !ok {
				continue
			}
			for _, edge := range lifecycled.Lifecycle().VisitedEdges() {
				coverage.Record(umpirefw.CoveragePoint{
					Kind: umpirefw.CoverageTransition,
					ID:   protocol.TransitionCoverageID(entry.Entity.Type(), edge),
				})
			}
		}
	}
}

func (i *evidenceIngestor) observeExecution(observed umpirefw.ExecutionObservation) error {
	if observed.Kind == umpirefw.ExecutionActionStart && observed.Action != "" {
		i.coverageMu.RLock()
		coverage := i.coverage
		i.coverageMu.RUnlock()
		if coverage != nil {
			coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageAction, ID: observed.Action})
		}
	}
	return i.trace.observeExecution(observed)
}

func (i *evidenceIngestor) check(ctx context.Context, root umpirefw.EntityID, final bool) []umpirefw.Violation {
	violations := i.rulebook.Check(ctx, final, &root)
	i.recordRuleCoverage(violations)
	return violations
}

func (i *evidenceIngestor) recordRuleCoverage(violations []umpirefw.Violation) {
	i.coverageMu.RLock()
	coverage := i.coverage
	i.coverageMu.RUnlock()
	if coverage == nil {
		return
	}
	for _, stats := range i.rulebook.Stats() {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRuleEvaluated, ID: stats.Name})
	}
	for _, violation := range violations {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRuleViolated, ID: violation.Rule})
	}
}

func (i *evidenceIngestor) purgeScope(root umpirefw.EntityID) {
	i.registry.PurgeScope(root)
	i.factLog.PurgeScope(root)
	i.rulebook.PurgeScope(root)
	i.relations.PurgeScope(root)
	i.trace.purgeScope(root.ID)
}

func (i *evidenceIngestor) setCoverage(coverage *umpirefw.Coverage) {
	i.coverageMu.Lock()
	i.coverage = coverage
	i.coverageMu.Unlock()
}
