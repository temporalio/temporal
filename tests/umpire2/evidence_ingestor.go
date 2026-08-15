package umpire2

import (
	"context"
	"errors"
	"sync"

	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/protocol"
)

type evidenceIngestor struct {
	runtime    *umpirefw.Runtime
	protocol   *protocol.Protocol
	trace      *executionTrace
	coverageMu sync.RWMutex
	coverage   *umpirefw.Coverage
}

func newEvidenceIngestor(
	runtime *umpirefw.Runtime,
	compiled *protocol.Protocol,
	trace *executionTrace,
) *evidenceIngestor {
	return &evidenceIngestor{
		runtime:  runtime,
		protocol: compiled,
		trace:    trace,
	}
}

func (i *evidenceIngestor) ingest(ctx context.Context, facts []umpirefw.Fact) error {
	if len(facts) == 0 {
		return nil
	}
	runtimeErr := i.runtime.Ingest(ctx, facts...)
	i.recordFactCoverage(facts)
	traceErr := i.trace.recordFacts(facts)
	return errors.Join(runtimeErr, traceErr)
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
	for root := range roots {
		view := i.runtime.View(root)
		for _, edge := range view.Relations() {
			coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRelation, ID: string(edge.Type)})
		}
		for _, entry := range view.AllEntities(0) {
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
	violations := i.runtime.Check(ctx, root, final)
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
	for _, stats := range i.runtime.RuleStats() {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRuleEvaluated, ID: stats.Name})
	}
	for _, violation := range violations {
		coverage.Record(umpirefw.CoveragePoint{Kind: umpirefw.CoverageRuleViolated, ID: violation.Rule})
	}
}

func (i *evidenceIngestor) purgeScope(root umpirefw.EntityID) {
	i.runtime.Purge(root)
	i.trace.purgeScope(root.ID)
}

func (i *evidenceIngestor) setCoverage(coverage *umpirefw.Coverage) {
	i.coverageMu.Lock()
	i.coverage = coverage
	i.coverageMu.Unlock()
}
