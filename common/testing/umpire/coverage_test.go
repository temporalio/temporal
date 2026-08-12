package umpire

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoverageDisabledIsNoOp(t *testing.T) {
	coverage, err := NewCoverage(false, CoveragePoint{Kind: CoverageFact, ID: "WorkflowStarted"})
	require.NoError(t, err)

	coverage.Record(CoveragePoint{Kind: CoverageFact, ID: "WorkflowStarted"})

	require.Empty(t, coverage.Snapshot())
	require.Empty(t, coverage.Unmet())
}

func TestCoverageReportsStableObservedAndUnmetCatalog(t *testing.T) {
	coverage, err := NewCoverage(true,
		CoveragePoint{Kind: CoverageTransition, ID: "Workflow:created/start/started"},
		CoveragePoint{Kind: CoverageFact, ID: "WorkflowStarted"},
		CoveragePoint{Kind: CoverageRuleEvaluated, ID: "EntityProgressRule"},
		CoveragePoint{Kind: CoverageRuleViolated, ID: "EntityProgressRule"},
		CoveragePoint{Kind: CoverageFact, ID: "WorkflowStarted"},
	)
	require.NoError(t, err)

	coverage.Record(CoveragePoint{Kind: CoverageRuleEvaluated, ID: "EntityProgressRule"})
	coverage.Record(CoveragePoint{Kind: CoverageFact, ID: "WorkflowStarted"})
	coverage.Record(CoveragePoint{Kind: CoverageFact, ID: "WorkflowStarted"})

	require.Equal(t, []CoveragePoint{
		{Kind: CoverageFact, ID: "WorkflowStarted"},
		{Kind: CoverageRuleEvaluated, ID: "EntityProgressRule"},
	}, coverage.Snapshot())
	require.Equal(t, []CoveragePoint{
		{Kind: CoverageTransition, ID: "Workflow:created/start/started"},
		{Kind: CoverageRuleViolated, ID: "EntityProgressRule"},
	}, coverage.Unmet())
}

func TestCoverageRejectsInvalidCatalogPoints(t *testing.T) {
	for _, point := range []CoveragePoint{
		{ID: "missing-kind"},
		{Kind: CoverageFact},
		{Kind: CoverageKind("unknown"), ID: "unknown"},
	} {
		coverage, err := NewCoverage(true, point)

		require.Nil(t, coverage)
		require.ErrorIs(t, err, ErrCoveragePoint)
	}
}

func TestCoverageSupportsConcurrentRecordsAndDefensiveSnapshots(t *testing.T) {
	coverage, err := NewCoverage(true, CoveragePoint{Kind: CoverageAction, ID: "start"})
	require.NoError(t, err)

	var waitGroup sync.WaitGroup
	for range 32 {
		waitGroup.Go(func() {
			coverage.Record(CoveragePoint{Kind: CoverageAction, ID: "start"})
		})
	}
	waitGroup.Wait()
	snapshot := coverage.Snapshot()
	require.Equal(t, []CoveragePoint{{Kind: CoverageAction, ID: "start"}}, snapshot)
	snapshot[0].ID = "changed"
	require.Equal(t, "start", coverage.Snapshot()[0].ID)
}
