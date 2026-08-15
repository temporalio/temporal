package protocol

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/model"
)

func TestCoverageCatalogDerivesStableProtocolSemantics(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)

	first, err := compiled.CoverageCatalog(CoverageCatalogOptions{})
	require.NoError(t, err)
	second, err := compiled.CoverageCatalog(CoverageCatalogOptions{})
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.True(t, slices.IsSortedFunc(first, compareCoveragePoints))
	require.NotEmpty(t, coveragePointsOfKind(first, umpire.CoverageFact))
	require.NotEmpty(t, coveragePointsOfKind(first, umpire.CoverageTransition))
	require.NotEmpty(t, coveragePointsOfKind(first, umpire.CoverageRelation))
	require.NotEmpty(t, coveragePointsOfKind(first, umpire.CoverageAction))

	first[0].ID = "mutated"
	again, err := compiled.CoverageCatalog(CoverageCatalogOptions{})
	require.NoError(t, err)
	require.NotEqual(t, first, again)
}

func TestCoverageCatalogSelectsNexusOperationTransitions(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	lifecycle, ok := compiled.Lifecycle(model.NexusOperationType)
	require.True(t, ok)
	want := make([]umpire.CoveragePoint, 0, len(lifecycle.Edges()))
	for _, edge := range lifecycle.Edges() {
		want = append(want, umpire.CoveragePoint{Kind: umpire.CoverageTransition, ID: TransitionCoverageID(model.NexusOperationType, edge)})
	}
	slices.SortFunc(want, compareCoveragePoints)

	got, err := compiled.CoverageCatalog(CoverageCatalogOptions{
		EntityTypes: []umpire.EntityType{model.NexusOperationType},
		Kinds:       []umpire.CoverageKind{umpire.CoverageTransition},
	})

	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestCoverageCatalogIncludesExecutableActionsOnceAndExcludesGaps(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	want := map[string]struct{}{}
	for _, entry := range compiled.ActionCatalog() {
		if entry.Action != nil {
			want[entry.Action.Name] = struct{}{}
		}
	}

	catalog, err := compiled.CoverageCatalog(CoverageCatalogOptions{Kinds: []umpire.CoverageKind{umpire.CoverageAction}})
	require.NoError(t, err)
	require.Len(t, catalog, len(want))
	for _, point := range catalog {
		_, exists := want[point.ID]
		require.True(t, exists, point.ID)
	}
}

func TestCoverageCatalogRejectsInvalidFilters(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)

	_, err = compiled.CoverageCatalog(CoverageCatalogOptions{EntityTypes: []umpire.EntityType{"Missing"}})
	require.ErrorContains(t, err, "unknown entity")
	_, err = compiled.CoverageCatalog(CoverageCatalogOptions{Kinds: []umpire.CoverageKind{umpire.CoverageRuleEvaluated}})
	require.ErrorContains(t, err, "unsupported coverage kind")
}

func TestProtocolNewCoverageUsesDerivedCatalog(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	options := CoverageCatalogOptions{
		EntityTypes: []umpire.EntityType{model.NexusOperationType},
		Kinds:       []umpire.CoverageKind{umpire.CoverageTransition},
	}
	catalog, err := compiled.CoverageCatalog(options)
	require.NoError(t, err)
	coverage, err := compiled.NewCoverage(true, options)
	require.NoError(t, err)

	require.Equal(t, catalog, coverage.Unmet())
}

func coveragePointsOfKind(points []umpire.CoveragePoint, kind umpire.CoverageKind) []umpire.CoveragePoint {
	return slices.DeleteFunc(slices.Clone(points), func(point umpire.CoveragePoint) bool {
		return point.Kind != kind
	})
}
