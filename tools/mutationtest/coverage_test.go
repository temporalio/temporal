package mutationtest

import (
	"context"
	"go/ast"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCollectCoverageMapsCoveredNodesAndReportsUncoveredBlocks(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	targets, err := loadTargets(ctx, repoDir, []string{"value.go"}, "test_dep")
	require.NoError(t, err)

	coverage, uncovered, err := collectCoverage(ctx, repoDir, targets, config{
		testTags: "test_dep",
		timeout:  5 * time.Second,
	}, filepath.Join(t.TempDir(), "coverage.out"))
	require.NoError(t, err)
	require.NotEmpty(t, uncovered)
	require.Equal(t, "value.go", uncovered[0].file)

	var valueReturn, untestedReturn ast.Node
	ast.Inspect(targets[0].syntax, func(node ast.Node) bool {
		function, ok := node.(*ast.FuncDecl)
		if !ok || function.Body == nil || len(function.Body.List) == 0 {
			return true
		}
		switch function.Name.Name {
		case "value":
			valueReturn = function.Body.List[0]
		case "untested":
			untestedReturn = function.Body.List[0]
		default:
		}
		return true
	})
	require.True(t, coverage.covers(targets[0], valueReturn.Pos()))
	require.False(t, coverage.covers(targets[0], untestedReturn.Pos()))
}

func TestCollectCoverageRejectsFailingBaseline(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copyFixture(t, "baseline-failure")
	targets, err := loadTargets(ctx, repoDir, []string{"value.go"}, "test_dep")
	require.NoError(t, err)

	_, _, err = collectCoverage(ctx, repoDir, targets, config{
		testTags: "test_dep",
		timeout:  5 * time.Second,
	}, filepath.Join(t.TempDir(), "coverage.out"))
	require.ErrorContains(t, err, "baseline failed")
}
