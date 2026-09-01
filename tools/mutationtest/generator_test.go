package mutationtest

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSelectedOperatorsAreExplicitAndCompileOriented(t *testing.T) {
	t.Parallel()

	operators, err := selectedOperators()
	require.NoError(t, err)
	names := make([]string, 0, len(operators))
	for _, operator := range operators {
		names = append(names, operator.name)
	}
	require.Equal(t, []string{
		"arithmetic/assign_invert",
		"arithmetic/assignment",
		"arithmetic/base",
		"arithmetic/bitwise",
		"branch/case",
		"branch/else",
		"branch/if",
		"conditional/negated",
		"expression/comparison",
		"loop/break",
		"loop/condition",
		"loop/range_break",
		"numbers/decrementer",
		"numbers/incrementer",
	}, names)
}

func TestGenerateMutantsOnlyEmitsCoveredUniqueMutations(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	discovery, err := prepareMutationDiscovery(ctx, repoDir, []string{"value.go"}, config{
		testTags: "test_dep",
		timeout:  5 * time.Second,
	}, filepath.Join(t.TempDir(), "coverage.out"))
	require.NoError(t, err)

	var records []mutantRecord
	stats, err := discovery.generate(ctx, func(record mutantRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.Zero(t, stats.duplicates)
	for index, record := range records {
		require.Equal(t, index+1, record.id)
		require.Equal(t, "value.go", record.file)
		require.NotContains(t, string(record.source), "return input - 1")
	}
}

func TestGenerateMutantsDeduplicatesEquivalentOutput(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)
	targets, err := loadTargets(ctx, repoDir, []string{"value.go"}, "test_dep")
	require.NoError(t, err)
	coverage, _, err := collectCoverage(ctx, repoDir, targets, config{
		testTags: "test_dep",
		timeout:  5 * time.Second,
	}, filepath.Join(t.TempDir(), "coverage.out"))
	require.NoError(t, err)
	operators, err := selectedOperators()
	require.NoError(t, err)

	var comparison mutationOperator
	for _, operator := range operators {
		if operator.name == "expression/comparison" {
			comparison = operator
			break
		}
	}
	require.NotNil(t, comparison.mutate)
	stats, err := generateMutantsWithOperators(
		ctx,
		targets,
		coverage,
		[]mutationOperator{comparison, comparison},
		func(mutantRecord) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, 1, stats.generated)
	require.Equal(t, 1, stats.duplicates)
}
