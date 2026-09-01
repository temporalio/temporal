package mutationtest

import (
	"context"
	"errors"
	"os"

	"go.temporal.io/server/tools/mutationtest/operators"
)

type mutationDiscovery struct {
	targets   []targetFile
	coverage  coverageIndex
	operators []operators.Operator
	uncovered []uncoveredFinding
}

func prepareMutationDiscovery(
	ctx context.Context,
	worktreeDir string,
	mutationFiles []string,
	cfg config,
	profilePath string,
	selectedOperators []operators.Operator,
) (_ mutationDiscovery, retErr error) {
	defer func() {
		if err := os.Remove(profilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, err)
		}
	}()

	targets, err := loadTargets(ctx, worktreeDir, mutationFiles, cfg.testTags)
	if err != nil {
		return mutationDiscovery{}, err
	}
	coverage, uncovered, err := collectCoverage(ctx, worktreeDir, targets, cfg, profilePath)
	if err != nil {
		return mutationDiscovery{}, err
	}
	return mutationDiscovery{
		targets:   targets,
		coverage:  coverage,
		operators: selectedOperators,
		uncovered: uncovered,
	}, nil
}

func (discovery mutationDiscovery) generate(ctx context.Context, emit func(mutantRecord) error) (generatorStats, error) {
	return generateMutantsWithOperators(ctx, discovery.targets, discovery.coverage, discovery.operators, emit)
}

func (discovery mutationDiscovery) writeUncoveredFindings(path string) error {
	return writeUncoveredFindings(path, discovery.uncovered)
}

func (discovery mutationDiscovery) uncoveredCount() int {
	return len(discovery.uncovered)
}
