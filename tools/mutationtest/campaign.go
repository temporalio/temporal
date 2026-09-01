package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type mutationCampaignVerdict string

const (
	campaignClean      mutationCampaignVerdict = "clean"
	campaignFindings   mutationCampaignVerdict = "findings"
	campaignIncomplete mutationCampaignVerdict = "incomplete"
)

type mutationCampaignResult struct {
	verdict mutationCampaignVerdict
}

func runMutationCampaign(ctx context.Context, cfg config) (_ mutationCampaignResult, retErr error) {
	runCtx := ctx
	cancel := func() {}
	if cfg.runTimeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, cfg.runTimeout)
	}
	defer cancel()

	prepared, err := prepareMutationRun(runCtx, cfg)
	if err != nil {
		return mutationCampaignResult{}, err
	}
	defer func() {
		retErr = errors.Join(retErr, prepared.workspaces.close(runCtx))
	}()
	stats, err := executePreparedMutationRun(runCtx, prepared)
	if err != nil {
		return mutationCampaignResult{}, err
	}
	return mutationCampaignResult{verdict: campaignVerdictForStats(stats)}, nil
}

type preparedMutationRun struct {
	cfg        config
	workspaces *executionWorkspaces
	discovery  mutationDiscovery
	shardCount int
}

func prepareMutationRun(ctx context.Context, cfg config) (_ preparedMutationRun, retErr error) {
	workspaces, err := openExecutionWorkspaces(ctx, cfg.outputRoot, cfg.ref)
	if err != nil {
		return preparedMutationRun{}, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, workspaces.close(ctx))
		}
	}()
	cfg.outputRoot = workspaces.runDir
	firstWorktreeDir := workspaces.dirs[0]

	mutationFiles, err := resolveMutationFiles(firstWorktreeDir, cfg.includeFiles, cfg.excludeFiles)
	if err != nil {
		return preparedMutationRun{}, err
	}
	if len(mutationFiles) == 0 {
		return preparedMutationRun{}, errors.New("no source files matched -include-files after exclusions")
	}
	selectedTestFiles, err := resolveFiles(firstWorktreeDir, cfg.testFiles, true)
	if err != nil {
		return preparedMutationRun{}, err
	}
	if len(selectedTestFiles) == 0 {
		return preparedMutationRun{}, errors.New("no test files matched -test-files")
	}
	shardCount := min(cfg.shardLevel, len(mutationFiles))
	cfg.print(workspaces.repoRoot, shardCount, mutationFiles, selectedTestFiles)
	if err := writeLines(filepath.Join(workspaces.runDir, "source-files.txt"), mutationFiles); err != nil {
		return preparedMutationRun{}, err
	}
	if err := writeLines(filepath.Join(workspaces.runDir, "test-files.txt"), selectedTestFiles); err != nil {
		return preparedMutationRun{}, err
	}
	if err := workspaces.prepareTests(selectedTestFiles); err != nil {
		return preparedMutationRun{}, err
	}

	fmt.Fprintln(os.Stderr, "[run] loading mutation targets and collecting baseline coverage")
	discovery, err := prepareMutationDiscovery(ctx, firstWorktreeDir, mutationFiles, cfg, filepath.Join(workspaces.runDir, "coverage.out"))
	if err != nil {
		return preparedMutationRun{}, err
	}
	if err := discovery.writeUncoveredFindings(filepath.Join(workspaces.runDir, "uncovered.txt")); err != nil {
		return preparedMutationRun{}, err
	}
	fmt.Fprintf(os.Stderr, "[run] baseline passed; uncovered blocks=%d\n", discovery.uncoveredCount())

	return preparedMutationRun{
		cfg:        cfg,
		workspaces: workspaces,
		discovery:  discovery,
		shardCount: shardCount,
	}, nil
}

func executePreparedMutationRun(ctx context.Context, run preparedMutationRun) (mutationRunStats, error) {
	if err := run.workspaces.prepareWorkers(ctx, run.shardCount); err != nil {
		return mutationRunStats{}, err
	}

	generatorStats, collected, err := executeGeneratedMutants(ctx, run, run.workspaces.dirs)
	if err != nil {
		return mutationRunStats{}, err
	}
	stats := aggregateStats(collected, generatorStats.duplicates, run.discovery.uncoveredCount())
	if err := writeRunArtifacts(run, collected, stats); err != nil {
		return mutationRunStats{}, err
	}
	return stats, nil
}

func campaignVerdictForStats(stats mutationRunStats) mutationCampaignVerdict {
	if stats.skipped > 0 {
		return campaignIncomplete
	}
	if stats.survived > 0 || stats.uncovered > 0 {
		return campaignFindings
	}
	return campaignClean
}
