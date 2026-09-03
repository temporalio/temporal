package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"

	"go.temporal.io/server/tools/mutationtest/operators"
)

const (
	exitMutationKilled   = 0
	exitMutationSurvived = 1
	exitMutationSkipped  = 2
)

func Main() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx)
}

func run(ctx context.Context) int {
	cfg, exitCode, ok := parseConfig(os.Args[1:])
	if !ok {
		return exitCode
	}
	if cfg.listMutations {
		descriptors, err := operators.List()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitMutationSkipped
		}
		printMutationOperators(descriptors)
		return exitMutationKilled
	}
	selectedOperators, err := operators.Resolve(cfg.mutations, cfg.excludeMutations)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	result, err := runMutationCampaign(ctx, cfg, selectedOperators)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	return campaignExitCode(result.verdict)
}

func printMutationOperators(descriptors []operators.Descriptor) {
	fmt.Println("Mutation Operators")
	category := ""
	for _, descriptor := range descriptors {
		if descriptor.Category != category {
			category = descriptor.Category
			fmt.Println(category)
		}
		markers := make([]string, 0, 2)
		if descriptor.Default {
			markers = append(markers, "default")
		}
		if descriptor.Implementation == "local" {
			markers = append(markers, "local")
		}
		suffix := ""
		if len(markers) > 0 {
			suffix = " [" + strings.Join(markers, ", ") + "]"
		}
		fmt.Printf("  %s%s\n", descriptor.Name, suffix)
	}
}

type shardResult struct {
	index         int
	logPath       string
	survivorsPath string
	killed        int
	survived      int
	skipped       int
	survivors     []mutationOutcome
	err           error
}

func executeGeneratedMutants(
	ctx context.Context,
	run preparedMutationRun,
	worktreeDirs []string,
) (generatorStats, []shardResult, error) {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	queues := make([]chan mutantRecord, run.shardCount)
	results := make(chan shardResult, run.shardCount)
	var workers sync.WaitGroup
	for index := range run.shardCount {
		queues[index] = make(chan mutantRecord, 1)
		workers.Go(func() {
			result := runMutationWorker(workerCtx, run, worktreeDirs[index], index, queues[index])
			if result.err != nil {
				cancel()
			}
			results <- result
		})
	}

	generatorStats, generationErr := run.discovery.generate(workerCtx, func(record mutantRecord) error {
		queue := queues[(record.id-1)%len(queues)]
		select {
		case queue <- record:
			return nil
		case <-workerCtx.Done():
			return workerCtx.Err()
		}
	})
	for _, queue := range queues {
		close(queue)
	}
	workers.Wait()
	close(results)

	collected := make([]shardResult, run.shardCount)
	for result := range results {
		collected[result.index] = result
	}
	if generationErr != nil && !errors.Is(generationErr, context.Canceled) {
		return generatorStats, nil, generationErr
	}
	for _, result := range collected {
		if result.err != nil {
			return generatorStats, nil, result.err
		}
	}
	if ctx.Err() != nil {
		return generatorStats, nil, ctx.Err()
	}
	return generatorStats, collected, nil
}

func writeRunArtifacts(run preparedMutationRun, collected []shardResult, stats mutationRunStats) error {
	for index := range collected {
		collected[index].survivorsPath = filepath.Join(run.workspaces.runDir, fmt.Sprintf("survivors-%02d.diff", index+1))
		if err := writeSurvivorOutcomes(collected[index].survivorsPath, collected[index].survivors); err != nil {
			return err
		}
	}
	if err := writeSurvivors(filepath.Join(run.workspaces.runDir, "survivors.diff"), collected); err != nil {
		return err
	}
	return writeMutationSummary(run.workspaces.repoRoot, run.workspaces.runDir, collected, stats)
}

func campaignExitCode(verdict mutationCampaignVerdict) int {
	switch verdict {
	case campaignClean:
		return exitMutationKilled
	case campaignFindings:
		return exitMutationSurvived
	default:
		return exitMutationSkipped
	}
}

func runMutationWorker(ctx context.Context, run preparedMutationRun, worktreeDir string, index int, records <-chan mutantRecord) (result shardResult) {
	result.index = index
	result.logPath = filepath.Join(run.workspaces.runDir, fmt.Sprintf("shard-%02d.log", index+1))
	logFile, err := os.Create(result.logPath)
	if err != nil {
		result.err = err
		return result
	}
	defer func() {
		result.err = errors.Join(result.err, logFile.Close())
	}()

	for record := range records {
		outcome := executeMutant(ctx, worktreeDir, run.cfg, record)
		if outcome.err != nil {
			result.err = outcome.err
			return result
		}
		if err := writeMutationLogEntry(logFile, outcome); err != nil {
			result.err = err
			return result
		}
		switch outcome.status {
		case mutationKilled:
			result.killed++
		case mutationSurvived:
			result.survived++
			outcome.record.source = nil
			result.survivors = append(result.survivors, outcome)
		case mutationSkipped:
			result.skipped++
		default:
			result.err = fmt.Errorf("mutant %d has unknown status %q", record.id, outcome.status)
			return result
		}
		fmt.Fprintf(os.Stderr, "[shard %d] mutant=%06d status=%s\n", index+1, record.id, outcome.status)
	}
	return result
}

func writeMutationLogEntry(file *os.File, outcome mutationOutcome) error {
	_, err := fmt.Fprintf(file,
		"=== mutant %06d ===\noperator: %s\nfile: %s:%d:%d\ndigest: %s\nstatus: %s\nreason: %s\n%s\n",
		outcome.record.id,
		outcome.record.operator,
		outcome.record.file,
		outcome.record.line,
		outcome.record.column,
		outcome.record.digest,
		outcome.status,
		outcome.reason,
		outcome.diff,
	)
	return err
}

func writeSurvivors(path string, results []shardResult) (retErr error) {
	survivors := make([]mutationOutcome, 0)
	for _, result := range results {
		survivors = append(survivors, result.survivors...)
	}
	slices.SortFunc(survivors, func(left, right mutationOutcome) int {
		return left.record.id - right.record.id
	})
	return writeSurvivorOutcomes(path, survivors)
}

func writeSurvivorOutcomes(path string, survivors []mutationOutcome) (retErr error) {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, file.Close())
	}()
	for _, survivor := range survivors {
		if err := writeMutationLogEntry(file, survivor); err != nil {
			return err
		}
	}
	return nil
}

func printFileList(title string, files []string) {
	fmt.Println()
	fmt.Printf("%s (%d)\n", title, len(files))
	for _, file := range files {
		fmt.Printf("  %s\n", file)
	}
}
