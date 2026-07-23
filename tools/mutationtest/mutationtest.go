package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	exitMutationKilled   = 0
	exitMutationSurvived = 1
	exitMutationSkipped  = 2
)

func Main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx))
}

func run(ctx context.Context) int {
	// go-mutesting invokes this binary as its per-mutant exec hook and sets MUTATE_CHANGED.
	if _, ok := os.LookupEnv("MUTATE_CHANGED"); ok {
		return runMutationExec(ctx)
	}
	cfg, exitCode, ok := parseConfig(os.Args[1:])
	if !ok {
		return exitCode
	}
	return runMutationSuite(ctx, cfg)
}

func runMutationSuite(ctx context.Context, cfg config) int {
	repoRoot, err := gitRepoRoot(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	fmt.Fprintln(os.Stderr, "[run] pruning stale git worktrees")
	if err := gitWorktreePrune(ctx, repoRoot); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}

	mutatorPath := cfg.mutatorPath
	if !filepath.IsAbs(mutatorPath) {
		mutatorPath = filepath.Join(repoRoot, mutatorPath)
	}
	cfg.mutatorPath = mutatorPath
	if !filepath.IsAbs(cfg.outputRoot) {
		cfg.outputRoot = filepath.Join(repoRoot, cfg.outputRoot)
	}
	mutationFiles, err := resolveMutationFiles(repoRoot, cfg.includeFiles, cfg.excludeFiles)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	if len(mutationFiles) == 0 {
		fmt.Fprintln(os.Stderr, "no source files matched -include-files after exclusions")
		return exitMutationSkipped
	}
	selectedTestFiles, err := resolveFiles(repoRoot, cfg.testFiles, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	if len(selectedTestFiles) == 0 {
		fmt.Fprintln(os.Stderr, "no test files matched -test-files")
		return exitMutationSkipped
	}
	effectiveShardCount := 1
	if cfg.shardLevel > 1 && len(mutationFiles) > 1 {
		effectiveShardCount = cfg.shardLevel
		if effectiveShardCount > len(mutationFiles) {
			effectiveShardCount = len(mutationFiles)
		}
	}
	cfg.print(repoRoot, effectiveShardCount, mutationFiles, selectedTestFiles)

	runDir, err := prepareRunDir(ctx, cfg.outputRoot)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	fmt.Fprintf(os.Stderr, "[run] prepared output directory %s\n", displayPath(repoRoot, runDir))
	if err := writeLines(filepath.Join(runDir, "source-files.txt"), mutationFiles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	if err := writeLines(filepath.Join(runDir, "test-files.txt"), selectedTestFiles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	firstWorktreeDir := filepath.Join(runDir, "worktree-01")
	fmt.Fprintf(os.Stderr, "[run] preparing preflight worktree %s\n", displayPath(repoRoot, firstWorktreeDir))
	if err := prepareShardWorktree(ctx, repoRoot, firstWorktreeDir, cfg.ref, selectedTestFiles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	fmt.Fprintln(os.Stderr, "[run] running preflight test build")
	if err := verifyTestBuild(ctx, firstWorktreeDir, cfg, selectedTestFiles); err != nil {
		_ = gitWorktreeRemove(context.WithoutCancel(ctx), firstWorktreeDir)
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	fmt.Fprintln(os.Stderr, "[run] preflight test build passed")

	if cfg.shardLevel > 1 && len(mutationFiles) > 1 {
		level := cfg.shardLevel
		if level > len(mutationFiles) {
			level = len(mutationFiles)
		}
		shards := make([][]string, level)
		for i, file := range mutationFiles {
			shardIndex := i % level
			shards[shardIndex] = append(shards[shardIndex], file)
		}

		results := make(chan shardResult, len(shards))
		var wg sync.WaitGroup
		for i, shardTargets := range shards {
			if len(shardTargets) == 0 {
				continue
			}
			wg.Add(1)
			go func(index int, targets []string) {
				defer wg.Done()
				result, err := runMutationShard(ctx, repoRoot, mutatorPath, cfg, runDir, selectedTestFiles, targets, index, index == 0)
				if err != nil {
					results <- shardResult{index: index, exitCode: exitMutationSkipped, err: err}
					return
				}
				results <- result
			}(i, shardTargets)
		}
		wg.Wait()
		close(results)

		collected := make([]shardResult, len(shards))
		finalExitCode := 0
		for result := range results {
			collected[result.index] = result
			if result.err != nil {
				finalExitCode = exitMutationSkipped
			} else if result.exitCode != 0 && finalExitCode == 0 {
				finalExitCode = result.exitCode
			}
		}
		for _, result := range collected {
			if result.logPath == "" && result.err == nil {
				continue
			}
			fmt.Printf("== shard %d ==\n", result.index+1)
			if result.err != nil {
				fmt.Fprintln(os.Stderr, result.err)
				continue
			}
			fmt.Printf("log: %s\n", displayPath(repoRoot, result.logPath))
		}
		fmt.Fprintln(os.Stderr, "[run] writing mutation summary")
		printMutationSummary(repoRoot, runDir, collected)
		return finalExitCode
	}

	result, err := runMutationShard(ctx, repoRoot, mutatorPath, cfg, runDir, selectedTestFiles, mutationFiles, 0, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitMutationSkipped
	}
	fmt.Printf("Mutation log: %s\n", displayPath(repoRoot, result.logPath))
	fmt.Fprintln(os.Stderr, "[run] writing mutation summary")
	printMutationSummary(repoRoot, runDir, []shardResult{result})
	return result.exitCode
}

func printFileList(title string, files []string) {
	fmt.Println()
	fmt.Printf("%s (%d)\n", title, len(files))
	for _, file := range files {
		fmt.Printf("  %s\n", file)
	}
}

func verifyTestBuild(ctx context.Context, worktreeDir string, cfg config, selectedTestFiles []string) error {
	cmd := exec.CommandContext(
		ctx,
		"make",
		"build-tests",
		"TEST_TAG="+cfg.testTags,
	)
	cmd.Dir = worktreeDir
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("test build preflight failed:\n%s", strings.TrimSpace(string(output)))
	}
	return nil
}

func prepareShardWorktree(ctx context.Context, repoRoot string, worktreeDir string, ref string, selectedTestFiles []string) error {
	fmt.Fprintf(os.Stderr, "[run] creating worktree %s at %s\n", displayPath(repoRoot, worktreeDir), ref)
	if err := os.RemoveAll(worktreeDir); err != nil {
		return err
	}
	if err := gitWorktreeAdd(ctx, repoRoot, worktreeDir, ref); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "[run] filtering test files in %s\n", displayPath(repoRoot, worktreeDir))
	if err := prepareTestFiles(worktreeDir, selectedTestFiles); err != nil {
		return err
	}
	return nil
}

func runMutationShard(ctx context.Context, repoRoot string, mutatorPath string, cfg config, runDir string, selectedTestFiles []string, targets []string, index int, reusePreparedWorktree bool) (shardResult, error) {
	worktreeDir := filepath.Join(runDir, fmt.Sprintf("worktree-%02d", index+1))
	defer func() {
		_ = gitWorktreeRemove(context.WithoutCancel(ctx), worktreeDir)
	}()
	if !reusePreparedWorktree {
		if err := prepareShardWorktree(ctx, repoRoot, worktreeDir, cfg.ref, selectedTestFiles); err != nil {
			return shardResult{}, err
		}
	}

	args := []string{
		"--exec", fmt.Sprintf("go run ./cmd/tools/mutationtest -output-root %s", runDir),
		"--exec-timeout", cfg.timeout,
	}
	args = append(args, targets...)
	logPath := filepath.Join(runDir, fmt.Sprintf("shard-%02d.log", index+1))
	logFile, err := os.Create(logPath)
	if err != nil {
		return shardResult{}, err
	}
	defer logFile.Close()
	shardLabel := fmt.Sprintf("[shard %d]", index+1)
	fmt.Fprintf(os.Stderr, "%s starting (%d source files, log=%s)\n", shardLabel, len(targets), displayPath(repoRoot, logPath))
	cmd := exec.CommandContext(ctx, mutatorPath, args...)
	cmd.Dir = worktreeDir
	cmd.Env = append(os.Environ(),
		"GOFLAGS=",
		"MUTATION_OUTPUT_PREFIX="+shardLabel,
		"MUTATION_TEST_TAGS="+cfg.testTags,
		"MUTATE_TIMEOUT="+cfg.timeout,
	)
	startedAt := time.Now()
	progressWriter := newProgressWriter(index+1, logFile)
	cmd.Stdout = progressWriter
	cmd.Stderr = progressWriter
	err = cmd.Run()
	stats := progressWriter.Stats()
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			fmt.Fprintf(os.Stderr, "%s finished in %s with exit code %d\n", shardLabel, time.Since(startedAt).Round(time.Second), exitErr.ExitCode())
			return shardResult{index: index, exitCode: exitErr.ExitCode(), logPath: logPath, stats: stats}, nil
		}
		return shardResult{}, err
	}
	fmt.Fprintf(os.Stderr, "%s finished in %s\n", shardLabel, time.Since(startedAt).Round(time.Second))
	return shardResult{index: index, exitCode: 0, logPath: logPath, stats: stats}, nil
}

type shardResult struct {
	index    int
	exitCode int
	logPath  string
	err      error
	stats    progressStats
}
