package mutationtest

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type config struct {
	outputRoot       string
	ref              string
	includeFiles     string
	excludeFiles     string
	testFiles        string
	testTags         string
	mutations        string
	excludeMutations string
	listMutations    bool
	timeout          time.Duration
	runTimeout       time.Duration
	shardLevel       int
	shardLevelRaw    string
}

func parseConfig(args []string) (config, int, bool) {
	fs := flag.NewFlagSet("mutationtest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := config{}
	fs.StringVar(&cfg.outputRoot, "output-root", "", "directory for mutation test output")
	fs.StringVar(&cfg.ref, "ref", "", "git ref used for the temporary worktree")
	fs.StringVar(&cfg.includeFiles, "include-files", "", "space-separated source file paths or glob patterns to mutate")
	fs.StringVar(&cfg.excludeFiles, "exclude-files", "", "space-separated source file paths or glob patterns to exclude from mutation")
	fs.StringVar(&cfg.testFiles, "test-files", "", "space-separated test file paths or glob patterns")
	fs.StringVar(&cfg.testTags, "test-tags", "", "build tags passed to go test")
	fs.StringVar(&cfg.mutations, "mutations", "", "space-separated mutation operator names, categories, default, or all")
	fs.StringVar(&cfg.excludeMutations, "exclude-mutations", "", "space-separated mutation operators or categories to exclude")
	fs.BoolVar(&cfg.listMutations, "list-mutations", false, "list supported mutation operators")
	fs.DurationVar(&cfg.timeout, "timeout", 3*time.Minute, "per-mutation timeout")
	fs.DurationVar(&cfg.runTimeout, "run-timeout", 0, "maximum runtime for the entire mutation run (0 disables the limit)")
	fs.StringVar(&cfg.shardLevelRaw, "shard-level", "", "number of shards to run in parallel")

	err := fs.Parse(args)
	if err != nil {
		stderrLogger.Println(err)
		return config{}, exitMutationSkipped, false
	}
	if cfg.listMutations {
		return cfg, 0, true
	}
	if strings.TrimSpace(cfg.ref) == "" {
		cfg.ref = "HEAD"
	}
	if strings.TrimSpace(cfg.testTags) == "" {
		cfg.testTags = "test_dep"
	}
	if cfg.timeout <= 0 {
		stderrLogger.Println("-timeout must be greater than zero")
		return config{}, exitMutationSkipped, false
	}
	if cfg.runTimeout < 0 {
		stderrLogger.Println("-run-timeout must not be negative")
		return config{}, exitMutationSkipped, false
	}
	if strings.TrimSpace(cfg.shardLevelRaw) == "" {
		cfg.shardLevel = 1
	} else {
		shardLevel, err := strconv.Atoi(cfg.shardLevelRaw)
		if err != nil {
			stderrLogger.Println("-shard-level must be an integer")
			return config{}, exitMutationSkipped, false
		}
		if shardLevel < 1 {
			shardLevel = 1
		}
		cfg.shardLevel = shardLevel
	}
	if strings.TrimSpace(cfg.outputRoot) == "" {
		stderrLogger.Println("-output-root must be set")
		return config{}, exitMutationSkipped, false
	}
	if strings.TrimSpace(cfg.includeFiles) == "" {
		stderrLogger.Println("-include-files must be set")
		return config{}, exitMutationSkipped, false
	}
	if strings.TrimSpace(cfg.testFiles) == "" {
		stderrLogger.Println("-test-files must be set")
		return config{}, exitMutationSkipped, false
	}
	return cfg, 0, true
}

func (cfg config) print(repoRoot string, effectiveShardCount int, operatorNames []string, mutationFiles []string, selectedTestFiles []string) {
	stdoutLogger.Println("Configuration")
	stdoutLogger.Printf("  ref:        %s", cfg.ref)
	stdoutLogger.Printf("  timeout:    %s", cfg.timeout)
	stdoutLogger.Printf("  run timeout: %s", formatOptionalDuration(cfg.runTimeout))
	stdoutLogger.Printf("  shards:     %d requested, %d effective", cfg.shardLevel, effectiveShardCount)
	stdoutLogger.Printf("  output:     %s", displayPath(repoRoot, cfg.outputRoot))
	stdoutLogger.Printf("  test cmd:   go test -count=1 -timeout %s -tags %s ./...", cfg.timeout, cfg.testTags)
	printFileList("Mutation Operators", operatorNames)
	printFileList("Source Files", mutationFiles)
	printFileList("Test Files", selectedTestFiles)
}

func formatOptionalDuration(duration time.Duration) string {
	if duration == 0 {
		return "unlimited"
	}
	return duration.String()
}

func displayPath(repoRoot string, path string) string {
	if !filepath.IsAbs(path) {
		return filepath.ToSlash(path)
	}
	relPath, err := filepath.Rel(repoRoot, path)
	if err != nil || relPath == ".." || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relPath)
}
