package mutationtest

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type config struct {
	mutatorPath   string
	outputRoot    string
	ref           string
	includeFiles  string
	excludeFiles  string
	testFiles     string
	testTags      string
	timeout       string
	shardLevel    int
	shardLevelRaw string
}

func parseConfig(args []string) (config, int, bool) {
	fs := flag.NewFlagSet("mutationtest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := config{}
	fs.StringVar(&cfg.mutatorPath, "mutator", "", "path to the go-mutesting binary")
	fs.StringVar(&cfg.outputRoot, "output-root", "", "directory for mutation test output")
	fs.StringVar(&cfg.ref, "ref", "", "git ref used for the temporary worktree")
	fs.StringVar(&cfg.includeFiles, "include-files", "", "space-separated source file paths or glob patterns to mutate")
	fs.StringVar(&cfg.excludeFiles, "exclude-files", "", "space-separated source file paths or glob patterns to exclude from mutation")
	fs.StringVar(&cfg.testFiles, "test-files", "", "space-separated test file paths or glob patterns")
	fs.StringVar(&cfg.testTags, "test-tags", "", "build tags passed to go test")
	fs.StringVar(&cfg.timeout, "timeout", "", "per-mutation timeout in seconds")
	fs.StringVar(&cfg.shardLevelRaw, "shard-level", "", "number of shards to run in parallel")

	err := fs.Parse(args)
	if err != nil {
		stderrLogger.Println(err)
		return config{}, exitMutationSkipped, false
	}
	if strings.TrimSpace(cfg.ref) == "" {
		cfg.ref = "HEAD"
	}
	if strings.TrimSpace(cfg.testTags) == "" {
		cfg.testTags = "test_dep"
	}
	if strings.TrimSpace(cfg.timeout) == "" {
		cfg.timeout = "180"
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
	if strings.TrimSpace(cfg.mutatorPath) == "" {
		stderrLogger.Println("-mutator must be set")
		return config{}, exitMutationSkipped, false
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

func (cfg config) print(repoRoot string, effectiveShardCount int, mutationFiles []string, selectedTestFiles []string) {
	stdoutLogger.Println("Configuration")
	stdoutLogger.Printf("  ref:        %s", cfg.ref)
	stdoutLogger.Printf("  mutator:    %s", displayPath(repoRoot, cfg.mutatorPath))
	stdoutLogger.Printf("  timeout:    %ss", cfg.timeout)
	stdoutLogger.Printf("  shards:     %d requested, %d effective", cfg.shardLevel, effectiveShardCount)
	stdoutLogger.Printf("  output:     %s", displayPath(repoRoot, cfg.outputRoot))
	stdoutLogger.Printf("  test cmd:   go test -count=1 -timeout <mutator-timeout> -tags %s ./...", cfg.testTags)
	printFileList("Source Files", mutationFiles)
	printFileList("Test Files", selectedTestFiles)
}

func parseExecOutputRoot(args []string) (string, error) {
	fs := flag.NewFlagSet("mutationtest-exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var outputRoot string
	fs.StringVar(&outputRoot, "output-root", "", "")
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	if strings.TrimSpace(outputRoot) == "" {
		return "", fmt.Errorf("-output-root must be set")
	}
	return outputRoot, nil
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
