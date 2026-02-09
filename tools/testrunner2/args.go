package testrunner2

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	maxAttemptsFlag      = "--max-attempts="
	coverProfileFlag     = "-coverprofile="
	junitReportFlag      = "--junitfile="
	runTimeoutFlag       = "--run-timeout="
	stuckTestTimeoutFlag = "--stuck-test-timeout="
	parallelismFlag      = "--parallelism="
	timeoutFlag          = "-timeout="
	tagsFlag             = "-tags="
	logDirFlag           = "--log-dir="
	groupByFlag          = "--group-by="
	runFlag              = "-run="
)

// GroupMode defines how tests are grouped for execution.
type GroupMode string

const (
	GroupByTest GroupMode = "test" // each TestXxx function runs separately (for functional tests)
	GroupByNone GroupMode = "none" // run go test directly on all packages without precompilation
)

type flagDefinition struct {
	prefix     string
	runnerOnly bool // If true, the flag is consumed by the runner and not passed to go test
	handle     func(value string, cfg *config) error
}

var flagDefinitions = []flagDefinition{
	{
		prefix:     maxAttemptsFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			n, err := strconv.Atoi(after)
			if err != nil {
				return fmt.Errorf("invalid argument %s: %w", maxAttemptsFlag, err)
			}
			if n == 0 {
				return fmt.Errorf("invalid argument %s: must be greater than zero", maxAttemptsFlag)
			}
			cfg.maxAttempts = n
			cfg.log("max attempts set to %d", cfg.maxAttempts)
			return nil
		},
	},
	{
		prefix:     parallelismFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			n, err := strconv.Atoi(after)
			if err != nil {
				return fmt.Errorf("invalid argument %s: %w", parallelismFlag, err)
			}
			if n <= 0 {
				return fmt.Errorf("invalid argument %s: must be greater than zero", parallelismFlag)
			}
			cfg.parallelism = n
			cfg.log("parallelism set to %d", cfg.parallelism)
			return nil
		},
	},
	{
		prefix:     runTimeoutFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			d, err := time.ParseDuration(after)
			if err != nil {
				return fmt.Errorf("invalid argument %s: %w", runTimeoutFlag, err)
			}
			cfg.runTimeout = d
			cfg.log("run timeout set to %v", cfg.runTimeout)
			return nil
		},
	},
	{
		prefix:     stuckTestTimeoutFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			d, err := time.ParseDuration(after)
			if err != nil {
				return fmt.Errorf("invalid argument %s: %w", stuckTestTimeoutFlag, err)
			}
			cfg.stuckTestTimeout = d
			cfg.log("stuck test timeout set to %v", cfg.stuckTestTimeout)
			return nil
		},
	},
	{
		prefix:     timeoutFlag,
		runnerOnly: false,
		handle: func(after string, cfg *config) error {
			d, err := time.ParseDuration(after)
			if err != nil {
				return fmt.Errorf("invalid argument %s: %w", timeoutFlag, err)
			}
			cfg.timeout = d
			cfg.log("total timeout set to %v", cfg.timeout)
			return nil
		},
	},
	{
		prefix:     runFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			cfg.runFilter = strings.Trim(after, "'\"")
			cfg.log("run filter set to %s", cfg.runFilter)
			return nil
		},
	},
	{
		prefix:     tagsFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			cfg.buildTags = after
			return nil
		},
	},
	{
		prefix:     logDirFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			cfg.logDir = after
			cfg.log("log directory set to %s", cfg.logDir)
			return nil
		},
	},
	{
		prefix:     groupByFlag,
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			switch GroupMode(after) {
			case GroupByTest, GroupByNone:
				cfg.groupBy = GroupMode(after)
			default:
				return fmt.Errorf("invalid argument %s: must be 'test' or 'none'", groupByFlag)
			}
			cfg.log("group-by mode set to %s", cfg.groupBy)
			return nil
		},
	},
	{
		prefix:     coverProfileFlag,
		runnerOnly: false,
		handle: func(after string, cfg *config) error {
			cfg.coverProfilePath = after
			return nil
		},
	},
	{
		prefix:     junitReportFlag,
		runnerOnly: false,
		handle: func(after string, cfg *config) error {
			cfg.junitReportPath = after
			return nil
		},
	},
}

// parseConfig parses command-line arguments and returns a config.
// It extracts testrunner-specific flags and sanitizes remaining args for go test.
func parseConfig(command string, args []string, cfg *config) ([]string, error) {
	var sanitizedArgs []string
	for _, arg := range args {
		matched := false
		for _, fd := range flagDefinitions {
			if after, ok := strings.CutPrefix(arg, fd.prefix); ok {
				if err := fd.handle(after, cfg); err != nil {
					return nil, err
				}
				if !fd.runnerOnly {
					sanitizedArgs = append(sanitizedArgs, arg)
				}
				matched = true
				break
			}
		}
		if !matched {
			sanitizedArgs = append(sanitizedArgs, arg)
		}
	}

	// If --run-timeout wasn't set, default to the total timeout
	if cfg.runTimeout == 0 && cfg.timeout > 0 {
		cfg.runTimeout = cfg.timeout
		cfg.log("run timeout defaulting to total timeout: %v", cfg.runTimeout)
	}

	switch command {
	case testCommand:
		if cfg.junitReportPath == "" {
			return nil, fmt.Errorf("missing required argument %q", junitReportFlag)
		}
		if cfg.coverProfilePath == "" {
			return nil, fmt.Errorf("missing required argument %q", coverProfileFlag)
		}
		if cfg.logDir == "" {
			return nil, fmt.Errorf("missing required argument %q", logDirFlag)
		}
		if cfg.groupBy == "" {
			return nil, fmt.Errorf("missing required argument %q: use 'test' for compiled per-test execution, 'none' for direct go test", groupByFlag)
		}
	case reportLogsCommand:
		if cfg.logDir == "" {
			return nil, fmt.Errorf("missing required argument %q", logDirFlag)
		}
	default:
		return nil, fmt.Errorf("unknown command %q", command)
	}

	return sanitizedArgs, nil
}

// parseTestArgs separates test directories from other args.
// Returns testDirs, baseArgs (for compilation), and testBinaryArgs (args after -args for test execution).
func parseTestArgs(args []string) (testDirs []string, baseArgs []string, testBinaryArgs []string) {
	var inTestArgs bool
	for _, arg := range args {
		if arg == "-args" {
			inTestArgs = true
			continue // don't include -args itself in any output
		}
		if inTestArgs {
			testBinaryArgs = append(testBinaryArgs, arg)
			continue
		}
		// Test directories start with ./ or /
		if strings.HasPrefix(arg, "./") || strings.HasPrefix(arg, "/") {
			testDirs = append(testDirs, arg)
		} else {
			baseArgs = append(baseArgs, arg)
		}
	}
	return
}

// defaultConfig returns a config with default values.
func defaultConfig() config {
	cfg := config{
		maxAttempts: 1,
		parallelism: runtime.NumCPU(),
		totalShards: 1,
		shardIndex:  0,
	}

	if v := os.Getenv("TEST_RUNNER_SHARDS_TOTAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.totalShards = n
		}
	}
	if v := os.Getenv("TEST_RUNNER_SHARD_INDEX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.shardIndex = n
		}
	}
	if v := os.Getenv("TEST_RUNNER_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.parallelism = n
		}
	}
	return cfg
}