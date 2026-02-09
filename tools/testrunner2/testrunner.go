package testrunner2

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	codeCoverageExtension = ".cover.out"
	logPrefix             = "[runner] "

	testCommand       = "test"
	reportLogsCommand = "report-logs"
)

const (
	maxAttemptsFlag      = "--max-attempts"
	coverProfileFlag     = "-coverprofile"
	junitReportFlag      = "--junitfile"
	runTimeoutFlag       = "--run-timeout"
	stuckTestTimeoutFlag = "--stuck-test-timeout"
	parallelismFlag      = "--parallelism"
	timeoutFlag          = "-timeout"
	tagsFlag             = "-tags"
	logDirFlag           = "--log-dir"
	groupByFlag          = "--group-by"
	runFlag              = "-run"
)

// GroupMode defines how tests are grouped for execution.
type GroupMode string

const (
	GroupByTest GroupMode = "test" // each TestXxx function runs separately (for functional tests)
	GroupByNone GroupMode = "none" // run go test directly on all packages without precompilation
)

type config struct {
	log              func(format string, v ...any)
	exec             execFunc
	junitReportPath  string
	coverProfilePath string
	buildTags        string
	logDir           string        // directory for log files
	timeout          time.Duration // overall timeout for the test run
	runTimeout       time.Duration // timeout per test run file
	stuckTestTimeout time.Duration // abort test if individual test runs longer than this
	runFilter        string        // -run filter pattern (used to filter tests)
	maxAttempts      int
	parallelism      int
	totalShards      int       // for CI sharding
	shardIndex       int       // for CI sharding
	testBinaryArgs   []string  // args to pass to test binary (after -args)
	env              []string  // extra environment variables for child processes
	groupBy          GroupMode // how to group tests: test, none
}

// Main is the entry point for the testrunner tool.
//
//nolint:revive // deep-exit allowed in Main
func Main() {
	log.SetOutput(os.Stdout)
	log.SetPrefix(logPrefix)
	log.SetFlags(log.Ltime)
	ctx := context.Background()

	if len(os.Args) < 2 {
		log.Fatal("expected at least 2 arguments")
	}

	command := os.Args[1]

	cfg := defaultConfig()
	cfg.log = log.Printf
	args, err := parseArgs(os.Args[2:], &cfg)
	if err != nil {
		log.Fatalf("failed to parse command line options: %v", err)
	}

	switch command {
	case testCommand:
		if cfg.junitReportPath == "" {
			log.Fatalf("missing required argument %q", junitReportFlag)
		}
		if cfg.coverProfilePath == "" {
			log.Fatalf("missing required argument %q", coverProfileFlag)
		}
		if cfg.logDir == "" {
			log.Fatalf("missing required argument %q", logDirFlag)
		}
		if cfg.groupBy == "" {
			log.Fatalf("missing required argument %q: use 'test' for compiled per-test execution, 'none' for direct go test", groupByFlag)
		}
		r := newRunner(cfg)
		if err := r.runTests(ctx, args); err != nil {
			log.Fatalf(logPrefix+"failed:\n%v", err)
		}
	case reportLogsCommand:
		if cfg.logDir == "" {
			log.Fatalf("missing required argument %q", logDirFlag)
		}
		if err := reportLogs(cfg.logDir, cfg.log); err != nil {
			log.Fatalf(logPrefix+"failed:\n%v", err)
		}
	default:
		log.Fatalf("unknown command %q", command)
	}
}

// --- arg parsing ---

type flagDefinition struct {
	runnerOnly bool // If true, the flag is consumed by the runner and not passed to go test
	handle     func(value string, cfg *config) error
}

func init() {
	for name := range flagDefinitions {
		if !strings.HasPrefix(name, "-") {
			panic(fmt.Sprintf("flag %q must start with -", name))
		}
		if strings.Contains(name, "=") {
			panic(fmt.Sprintf("flag %q must not contain =; it is appended during lookup", name))
		}
	}
}

var flagDefinitions = map[string]flagDefinition{
	maxAttemptsFlag: {
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
	parallelismFlag: {
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
	runTimeoutFlag: {
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
	stuckTestTimeoutFlag: {
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
	timeoutFlag: {
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
	runFlag: {
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			cfg.runFilter = strings.Trim(after, "'\"")
			cfg.log("run filter set to %s", cfg.runFilter)
			return nil
		},
	},
	tagsFlag: {
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			cfg.buildTags = after
			return nil
		},
	},
	logDirFlag: {
		runnerOnly: true,
		handle: func(after string, cfg *config) error {
			cfg.logDir = after
			cfg.log("log directory set to %s", cfg.logDir)
			return nil
		},
	},
	groupByFlag: {
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
	coverProfileFlag: {
		runnerOnly: false,
		handle: func(after string, cfg *config) error {
			cfg.coverProfilePath = after
			return nil
		},
	},
	junitReportFlag: {
		runnerOnly: false,
		handle: func(after string, cfg *config) error {
			cfg.junitReportPath = after
			return nil
		},
	},
}

// defaultConfig returns a config with default values
// and values from environment variables
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

// parseArgs parses command-line arguments into cfg.
// It extracts testrunner-specific flags and returns remaining args for go test.
func parseArgs(args []string, cfg *config) ([]string, error) {
	var sanitizedArgs []string
	for _, arg := range args {
		if fd := lookupFlag(arg); fd != nil {
			// Extract value after "key="
			value := arg[strings.IndexByte(arg, '=')+1:]
			if err := fd.handle(value, cfg); err != nil {
				return nil, err
			}
			if !fd.runnerOnly {
				sanitizedArgs = append(sanitizedArgs, arg)
			}
		} else {
			sanitizedArgs = append(sanitizedArgs, arg)
		}
	}

	// If --run-timeout wasn't set, default to the total timeout
	if cfg.runTimeout == 0 && cfg.timeout > 0 {
		cfg.runTimeout = cfg.timeout
		cfg.log("run timeout defaulting to total timeout: %v", cfg.runTimeout)
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

// filterBaseArgs separates runner-managed flags from go test passthrough args.
// It returns whether -race was present and the remaining args to pass to go test.
func filterBaseArgs(baseArgs []string) (race bool, extraArgs []string) {
	for _, arg := range baseArgs {
		switch {
		case arg == "-race":
			race = true
		case arg == "--":
			// stop processing; everything after is already in testBinaryArgs
		case lookupFlag(arg) != nil:
			// Already managed by the runner; skip.
		default:
			extraArgs = append(extraArgs, arg)
		}
	}
	return
}

// lookupFlag returns the flagDefinition for arg, or nil if arg is not a known flag.
// All flags use "key=value" syntax, so we split on the first "=" for a map lookup.
func lookupFlag(arg string) *flagDefinition {
	if argName, _, ok := strings.Cut(arg, "="); ok {
		if fd, ok := flagDefinitions[argName]; ok {
			return &fd
		}
	}
	return nil
}
