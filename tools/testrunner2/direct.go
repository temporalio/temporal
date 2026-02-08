package testrunner2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// runDirectMode runs all tests via `go test` without precompilation, using the
// scheduler for retry lifecycle. This is the GroupByNone path.
func (r *runner) runDirectMode(ctx context.Context, testDirs []string, baseArgs []string) error {
	r.log("running in 'none' mode - executing go test directly without precompilation")

	// Create log directory
	if err := os.MkdirAll(r.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	r.log("log directory: %s", r.logDir)

	// Parse base args: extract flags the runner manages and collect go test flags.
	var race bool
	var extraArgs []string
	for _, arg := range baseArgs {
		switch {
		case arg == "-race":
			race = true
		case arg == "--":
			// stop processing; everything after is already in testBinaryArgs
		case strings.HasPrefix(arg, "-timeout="),
			strings.HasPrefix(arg, "-tags="),
			strings.HasPrefix(arg, "-coverprofile="),
			strings.HasPrefix(arg, "--junitfile="),
			strings.HasPrefix(arg, "--log-dir="),
			strings.HasPrefix(arg, "--max-attempts="),
			strings.HasPrefix(arg, "--run-timeout="),
			strings.HasPrefix(arg, "--stuck-test-timeout="),
			strings.HasPrefix(arg, "--parallelism="),
			strings.HasPrefix(arg, "--group-by="),
			strings.HasPrefix(arg, "-run="):
			// Already managed by the runner; skip.
		case strings.HasPrefix(arg, "-"):
			// Pass through other go test flags (e.g., -shuffle, -count).
			extraArgs = append(extraArgs, arg)
		default:
			// Pass through non-flag values that follow flags (e.g., "on" in "-shuffle on").
			extraArgs = append(extraArgs, arg)
		}
	}

	// Normalize package paths (ensure ./ prefix)
	pkgs := make([]string, len(testDirs))
	for i, dir := range testDirs {
		if !strings.HasPrefix(dir, "./") && !strings.HasPrefix(dir, "/") {
			pkgs[i] = "./" + dir
		} else {
			pkgs[i] = dir
		}
	}
	r.log("test packages: %v", pkgs)

	// Create result collector and progress tracker
	r.collector = &resultCollector{}
	r.progress = &progressTracker{total: 1}

	// Run via scheduler (at least 2 workers for mid-stream retries)
	sched := newScheduler(max(2, r.parallelism))
	sched.run(ctx, []*queueItem{r.newExecItem(r.directExecConfig(pkgs, race, extraArgs, 1, "", ""))})

	// Finalize report
	if err := r.finalizeReport(r.collector.junitReports); err != nil {
		return err
	}

	if len(r.collector.errors) > 0 {
		return errors.Join(r.collector.errors...)
	}
	r.log("test run completed")
	return nil
}

// directExecConfig creates an execConfig for running go test directly.
func (r *runner) directExecConfig(pkgs []string, race bool, extraArgs []string, attempt int, runFilter, skipFilter string) execConfig {
	desc := "all"

	// When there's a run filter, this is a retry for specific tests. Multiple
	// retries at the same attempt number (from stream retries) need unique file
	// names to avoid overwriting each other's log and JUnit files.
	fileSuffix := ""
	if runFilter != "" || skipFilter != "" {
		fileSuffix = fmt.Sprintf("_%d", r.directRetrySeq.Add(1))
	}

	retry := r.buildRetryHandler(func(plan retryPlan, attempt int) *queueItem {
		runF := buildTestFilterPattern(plan.tests)
		skipF := buildTestFilterPattern(plan.skipTests)
		return r.newExecItem(r.directExecConfig(pkgs, race, extraArgs, attempt, runF, skipF))
	})

	timeout := r.effectiveTimeout()

	return execConfig{
		startProcess: func(ctx context.Context, output io.Writer) commandResult {
			return runDirectGoTest(ctx, r.exec, runDirectGoTestInput{
				pkgs:         pkgs,
				buildTags:    r.buildTags,
				race:         race,
				coverProfile: r.coverProfilePath,
				timeout:      timeout,
				env:          append(r.env, fmt.Sprintf("TEMPORAL_TEST_ATTEMPT=%d", attempt)),
				output:       output,
				runFilter:    runFilter,
				skipFilter:   skipFilter,
				extraArgs:    extraArgs,
			}, func(command string) {
				r.console.WriteGrouped(
					fmt.Sprintf("%s%s 🚀 %s (attempt %d)", logPrefix, time.Now().Format("15:04:05"), desc, attempt),
					"$ "+command+"\n",
				)
			})
		},
		label:            desc,
		attempt:          attempt,
		logPath:          filepath.Join(r.logDir, fmt.Sprintf("all_mode_attempt_%d%s.log", attempt, fileSuffix)),
		junitPath:        filepath.Join(r.logDir, fmt.Sprintf("junit_all_attempt_%d%s.xml", attempt, fileSuffix)),
		streamRetries: true,
		retry:         retry,
	}
}
