package testrunner2

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// compiledExecConfig creates an execConfig for running a precompiled test binary.
func (r *runner) compiledExecConfig(unit workUnit, binaryPath string, attempt int) execConfig {
	timeout := r.effectiveTimeout()

	desc := unit.label
	coverProfile := fmt.Sprintf("%s_run_%d%s",
		strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
		attempt,
		codeCoverageExtension)
	coverProfile, _ = filepath.Abs(coverProfile)

	junitFilename := fmt.Sprintf("junit_%s.xml", uuid.New().String())
	junitPath := filepath.Join(r.logDir, junitFilename)
	logPath, _ := filepath.Abs(buildLogFilename(r.logDir, desc))

	fileNames := make([]string, len(unit.files))
	for i, tf := range unit.files {
		fileNames[i] = filepath.Base(tf.path)
	}

	retryForFailures, retryForCrash, retryForUnknown := r.buildRetryHooks(
		func(plan retryPlan, attempt int) *queueItem {
			var wu workUnit
			if plan.tests != nil && plan.skipTests == nil {
				// Simple failure retry: build a retry unit from the failed test names
				var failedTests []testCase
				for _, name := range plan.tests {
					failedTests = append(failedTests, testCase{name: name, attempts: attempt})
				}
				retryUnit := buildRetryUnit(unit, failedTests)
				if retryUnit == nil {
					return nil
				}
				wu = *retryUnit
			} else {
				// Crash/quarantine retry: use the original unit with run/skip lists.
				// Merge plan's skip list with the current unit's to accumulate
				// skips across attempts (so subtests that passed in earlier
				// attempts don't re-run).
				wu = workUnit{
					pkg:       unit.pkg,
					files:     unit.files,
					label:     unit.label,
					skipTests: mergeUnique(unit.skipTests, plan.skipTests),
				}
				if plan.tests != nil {
					wu.tests = make([]testCase, len(plan.tests))
					for i, name := range plan.tests {
						wu.tests[i] = testCase{name: name, attempts: attempt - 1}
					}
				} else {
					wu.tests = incrementAttempts(unit.tests, attempt-1)
				}
			}
			// Don't emit if skip would cover every test (vacuous plan)
			if wouldSkipAll(wu.tests, wu.skipTests) {
				return nil
			}
			return r.newExecItem(r.compiledExecConfig(wu, binaryPath, attempt))
		},
	)

	return execConfig{
		startProcess: func(ctx context.Context, output io.Writer) commandResult {
			return executeTest(ctx, r.exec, executeTestInput{
				binary:       binaryPath,
				pkgDir:       unit.pkg,
				tests:        unit.tests,
				skipPattern:  buildTestFilterPattern(unit.skipTests),
				timeout:      timeout,
				coverProfile: coverProfile,
				extraArgs:    r.testBinaryArgs,
				env:          append(r.env, fmt.Sprintf("TEMPORAL_TEST_ATTEMPT=%d", attempt)),
				output:       output,
			}, func(command string) {
				r.console.WriteGrouped(
					fmt.Sprintf("%s%s 🚀 %s (attempt %d)", logPrefix, time.Now().Format("15:04:05"), desc, attempt),
					"$ "+command+"\n",
				)
			})
		},
		label:     desc,
		attempt:   attempt,
		logPath:   logPath,
		junitPath: junitPath,
		logHeader: &logFileHeader{
			Package:   unit.pkg,
			TestFiles: fileNames,
			Attempt:   attempt,
			Started:   time.Now(),
			Command:   binaryPath,
		},
		streamRetries:    false, // one test per process
		retryForFailures: retryForFailures,
		retryForCrash:    retryForCrash,
		retryForUnknown:  retryForUnknown,
	}
}

// createCompileItems creates compile queue items for each package.
func (r *runner) createCompileItems(tp *testPackage, units []workUnit, binDir string, baseArgs []string) []*queueItem {
	// Group units by package for compilation
	unitsByPkg := make(map[string][]workUnit)
	for _, u := range units {
		unitsByPkg[u.pkg] = append(unitsByPkg[u.pkg], u)
	}

	// Sort units by file size (largest first) for better load balancing
	for _, pkgUnits := range unitsByPkg {
		slices.SortFunc(pkgUnits, func(a, b workUnit) int {
			return cmp.Compare(unitFileSize(b), unitFileSize(a)) // descending
		})
	}

	pkgs := slices.Sorted(maps.Keys(unitsByPkg))

	items := make([]*queueItem, 0, len(pkgs))
	for _, pkg := range pkgs {
		binName := strings.ReplaceAll(strings.TrimPrefix(pkg, "./"), "/", "_") + ".test"
		binaryPath := filepath.Join(binDir, binName)

		items = append(items, r.newCompileItem(pkg, unitsByPkg[pkg], binaryPath, baseArgs))
	}
	return items
}

func (r *runner) newCompileItem(pkg string, units []workUnit, binaryPath string, baseArgs []string) *queueItem {
	var lc *logCapture
	var compileErr error

	return &queueItem{
		run: func(ctx context.Context, emit func(...*queueItem)) {
			// Create log capture for compile output
			logPath := buildCompileLogFilename(r.logDir)
			var err error
			lc, err = newLogCapture(logCaptureConfig{
				LogPath: logPath,
			})
			if err != nil {
				compileErr = fmt.Errorf("failed to create compile log file %s: %w", logPath, err)
				r.collector.addError(compileErr)
				return
			}

			result := compileTest(ctx, r.exec, compileTestInput{
				pkg:        pkg,
				binaryPath: binaryPath,
				buildTags:  r.buildTags,
				baseArgs:   baseArgs,
				env:        r.env,
				output:     lc,
			}, func(command string) {
				r.console.WriteGrouped(fmt.Sprintf("%s%s 🚀 compiling %s", logPrefix, time.Now().Format("15:04:05"), pkg), "$ "+command+"\n")
			})

			if result.exitCode != 0 {
				compileErr = fmt.Errorf("failed to compile %s (exit code %d)", pkg, result.exitCode)
				r.collector.addError(compileErr)
			}

			// Get output for display
			outputStr, err := lc.GetOutput()
			if err != nil {
				r.log("warning: failed to get compile output: %v", err)
			}

			header := fmt.Sprintf("%s%s 🔨 compiled %s", logPrefix, time.Now().Format("15:04:05"), pkg)
			r.console.WriteGrouped(header, outputStr)

			_ = lc.Close()

			// Emit test items if compilation succeeded
			if compileErr != nil {
				return
			}

			r.log("starting test run for %s: units=%d", pkg, len(units))

			var items []*queueItem
			for _, unit := range units {
				items = append(items, r.newExecItem(r.compiledExecConfig(unit, binaryPath, 1)))
			}
			emit(items...)
		},
	}
}
