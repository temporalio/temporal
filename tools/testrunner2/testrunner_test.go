package testrunner2

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	t.Parallel()

	t.Run("passing tests", func(t *testing.T) {
		t.Parallel()

		t.Run("group mode: test", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/passing"}, "--group-by=test")
			require.NoError(t, res.err)

			assertJUnit(t, res,
				passed("TestPass1"),
				passed("TestPass2"),
				passed("TestPass3"),
			)
			assertConsole(t, res,
				printed("starting scheduler with parallelism=1"),
				printed("🚀 compiling", "./testpkg/passing"),
				printed("$", "go test -c", "./testpkg/passing"),
				printed("🔨 compiled", "./testpkg/passing"),
				printed("🚀", "TestPass1", "attempt 1"),
				printed("$", ".test", "-test.run ^TestPass1$"),
				printed("✅ [1/3]", "TestPass1", "attempt=1", "passed=1/1"),
				printed("🚀", "TestPass2", "attempt 1"),
				printed("$", ".test", "-test.run ^TestPass2$"),
				printed("✅ [2/3]", "TestPass2", "attempt=1", "passed=1/1"),
				printed("🚀", "TestPass3", "attempt 1"),
				printed("$", ".test", "-test.run ^TestPass3$"),
				printed("✅ [3/3]", "TestPass3", "attempt=1", "passed=1/1"),
				printed("test run completed"),
				notPrinted("failure="),
			)
			assertNoLogFiles(t, res)
		})

		t.Run("group mode: none", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/passing"}, "--group-by=none")
			require.NoError(t, res.err)

			assertJUnit(t, res,
				passed("TestPass1"),
				passed("TestPass2"),
				passed("TestPass3"),
			)
			assertConsole(t, res,
				printed("running in 'none' mode"),
				printed("🚀", "all", "attempt 1"),
				printed("$", "go test", "./testpkg/passing"),
				printed("✅", "all", "attempt=1"),
				printed("test run completed"),
				notPrinted("go test -c"), // no compile step
			)
			assertNoLogFiles(t, res)
		})

		t.Run("group mode: none with -run filter", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/passing"}, "--group-by=none", "-run=TestPass1")
			require.NoError(t, res.err)

			assertJUnit(t, res,
				passed("TestPass1"),
			)
			assertConsole(t, res,
				printed("running in 'none' mode"),
				printed("🚀", "all", "attempt 1"),
				printed("$", "go test", "-run", "TestPass1", "./testpkg/passing"),
				printed("✅", "all", "attempt=1"),
				printed("test run completed"),
			)
			assertNoLogFiles(t, res)
		})
	})

	t.Run("failure: always-failing test", func(t *testing.T) {
		t.Parallel()

		t.Run("group mode: test", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/failing"}, "--group-by=test", "--max-attempts=2")
			require.Error(t, res.err)
			require.Contains(t, res.err.Error(), "failed on attempt 2")

			assertJUnit(t, res,
				passed("TestOK"),
				failed("TestAlwaysFails", "Failed"),
				failed("TestAlwaysFails (retry 1)", "Failed"),
			)
			assertConsole(t, res,
				printed("$", ".test", "-test.run ^TestAlwaysFails$"),
				printed("🔄 scheduling retry:", "^TestAlwaysFails$"), // mid-stream retry
				printed("❌️", "TestAlwaysFails", "attempt=1", "failure=failed"),
				printed("--- TestAlwaysFails"), // failed test name shown in body
				printed("always fails"),        // failure details shown in body
				printed("$", ".test", "-test.run ^TestOK$"),
				printed("✅", "TestOK", "attempt=1", "passed=1/1"),
				printed("🚀", "TestAlwaysFails", "attempt 2"),
				printed("$", ".test", "-test.run ^TestAlwaysFails$"),
				printed("❌️", "TestAlwaysFails", "attempt=2", "failure=failed"),
				notPrinted("test run completed"),
			)
			assertLogFiles(t, res, // attempt 1 + attempt 2 both fail
				file("TestAlwaysFails",
					"TESTRUNNER LOG",
					"Attempt:     1",
					"always fails",
					"FAIL",
				),
				file("TestAlwaysFails",
					"TESTRUNNER LOG",
					"Attempt:     2",
					"always fails",
					"FAIL",
				),
			)
		})

		t.Run("group mode: none", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/failing"}, "--group-by=none", "--max-attempts=2")
			require.Error(t, res.err)

			assertConsole(t, res,
				// Mid-stream retry fires before the first attempt finishes
				printed("🚀", "all", "attempt 1"),
				printed("$", "go test", "./testpkg/failing"),
				printed("🔄 scheduling retry:", "^TestAlwaysFails$"),
				printed("🚀", "all", "attempt 2"),
				printed("$", "go test", "-run", "^TestAlwaysFails$", "./testpkg/failing"),
				printed("❌️", "all", "attempt=1", "failure=failed"),
				printed("--- TestAlwaysFails"),
				printed("always fails"),
				printed("❌️", "all", "attempt=2"),
				notPrinted("go test -c"), // no compile step
			)
		})
	})

	t.Run("failure: flaky test", func(t *testing.T) {
		t.Parallel()

		t.Run("group mode: test", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/flaky"}, "--group-by=test", "--max-attempts=2")
			require.NoError(t, res.err, "flaky test should pass on retry")

			assertConsole(t, res,
				printed("$", ".test", "-test.run ^TestStable$"),
				printed("✅", "TestStable", "attempt=1", "passed=1/1"),
				printed("$", ".test", "-test.run ^TestFlaky$"),
				printed("🔄 scheduling retry:", "^TestFlaky$"), // mid-stream retry
				printed("❌️", "TestFlaky", "failure=failed"),
				printed("--- TestFlaky"),                     // failed test name shown in body
				printed("intentional first-attempt failure"), // failure details shown in body
				printed("$", ".test", "-test.run ^TestFlaky$"),
				printed("✅", "TestFlaky", "attempt=2", "passed=1/1"),
				printed("test run completed"),
			)
			assertLogFiles(t, res, // attempt 1 fails
				file("TestFlaky",
					"TESTRUNNER LOG",
					"Attempt:     1",
					"intentional first-attempt failure",
					"FAIL",
				),
			)
		})

		t.Run("group mode: none", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/flaky"}, "--group-by=none", "--max-attempts=2")
			require.NoError(t, res.err)

			assertConsole(t, res,
				printed("running in 'none' mode"),
				// Mid-stream retry fires before the first attempt finishes
				printed("🚀", "all", "attempt 1"),
				printed("$", "go test", "./testpkg/flaky"),
				printed("🔄 scheduling retry:", "^TestFlaky$"),
				printed("🚀", "all", "attempt 2"),
				printed("$", "go test", "-run", "^TestFlaky$", "./testpkg/flaky"),
				printed("❌️", "all", "attempt=1", "failure=failed"),
				printed("--- TestFlaky"),
				printed("intentional first-attempt failure"),
				printed("✅", "all", "attempt=2"),
				printed("test run completed"),
				notPrinted("go test -c"), // no compile step
			)
		})
	})

	t.Run("failure: flaky subtest", func(t *testing.T) {
		t.Parallel()

		// Only the leaf subtest TestSuite/FailChild should be retried, not the parent TestSuite.
		res := runIntegTest(t, []string{"./testpkg/subfail"}, "--group-by=test", "--max-attempts=2")
		require.NoError(t, res.err, "should pass on retry")

		assertConsole(t, res,
			printed("$", ".test", "-test.run ^TestSuite$"),
			printed("🔄 scheduling retry:", "^TestSuite$/^FailChild$"), // retry scheduled before end of test
			printed("❌️", "TestSuite", "failure=failed"),
			printed("$", ".test", "-test.run ^TestOK$"),
			printed("✅", "TestOK", "attempt=1", "passed=1/1"),
			// Retry must target only the leaf subtest, skipping PassChild
			printed("$", ".test", "-test.run ^TestSuite$/^FailChild$", "-test.skip ^TestSuite$/^PassChild$"),
			printed("✅", "TestSuite", "attempt=2", "passed=2/2"),
			printed("test run completed"),
		)
		assertLogFiles(t, res, // attempt 1 fails
			file("TestSuite",
				"TESTRUNNER LOG",
				"Attempt:     1",
				"intentional subtest failure",
				"FAIL",
			),
		)
	})

	t.Run("failure: crash", func(t *testing.T) {
		t.Parallel()

		// TestCrashOnce panics on attempt 1, passes on retry.
		res := runIntegTest(t, []string{"./testpkg/crashonce"}, "--group-by=test", "--max-attempts=2")

		assertJUnit(t, res,
			failed("TestCrashOnce", "panic"),
			passed("TestCrashOnce (retry 1)"),
		)
		assertConsole(t, res,
			printed("$", ".test", "-test.run ^TestCrashOnce$"),
			printed("❌️"),
			printed("PANIC:", "nil pointer dereference"),
			printed("🔄 scheduling retry:", "^TestCrashOnce$"), // post-exit crash recovery
			printed("$", ".test", "-test.run ^TestCrashOnce$"),
			printed("✅", "TestCrashOnce", "attempt=2", "passed=1/1"),
			printed("test run completed"),
		)
		assertLogFiles(t, res, // attempt 1 crashes
			file("TestCrashOnce",
				"TESTRUNNER LOG",
				"Attempt:     1",
				"nil pointer dereference",
			),
		)
	})

	t.Run("failure: timeout", func(t *testing.T) {
		t.Parallel()

		t.Run("retry subtests", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/subtimeout"}, "--group-by=test", "--max-attempts=2", "--stuck-test-timeout=2s")
			require.NoError(t, res.err)

			assertConsole(t, res,
				printed("$", ".test", "-test.run ^TestAlone$"),
				printed("🔄 scheduling retry:", "^TestAlone$"),
				printed("❌️", "TestAlone", "failure=timeout"),
				printed("--- TIMEOUT:", "TestAlone"),
				printed("$", ".test", "-test.run ^TestWithSub$"),
				printed("🔄 scheduling retry:", "^TestWithSub$/^Child$"),
				printed("❌️", "TestWithSub", "failure=timeout"),
				printed("--- TIMEOUT:", "TestWithSub/Child"),
				notPrinted("— in TestWithSub\n"),
				// Stuck retry targets the specific stuck test
				printed("$", ".test", "-test.run ^TestAlone$"),
				printed("✅", "TestAlone", "attempt=2", "passed=1/1"),
				printed("$", ".test", "-test.run ^TestWithSub$/^Child$"),
				printed("✅", "TestWithSub", "attempt=2"),
				printed("test run completed"),
			)
			assertLogFiles(t, res, // TestWithSub + TestAlone both get stuck on attempt 1
				file("TestWithSub",
					"TESTRUNNER LOG",
					"Attempt:     1",
					"=== RUN   TestWithSub",
				),
				file("TestAlone",
					"TESTRUNNER LOG",
					"Attempt:     1",
					"=== RUN   TestAlone",
				),
			)
		})

		t.Run("stuck detection", func(t *testing.T) {
			t.Parallel()

			// TestSlowOnce sleeps for 1min on attempt 1. With --stuck-test-timeout=2s,
			// the monitor detects no progress after ~2s and aborts the test process.
			// The test passes on retry.
			res := runIntegTest(t, []string{"./testpkg/timeout"}, "--group-by=test", "--max-attempts=2", "--stuck-test-timeout=2s")
			require.NoError(t, res.err, "should pass on retry")

			assertJUnit(t, res,
				passed("TestQuick"),
				failed("TestSlowOnce", "stuck"),
				passed("TestSlowOnce (retry 1)"),
			)
			assertConsole(t, res,
				printed("✅", "TestQuick", "attempt=1", "passed=1/1"),
				printed("🔄 scheduling retry:", "^TestSlowOnce$"),
				printed("❌️", "TestSlowOnce", "failure=timeout"),
				printed("--- TIMEOUT:", "test stuck", "no progress for"),
				printed("✅", "TestSlowOnce", "attempt=2", "passed=1/1"),
				printed("test run completed"),
			)
			assertLogFiles(t, res,
				file("TestSlowOnce",
					"TESTRUNNER LOG",
					"Attempt:     1",
					"=== RUN   TestSlowOnce",
				),
			)
		})
	})

	t.Run("failure: data race", func(t *testing.T) {
		t.Parallel()

		// Race detector is probabilistic; retry until it catches the race
		// and produces a complete result (passed + failed entries in junit).
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			res := runIntegTest(t, []string{"./testpkg/racing"}, "--group-by=test", "-race")
			if !assert.Error(ct, res.err) {
				return
			}
			assertJUnit(ct, res,
				passed("TestRace"),
				failed("TestRace", "race"),
			)
			assertConsole(ct, res,
				printed("$", ".test", "-test.run ^TestRace$"),
				printed("❌️", "failure=crash"),
				printed("RACE:", "Data race detected", "TestRace"),
				notPrinted("test run completed"),
			)
			assertLogFiles(ct, res, // TestRace crashes, no retry (max-attempts=1)
				file("TestRace",
					"TESTRUNNER LOG",
					"Attempt:     1",
					"DATA RACE",
				),
			)
		}, 30*time.Second, time.Second)
	})

	t.Run("all failure modes together", func(t *testing.T) {
		t.Parallel()

		// Combines flaky (test failure), timeout, and crashonce packages.
		// Each test runs independently in test mode; all failures retry
		// and pass on attempt 2.
		res := runIntegTest(t, []string{
			"./testpkg/flaky",
			"./testpkg/timeout",
			"./testpkg/crashonce",
		}, "--group-by=test", "--max-attempts=2", "--stuck-test-timeout=2s")
		require.NoError(t, res.err)

		assertConsole(t, res,
			// All three failure modes on attempt 1
			printed("❌️", "TestCrashOnce", "failure=crash"),
			printed("❌️", "TestFlaky", "failure=failed"),
			printed("❌️", "TestSlowOnce", "failure=timeout"),
			// All pass on attempt 2
			printed("✅", "TestCrashOnce", "attempt=2"),
			printed("✅", "TestFlaky", "attempt=2"),
			printed("✅", "TestSlowOnce", "attempt=2"),
			printed("test run completed"),
		)
		assertLogFiles(t, res,
			file("TestFlaky",
				"intentional first-attempt failure",
			),
			file("TestSlowOnce",
				"=== RUN   TestSlowOnce",
			),
			file("TestCrashOnce",
				"nil pointer dereference",
			),
		)
	})

	t.Run("multiple packages", func(t *testing.T) {
		t.Parallel()

		res := runIntegTest(t, []string{
			"./testpkg/passing",
			"./testpkg/multifile",
		}, "--group-by=test")
		require.NoError(t, res.err)

		assertJUnit(t, res,
			passed("TestA1"),
			passed("TestA2"),
			passed("TestB1"),
			passed("TestPass1"),
			passed("TestPass2"),
			passed("TestPass3"),
		)
		assertConsole(t, res,
			printed("🔨 compiled ./testpkg/multifile"),
			printed("🔨 compiled ./testpkg/passing"), // compile all first
			printed("🚀", "TestA1", "attempt 1"),
			printed("🚀", "TestA2", "attempt 1"),
			printed("🚀", "TestB1", "attempt 1"),
			printed("🚀", "TestPass1", "attempt 1"),
			printed("🚀", "TestPass2", "attempt 1"),
			printed("🚀", "TestPass3", "attempt 1"),
		)
		assertNoLogFiles(t, res)
	})

	t.Run("sharding", func(t *testing.T) {
		t.Parallel()

		t.Run("shard 0", func(t *testing.T) {
			t.Parallel()

			res := runInteg(t, []string{"./testpkg/passing", "./testpkg/multifile"}, func(cfg *config) {
				cfg.totalShards = 2
				cfg.shardIndex = 0
			}, nil, "--group-by=test")
			require.NoError(t, res.err)

			assertJUnit(t, res,
				passed("TestPass2"),
				passed("TestA2"),
				passed("TestB1"),
			)
		})

		t.Run("shard 1", func(t *testing.T) {
			t.Parallel()

			res := runInteg(t, []string{"./testpkg/passing", "./testpkg/multifile"}, func(cfg *config) {
				cfg.totalShards = 2
				cfg.shardIndex = 1
			}, nil, "--group-by=test")
			require.NoError(t, res.err)

			assertJUnit(t, res,
				passed("TestPass1"),
				passed("TestPass3"),
				passed("TestA1"),
			)
		})
	})

	t.Run("direct mode: multiple failures retried without file collision", func(t *testing.T) {
		t.Parallel()

		// In direct mode (group-by=none), multiple tests failing on the same
		// attempt each produce a separate stream retry. Before the fix, all
		// retries shared the same log/JUnit file name (all_mode_attempt_N),
		// causing concurrent retries to corrupt each other's output. Use
		// parallelism=2 so retries can overlap (the default test parallelism
		// of 1 serializes retries, masking the collision).
		res := runIntegTest(t, []string{
			"./testpkg/flaky",   // TestFlaky fails on attempt 1
			"./testpkg/subfail", // TestSuite/FailChild fails on attempt 1
		}, "--group-by=none", "--max-attempts=2", "--parallelism=2")
		require.NoError(t, res.err)

		assertJUnit(t, res,
			passed("TestStable"),
			passed("TestFlaky (retry 1)"),
			passed("TestOK"),
			passed("TestSuite/FailChild (retry 1)"),
			passed("TestSuite/PassChild"),
			failed("TestFlaky"),
			failed("TestSuite/FailChild"),
		)
	})
}

// ---- test helpers ----

// integResult holds the output from a real integration test run.
type integResult struct {
	err       error
	junitPath string
	logDir    string
	console   string // captured console and log output
}

// runIntegTest runs the testrunner with real execution for the given dirs and optional flags.
func runIntegTest(t *testing.T, dirs []string, flags ...string) integResult {
	return runInteg(t, dirs, nil, nil, flags...)
}

// runInteg runs the testrunner with real execution, optional config modifier, env, and flags.
func runInteg(t *testing.T, dirs []string, modifyConfig func(*config), env []string, flags ...string) integResult {
	t.Helper()

	outDir := t.TempDir()
	args := []string{
		"--junitfile=" + filepath.Join(outDir, "report.xml"),
		"-coverprofile=" + filepath.Join(outDir, "cover.out"),
		"--log-dir=" + filepath.Join(outDir, "logs"),
		"--parallelism=1",
	}
	args = append(args, flags...)
	args = append(args, dirs...)

	cfg := defaultConfig()
	cfg.exec = defaultExec
	cfg.env = env

	var consoleBuf bytes.Buffer
	mu := &sync.Mutex{}
	cfg.log = func(format string, v ...any) {
		mu.Lock()
		fmt.Fprintf(&consoleBuf, format+"\n", v...)
		mu.Unlock()
	}

	testArgs, parseErr := parseArgs(args, &cfg)
	require.NoError(t, parseErr, "failed to parse args: %v", args)

	if modifyConfig != nil {
		modifyConfig(&cfg)
	}

	r := &runner{
		config:  cfg,
		console: &consoleWriter{mu: mu, w: &consoleBuf},
	}

	err := r.runTests(context.Background(), testArgs)

	return integResult{
		err:       err,
		junitPath: cfg.junitReportPath,
		logDir:    cfg.logDir,
		console:   consoleBuf.String(),
	}
}

// ---- log file assertions ----

type logFileOpt func(*logFileExpect)

type logFileSpec struct {
	name    string   // substring that must appear in the filename
	content []string // substrings that must all appear in the file content
}

type logFileExpect struct {
	fileSpecs []logFileSpec
}

// file creates a per-file matcher. Each file() must match a distinct log file.
// The strings are substrings that must all appear in the file's content.
// The first argument is the test name that must appear in the filename;
// remaining arguments are substrings that must all appear in the file content.
func file(name string, contentStrs ...string) logFileOpt {
	return func(e *logFileExpect) {
		e.fileSpecs = append(e.fileSpecs, logFileSpec{name: name, content: contentStrs})
	}
}

// assertNoLogFiles asserts that no test log files exist (compile logs excluded).
func assertNoLogFiles(t *testing.T, res integResult) {
	t.Helper()
	files := readTestLogFiles(res.logDir)
	require.Empty(t, files, "expected no test log files, found %d", len(files))
}

// assertLogFiles checks test log files on disk. The number of file() specs must
// equal the number of log files, and each must match a distinct file.
// Each file() spec checks that the test name appears in the filename and
// that all content substrings appear in the file content.
func assertLogFiles(t require.TestingT, res integResult, opts ...logFileOpt) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	files := readTestLogFiles(res.logDir)

	var expect logFileExpect
	for _, o := range opts {
		o(&expect)
	}

	require.Len(t, files, len(expect.fileSpecs),
		"expected %d test log files, found %d", len(expect.fileSpecs), len(files))
	require.True(t, matchLogFiles(expect.fileSpecs, files),
		"could not match all file() specs to distinct log files\nspecs: %v\nfiles:\n%s",
		formatSpecs(expect.fileSpecs), formatFiles(files))
}

// matchLogFiles returns true if each spec can be assigned to a distinct file
// where the filename contains the spec's name and the content contains all
// required substrings.
func matchLogFiles(specs []logFileSpec, files []testLogFile) bool {
	used := make([]bool, len(files))
	return matchBacktrack(specs, files, used, 0)
}

func matchBacktrack(specs []logFileSpec, files []testLogFile, used []bool, idx int) bool {
	if idx == len(specs) {
		return true
	}
	for i, f := range files {
		if used[i] {
			continue
		}
		if fileMatchesSpec(f, specs[idx]) {
			used[i] = true
			if matchBacktrack(specs, files, used, idx+1) {
				return true
			}
			used[i] = false
		}
	}
	return false
}

func fileMatchesSpec(f testLogFile, spec logFileSpec) bool {
	if spec.name != "" && !strings.Contains(f.name, spec.name) {
		return false
	}
	for _, s := range spec.content {
		if !strings.Contains(f.content, s) {
			return false
		}
	}
	return true
}

func formatSpecs(specs []logFileSpec) string {
	var b strings.Builder
	for i, s := range specs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "file(%q, %v)", s.name, s.content)
	}
	return b.String()
}

func formatFiles(files []testLogFile) string {
	var b strings.Builder
	for i, f := range files {
		fmt.Fprintf(&b, "--- file %d: %s (len=%d) ---\n", i, f.name, len(f.content))
		if len(f.content) > 200 {
			b.WriteString(f.content[:200])
			b.WriteString("...\n")
		} else {
			b.WriteString(f.content)
			if len(f.content) > 0 && f.content[len(f.content)-1] != '\n' {
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}

// testLogFile holds the name and contents of a log file.
type testLogFile struct {
	name    string
	content string
}

// readTestLogFiles returns the name and contents of each .log file in dir,
// excluding compile log files.
func readTestLogFiles(dir string) []testLogFile {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var files []testLogFile
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".log") || strings.HasPrefix(name, "compile_") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err == nil {
			files = append(files, testLogFile{name: name, content: string(data)})
		}
	}
	return files
}

// ---- junit assertions ----

type junitOpt func(*junitExpect)

type failedTest struct {
	name string
	msgs []string // failure message must contain one of these (empty = don't check)
}

type junitExpect struct {
	passedNames []string
	failedTests []failedTest
}

func passed(name string) junitOpt {
	return func(e *junitExpect) {
		e.passedNames = append(e.passedNames, name)
	}
}

// failed asserts a test failed. Optional msgs check that the failure message contains
// at least one of the given substrings.
func failed(name string, msgs ...string) junitOpt {
	return func(e *junitExpect) {
		e.failedTests = append(e.failedTests, failedTest{name: name, msgs: msgs})
	}
}

// assertJUnit reads a JUnit report and checks it exhaustively against the given options.
// All passed/failed tests must be accounted for; unexpected tests cause failure.
func assertJUnit(t require.TestingT, res integResult, opts ...junitOpt) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	var expect junitExpect
	for _, o := range opts {
		o(&expect)
	}

	jr := &junitReport{path: res.junitPath}
	require.NoError(t, jr.read(), "failed to read junit report at %s", res.junitPath)

	var gotPassed, gotFailed []string
	for tc := range jr.testcases() {
		switch {
		case tc.Failure != nil:
			gotFailed = append(gotFailed, tc.Name)
		case tc.Error == nil:
			gotPassed = append(gotPassed, tc.Name)
		default:
			// skip error cases without failure
		}
	}

	wantFailed := make([]string, len(expect.failedTests))
	for i, ft := range expect.failedTests {
		wantFailed[i] = ft.name
	}

	slices.Sort(gotPassed)
	slices.Sort(gotFailed)
	slices.Sort(expect.passedNames)
	slices.Sort(wantFailed)
	if expect.passedNames == nil {
		expect.passedNames = []string{}
	}
	if gotPassed == nil {
		gotPassed = []string{}
	}
	if gotFailed == nil {
		gotFailed = []string{}
	}
	require.Equal(t, expect.passedNames, gotPassed)
	require.Equal(t, wantFailed, gotFailed)
	require.Equal(t, len(expect.passedNames)+len(wantFailed), jr.Tests)
	require.Equal(t, len(wantFailed), jr.Failures)

	// Check failure messages
	for _, ft := range expect.failedTests {
		if len(ft.msgs) == 0 {
			continue
		}
		for tc := range jr.testcases() {
			if tc.Name != ft.name || tc.Failure == nil {
				continue
			}
			matched := false
			for _, msg := range ft.msgs {
				if strings.Contains(tc.Failure.Message, msg) {
					matched = true
					break
				}
			}
			if !matched {
				require.Fail(t, fmt.Sprintf(
					"test %s: expected failure message containing one of %v, got: %q",
					ft.name, ft.msgs, tc.Failure.Message))
			}
		}
	}
}

// ---- console assertions ----

type consoleOpt func(*consoleExpect)

type consoleExpect struct {
	containGroups  [][]string
	notContainStrs []string
}

// printed asserts that a line exists containing all given substrings.
// Groups are matched in order: each finds the next line after the previous match.
func printed(strs ...string) consoleOpt {
	return func(e *consoleExpect) {
		e.containGroups = append(e.containGroups, strs)
	}
}

func notPrinted(str string) consoleOpt {
	return func(e *consoleExpect) {
		e.notContainStrs = append(e.notContainStrs, str)
	}
}

// assertConsole checks console output line-by-line against the given options.
func assertConsole(t require.TestingT, res integResult, opts ...consoleOpt) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	var expect consoleExpect
	for _, o := range opts {
		o(&expect)
	}
	lines := strings.Split(res.console, "\n")
	pos := 0
	for _, group := range expect.containGroups {
		found := false
		for i := pos; i < len(lines); i++ {
			if lineContainsAll(lines[i], group) {
				pos = i + 1
				found = true
				break
			}
		}
		require.True(t, found,
			"console should contain line with all of %v (searching from line %d)\nconsole output:\n%s",
			group, pos, res.console)
	}
	for _, notWant := range expect.notContainStrs {
		require.NotContains(t, res.console, notWant, "console should not contain %q", notWant)
	}
}

func lineContainsAll(line string, strs []string) bool {
	for _, s := range strs {
		if !strings.Contains(line, s) {
			return false
		}
	}
	return true
}
