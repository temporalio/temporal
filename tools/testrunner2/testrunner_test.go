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
				passed("TestA1"),
				passed("TestA2"),
				passed("TestB1"),
			)
			assertConsole(t, res,
				printed("starting scheduler with parallelism=1"),
				printed("🚀 compiling", "./testpkg/passing"),
				printed("$", "go test -c", "./testpkg/passing"),
				printed("🔨 compiled", "./testpkg/passing"),
				printed("discovered 3 tests"),
				printed("$", ".test", "-test.run ^TestA1$"),
				printed("✅ [1/3]", "TestA1", "attempt=1", "passed=1/1"),
				printed("$", ".test", "-test.run ^TestA2$"),
				printed("✅ [2/3]", "TestA2", "attempt=1", "passed=1/1"),
				printed("$", ".test", "-test.run ^TestB1$"),
				printed("✅ [3/3]", "TestB1", "attempt=1", "passed=1/1"),
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
				passed("TestA1"),
				passed("TestA2"),
				passed("TestB1"),
			)
			assertConsole(t, res,
				printed("running in 'none' mode"),
				printed("🚀", "all", "attempt 1"),
				printed("$", "go test", "./testpkg/passing"),
				printed("✅ [1/1]", "all", "attempt=1", "passed=3/3"),
				printed("test run completed"),
				notPrinted("go test -c"), // no compile step
			)
			assertNoLogFiles(t, res)
		})

		t.Run("group mode: none with -run filter", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/passing"}, "--group-by=none", "-run=TestA1")
			require.NoError(t, res.err)

			assertJUnit(t, res,
				passed("TestA1"),
			)
			assertConsole(t, res,
				printed("running in 'none' mode"),
				printed("🚀", "all", "attempt 1"),
				printed("$", "go test", "-run", "TestA1", "./testpkg/passing"),
				printed("✅ [1/1]", "all", "attempt=1", "passed=1/1"),
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
				printed("❌️", "TestAlwaysFails", "attempt=1", "passed=0/1", "failure=failed"),
				printed("--- TestAlwaysFails"), // failed test name shown in body
				printed("always fails"),        // failure details shown in body
				printed("$", ".test", "-test.run ^TestOK$"),
				printed("✅ [1/2]", "TestOK", "attempt=1", "passed=1/1"),
				printed("🚀", "TestAlwaysFails", "attempt 2"),
				printed("$", ".test", "-test.run ^TestAlwaysFails$"),
				printed("❌️", "TestAlwaysFails", "attempt=2", "passed=0/1", "failure=failed"),
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
				printed("❌️", "all", "attempt=1", "passed=1/2", "failure=failed"),
				printed("--- TestAlwaysFails"),
				printed("always fails"),
				printed("❌️", "all", "attempt=2", "passed=0/1", "failure=failed"),
				notPrinted("go test -c"), // no compile step
			)
		})
	})

	t.Run("failure: flaky test", func(t *testing.T) {
		t.Parallel()

		t.Run("group mode: test", func(t *testing.T) {
			t.Parallel()

			res := runIntegTest(t, []string{"./testpkg/flaky"}, "--group-by=test", "--max-attempts=2", "-run=TestFlaky|TestStable")
			require.NoError(t, res.err, "flaky test should pass on retry")

			assertConsole(t, res,
				printed("$", ".test", "-test.run ^TestStable$"),
				printed("✅ [1/2]", "TestStable", "attempt=1", "passed=1/1"),
				printed("$", ".test", "-test.run ^TestFlaky$"),
				printed("🔄 scheduling retry:", "^TestFlaky$"), // mid-stream retry
				printed("❌️", "TestFlaky", "attempt=1", "passed=0/1", "failure=failed"),
				printed("--- TestFlaky"),                     // failed test name shown in body
				printed("intentional first-attempt failure"), // failure details shown in body
				printed("$", ".test", "-test.run ^TestFlaky$"),
				printed("✅ [2/2]", "TestFlaky", "attempt=2", "passed=1/1"),
				printed("test run completed"),
			)
			assertLogFiles(t, res,
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

			res := runIntegTest(t, []string{"./testpkg/flaky"}, "--group-by=none", "--max-attempts=2", "-run=TestFlaky|TestStable")
			require.NoError(t, res.err)

			assertConsole(t, res,
				printed("running in 'none' mode"),
				printed("🚀", "all", "attempt 1"),
				printed("$", "go test", "-run", "TestFlaky|TestStable", "./testpkg/flaky"),
				printed("🔄 scheduling retry:", "^TestFlaky$"),
				printed("🚀", "all", "attempt 2"),
				printed("$", "go test", "-run", "^TestFlaky$", "./testpkg/flaky"),
				printed("❌️", "all", "attempt=1", "passed=1/2", "failure=failed"),
				printed("--- TestFlaky"),
				printed("intentional first-attempt failure"),
				printed("✅ [1/1]", "all", "attempt=2", "passed=1/1"),
				printed("test run completed"),
				notPrinted("go test -c"),
			)
		})
	})

	t.Run("failure: flaky subtest", func(t *testing.T) {
		t.Parallel()

		// Only the leaf subtest TestSuite/FailChild should be retried, not the parent.
		// The retry skips the already-passed PassChild sibling via -test.skip.
		res := runIntegTest(t, []string{"./testpkg/flaky"}, "--group-by=test", "--max-attempts=2", "-run=TestSuite")
		require.NoError(t, res.err, "should pass on retry")

		assertConsole(t, res,
			printed("$", ".test", "-test.run ^TestSuite$"),
			printed("🔄 scheduling retry:", "^TestSuite$/^FailChild$"),
			printed("❌️", "TestSuite", "attempt=1", "passed=1/3", "failure=failed"),
			// Retry targets only the leaf subtest, skipping passed sibling
			printed("$", ".test",
				"-test.run ^TestSuite$/^FailChild$",
				"-test.skip ^TestSuite$/^PassChild$"),
			printed("✅ [1/1]", "TestSuite", "attempt=2", "passed=2/2"),
			printed("test run completed"),
		)
		assertLogFiles(t, res,
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

		// TestCrash panics on attempt 1, passes on retry.
		res := runIntegTest(t, []string{"./testpkg/crash"}, "--group-by=test", "--max-attempts=2")

		assertJUnit(t, res,
			failed("TestCrash", "panic"),
			passed("TestCrash (retry 1)"),
		)
		assertConsole(t, res,
			printed("$", ".test", "-test.run ^TestCrash$"),
			printed("❌️", "TestCrash", "attempt=1", "passed=0/?", "failure=crash"),
			printed("PANIC:", "intentional crash"),
			printed("🔄 scheduling retry:", "^TestCrash$"), // post-exit crash recovery
			printed("$", ".test", "-test.run ^TestCrash$"),
			printed("✅ [1/1]", "TestCrash", "attempt=2", "passed=1/1"),
			printed("test run completed"),
		)
		assertLogFiles(t, res, // attempt 1 crashes
			file("TestCrash",
				"TESTRUNNER LOG",
				"Attempt:     1",
				"intentional crash",
			),
		)
	})

	t.Run("failure: test timeout", func(t *testing.T) {
		t.Parallel()

		// TestSlowOnce gets stuck on attempt 1. The stuck detector fires and
		// the test is retried. TestQuick passes normally.
		res := runIntegTest(t, []string{"./testpkg/timeout"}, "--group-by=test", "--max-attempts=2", "--stuck-test-timeout=2s", "-run=TestSlowOnce|TestQuick")
		require.NoError(t, res.err, "should pass on retry")

		assertJUnit(t, res,
			passed("TestQuick"),
			failed("TestSlowOnce", "stuck"),
			passed("TestSlowOnce (retry 1)"),
		)
		assertConsole(t, res,
			printed("$", ".test", "-test.run ^TestQuick$"),
			printed("✅ [1/2]", "TestQuick", "attempt=1", "passed=1/1"),
			printed("$", ".test", "-test.run ^TestSlowOnce$"),
			printed("🔄 scheduling retry:", "^TestSlowOnce$"),
			printed("❌️", "TestSlowOnce", "attempt=1", "passed=0/?", "failure=timeout"),
			printed("--- TIMEOUT:", "test stuck", "TestSlowOnce", "no progress for"),
			printed("$", ".test", "-test.run ^TestSlowOnce$"),
			printed("✅ [2/2]", "TestSlowOnce", "attempt=2", "passed=1/1"),
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

	t.Run("failure: subtest timeout", func(t *testing.T) {
		t.Parallel()

		// TestWithSub/Slow gets stuck on attempt 1. Pass1 and Pass2 complete
		// before stuck detection fires. The retry targets only the stuck leaf
		// subtest and skips already-passed siblings via -test.skip.
		res := runIntegTest(t, []string{"./testpkg/timeout"}, "--group-by=test", "--max-attempts=2", "--stuck-test-timeout=2s", "-run=TestWithSub")
		require.NoError(t, res.err, "should pass on retry")

		assertConsole(t, res,
			printed("$", ".test", "-test.run ^TestWithSub$"),
			printed("🔄 scheduling retry:", "^TestWithSub$/^Slow$"),
			printed("❌️", "TestWithSub", "attempt=1", "passed=2/?", "failure=timeout"),
			printed("--- TIMEOUT:", "TestWithSub/Slow"),
			notPrinted("— in TestWithSub\n"), // leaf shown, not parent
			// Retry skips passed siblings
			printed("$", ".test", "-test.run ^TestWithSub$/^Slow$", "-test.skip ^TestWithSub$/^(Pass"),
			printed("✅ [1/1]", "TestWithSub", "attempt=2", "passed=2/2"),
			printed("test run completed"),
		)
		assertLogFiles(t, res,
			file("TestWithSub",
				"TESTRUNNER LOG",
				"Attempt:     1",
				"=== RUN   TestWithSub",
			),
		)
	})

	t.Run("failure: parent stuck after subtests pass", func(t *testing.T) {
		t.Parallel()

		// TestParentStuck's children both pass, then the parent hangs
		// (simulating a stuck teardown). The stuck detector should report
		// the parent and the retry should skip already-passed children.
		res := runIntegTest(t, []string{"./testpkg/timeout"}, "--group-by=test", "--max-attempts=2", "--stuck-test-timeout=2s", "-run=TestParentStuck")
		require.NoError(t, res.err, "should pass on retry")

		assertConsole(t, res,
			printed("$", ".test", "-test.run ^TestParentStuck$"),
			printed("🔄 scheduling retry:", "^TestParentStuck$"),
			printed("❌️", "TestParentStuck", "attempt=1", "passed=2/?", "failure=timeout"),
			printed("--- TIMEOUT:", "TestParentStuck"),
			// Retry skips passed children
			printed("$", ".test", "-test.run ^TestParentStuck$", "-test.skip ^TestParentStuck$/^(Child"),
			printed("✅ [1/1]", "TestParentStuck", "attempt=2", "passed=1/1"),
			printed("test run completed"),
		)
	})

	t.Run("failure: data race", func(t *testing.T) {
		t.Parallel()

		// Race detector is probabilistic; retry until it catches the race
		// and produces a complete result (passed + failed entries in junit).
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			res := runIntegTest(t, []string{"./testpkg/datarace"}, "--group-by=test", "-race")
			if !assert.Error(ct, res.err) {
				return
			}
			assertJUnit(ct, res,
				passed("TestRace"),
				failed("TestRace", "race"),
			)
			assertConsole(ct, res,
				printed("$", ".test", "-test.run ^TestRace$"),
				printed("❌️", "attempt=1", "failure=crash"),
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

	t.Run("sharding", func(t *testing.T) {
		t.Parallel()

		t.Run("shard 0", func(t *testing.T) {
			t.Parallel()

			res := runInteg(t, []string{"./testpkg/passing"}, func(cfg *config) {
				cfg.totalShards = 2
				cfg.shardIndex = 0
			}, nil, "--group-by=test")
			require.NoError(t, res.err)

			assertJUnit(t, res,
				passed("TestA2"),
				passed("TestB1"),
			)
		})

		t.Run("shard 1", func(t *testing.T) {
			t.Parallel()

			res := runInteg(t, []string{"./testpkg/passing"}, func(cfg *config) {
				cfg.totalShards = 2
				cfg.shardIndex = 1
			}, nil, "--group-by=test")
			require.NoError(t, res.err)

			assertJUnit(t, res,
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
		// The flaky package has both TestFlaky (top-level) and TestSuite/FailChild
		// (subtest) failing on attempt 1, producing two concurrent retries.
		res := runIntegTest(t, []string{
			"./testpkg/flaky",
		}, "--group-by=none", "--max-attempts=2", "--parallelism=2")
		require.NoError(t, res.err)

		assertJUnit(t, res,
			passed("TestStable"),
			passed("TestFlaky (retry 1)"),
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
