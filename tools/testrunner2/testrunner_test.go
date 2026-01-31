package testrunner2

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunnerReportLogs(t *testing.T) {
	// Create a temp log directory with test log files
	logDir := t.TempDir()

	// Create package directories and log files
	pkg1Dir := filepath.Join(logDir, "common_persistence")
	require.NoError(t, os.MkdirAll(pkg1Dir, 0755))

	pkg2Dir := filepath.Join(logDir, "service_history")
	require.NoError(t, os.MkdirAll(pkg2Dir, 0755))

	// Create log files
	require.NoError(t, os.WriteFile(
		filepath.Join(pkg1Dir, "compile.log"),
		[]byte("compile output for persistence\n"),
		0644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(pkg1Dir, "persistence_attempt1.log"),
		[]byte("test output for persistence\n"),
		0644,
	))
	require.NoError(t, os.WriteFile(
		filepath.Join(pkg2Dir, "history_attempt1.log"),
		[]byte("test output for history\n"),
		0644,
	))

	r := newRunner()
	r.logDir = logDir

	// Capture stdout
	oldStdout := os.Stdout
	r2, w, _ := os.Pipe()
	os.Stdout = w

	err := r.reportLogs()
	require.NoError(t, err)

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r2)
	output := buf.String()

	// Verify output contains expected content
	require.Contains(t, output, "compile output for persistence")
	require.Contains(t, output, "test output for persistence")
	require.Contains(t, output, "test output for history")
}

func TestRunnerReportLogs_EmptyDir(t *testing.T) {
	logDir := t.TempDir()

	r := newRunner()
	r.logDir = logDir

	err := r.reportLogs()
	require.NoError(t, err)
}

func TestRunnerReportLogs_MissingDir(t *testing.T) {
	r := newRunner()
	r.logDir = "/nonexistent/path"

	err := r.reportLogs()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read log directory")
}

// --- Integration tests with stubbed exec ---

// execStub returns responses based on pattern matching against the command.
type execStub struct {
	t            *testing.T
	mu           sync.Mutex
	calls        []stubCall
	compileByPkg map[string][]stubResponse // responses by package pattern for compile
	compileIdx   map[string]int            // current index per package pattern
	byPattern    map[string][]stubResponse // responses by test pattern, consumed in order
	patternIdx   map[string]int            // current index per pattern
}

type stubCall struct {
	Dir  string
	Name string
	Args []string
}

type stubResponse struct {
	Stdout   string
	ExitCode int
}

func newExecStub(t *testing.T) *execStub {
	return &execStub{
		t:            t,
		compileByPkg: make(map[string][]stubResponse),
		compileIdx:   make(map[string]int),
		byPattern:    make(map[string][]stubResponse),
		patternIdx:   make(map[string]int),
	}
}

// onCompile adds a response for compile commands matching the package pattern.
func (s *execStub) onCompile(pkgPattern string, resp stubResponse) *execStub {
	s.compileByPkg[pkgPattern] = append(s.compileByPkg[pkgPattern], resp)
	return s
}

// onTest adds a response for tests matching the pattern (e.g., "Foo" matches TestFoo1, TestFoo2).
func (s *execStub) onTest(pattern string, resp stubResponse) *execStub {
	s.byPattern[pattern] = append(s.byPattern[pattern], resp)
	return s
}

func (s *execStub) exec(ctx context.Context, dir, name string, args []string, output io.Writer) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, stubCall{Dir: dir, Name: name, Args: args})

	// Check if this is a compile command (go test -c)
	if name == "go" {
		var isCompile bool
		var pkgArg string
		for i, arg := range args {
			if arg == "-c" {
				isCompile = true
			}
			// Package is typically the last argument
			if !strings.HasPrefix(arg, "-") && i > 0 {
				pkgArg = arg
			}
		}
		if isCompile {
			for pattern, responses := range s.compileByPkg {
				if strings.Contains(pkgArg, pattern) {
					idx := s.compileIdx[pattern]
					if idx < len(responses) {
						s.compileIdx[pattern] = idx + 1
						resp := responses[idx]
						if resp.Stdout != "" {
							_, _ = io.WriteString(output, resp.Stdout)
						}
						return resp.ExitCode
					}
				}
			}
			s.t.Errorf("no compile response configured for package %q", pkgArg)
			return 1
		}
	}

	// Find the test pattern being run from -test.run arg
	var testPattern string
	for i, arg := range args {
		if arg == "-test.run" && i+1 < len(args) {
			testPattern = args[i+1]
			break
		}
	}

	// Match against registered patterns
	for pattern, responses := range s.byPattern {
		if strings.Contains(testPattern, pattern) {
			idx := s.patternIdx[pattern]
			if idx < len(responses) {
				s.patternIdx[pattern] = idx + 1
				resp := responses[idx]
				if resp.Stdout != "" {
					_, _ = io.WriteString(output, resp.Stdout)
				}
				return resp.ExitCode
			}
		}
	}

	s.t.Errorf("no response configured for pattern %q", testPattern)
	return 1
}

func (s *execStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// runTestsWithStub runs the testrunner with stubbed exec.
func runTestsWithStub(t *testing.T, args []string, stub *execStub) (err error, logs *bytes.Buffer) {
	return runTestsWithConfig(t, args, stub, nil)
}

// runTestsWithConfig runs the testrunner with stubbed exec and optional config modifier.
func runTestsWithConfig(t *testing.T, args []string, stub *execStub, modifyConfig func(*config)) (err error, logs *bytes.Buffer) {
	t.Helper()

	logs = &bytes.Buffer{}

	cfg := defaultConfig()
	cfg.exec = stub.exec
	cfg.log = func(format string, v ...any) {} // silent

	testArgs, parseErr := parseConfig("test", args, &cfg)
	require.NoError(t, parseErr, "failed to parse args")

	if modifyConfig != nil {
		modifyConfig(&cfg)
	}

	r := &runner{
		config:    cfg,
		console:   &consoleWriter{mu: &sync.Mutex{}},
		collector: &resultCollector{},
	}

	// Capture stdout
	oldStdout := os.Stdout
	rPipe, wPipe, _ := os.Pipe()
	os.Stdout = wPipe

	err = r.runTests(context.Background(), testArgs)

	_ = wPipe.Close()
	os.Stdout = oldStdout
	_, _ = io.Copy(logs, rPipe)

	return err, logs
}

// fixture loads a fixture file from testdata.
func fixture(name string) string {
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		panic(err)
	}
	return string(data)
}

func baseArgs(t *testing.T) []string {
	t.Helper()
	outDir := t.TempDir()
	return []string{
		"--junitfile=" + filepath.Join(outDir, "report.xml"),
		"-coverprofile=" + filepath.Join(outDir, "cover.out"),
		"--log-dir=" + filepath.Join(outDir, "logs"),
		"--group-by=file",
		"--max-attempts=3",
		"--parallelism=1",
		"./testdata/fakepkg1",
	}
}

func TestIntegration_PassOnFirstAttempt(t *testing.T) {
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}).
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	err, logs := runTestsWithStub(t, baseArgs(t), stub)

	require.NoError(t, err)
	assert.Equal(t, 3, stub.callCount()) // compile + foo + baz

	// Verify log output
	output := logs.String()
	assert.Contains(t, output, "🔨 compiled ./testdata/fakepkg1")
	assert.Contains(t, output, "✅ [1/2] foo_test.go (TestFoo1, TestFoo2, TestFoo3) (attempt=1, passed=2/2, runtime=")
	assert.Contains(t, output, "✅ [2/2] baz_test.go (TestBaz1) (attempt=1, passed=1/1, runtime=")
	assert.NotContains(t, output, "failure=")
}

func TestIntegration_FailThenRetryPass(t *testing.T) {
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_fail.log"), ExitCode: 1}). // attempt 1
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}). // retry
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	err, logs := runTestsWithStub(t, baseArgs(t), stub)

	require.NoError(t, err)
	assert.Equal(t, 4, stub.callCount()) // compile + foo fail + baz + foo retry

	// Verify log output
	output := logs.String()
	assert.Contains(t, output, "❌️ foo_test.go (TestFoo1, TestFoo2, TestFoo3) (attempt=1, passed=1/2, failure=failed, runtime=")
	assert.Contains(t, output, "✅ [2/2] foo_test.go (TestFoo1) (attempt=2, passed=2/2, runtime=")
}

func TestIntegration_MaxAttemptsExceeded(t *testing.T) {
	failFoo := fixture("foo_fail.log")
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: failFoo, ExitCode: 1}). // attempt 1
		onTest("Foo", stubResponse{Stdout: failFoo, ExitCode: 1}). // attempt 2
		onTest("Foo", stubResponse{Stdout: failFoo, ExitCode: 1}). // attempt 3
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	err, _ := runTestsWithStub(t, baseArgs(t), stub)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed on attempt 3")
	assert.Equal(t, 5, stub.callCount())
}

func TestIntegration_CrashTriggersRetry(t *testing.T) {
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_crash.log"), ExitCode: 2}). // crash
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}).  // retry
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	_, logs := runTestsWithStub(t, baseArgs(t), stub)

	// Verify retry happened (merge validation may error on alerts)
	assert.Equal(t, 4, stub.callCount())

	// Verify log output shows crash with unknown total and PANIC alert
	output := logs.String()
	assert.Regexp(t, `❌️ foo_test.go \(TestFoo1, TestFoo2, TestFoo3\) \(attempt=1, passed=\d+/\?, failure=crash, runtime=`, output)
	assert.Contains(t, output, "PANIC: runtime error: nil pointer dereference")
}

func TestIntegration_TimeoutTriggersRetry(t *testing.T) {
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_timeout2.log"), ExitCode: 1}). // timeout
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}).     // retry
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	_, logs := runTestsWithStub(t, baseArgs(t), stub)

	assert.Equal(t, 4, stub.callCount())

	// Verify log output shows timeout with unknown total
	output := logs.String()
	assert.Regexp(t, `❌️ foo_test.go \(TestFoo1, TestFoo2, TestFoo3\) \(attempt=1, passed=\d+/\?, failure=timeout, runtime=`, output)

	// Verify TIMEOUT alerts show full test names (subtests, not parents).
	// With the fixture having TestFoo1, TestFoo1/SubTest1, and TestFoo2 running,
	// only TestFoo1/SubTest1 (the subtest) and TestFoo2 should be reported,
	// NOT TestFoo1 (the parent that has a subtest running).
	assert.Contains(t, output, "TIMEOUT: test timed out after 1s — in TestFoo1/SubTest1")
	assert.Contains(t, output, "TIMEOUT: test timed out after 1s — in TestFoo2")
	assert.NotContains(t, output, "— in TestFoo1\n") // parent should not be reported separately

	// Verify the retry uses the correct -run pattern
	var retryCall *stubCall
	for i := range stub.calls {
		if strings.Contains(stub.calls[i].Name, ".test") {
			// The second test call (index 1 after compile) is the retry if foo_pass succeeds
			retryCall = &stub.calls[i]
		}
	}
	require.NotNil(t, retryCall, "should have a retry test call")
	retryArgs := strings.Join(retryCall.Args, " ")
	// Retry should include both top-level tests since timeout affected the whole run
	assert.Contains(t, retryArgs, "TestFoo1")
	assert.Contains(t, retryArgs, "TestFoo2")
}

func TestIntegration_TimeoutSkipsPassedTests(t *testing.T) {
	// This test verifies that when a timeout occurs after some tests have passed,
	// the retry uses -test.skip to skip the already-passed tests.
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_timeout1.log"), ExitCode: 1}). // partial timeout: TestFoo1 passed, TestFoo2 timed out
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}).     // retry with skip
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	_, logs := runTestsWithStub(t, baseArgs(t), stub)

	assert.Equal(t, 4, stub.callCount())

	output := logs.String()
	// Verify timeout was detected
	assert.Regexp(t, `❌️ foo_test.go \(TestFoo1, TestFoo2, TestFoo3\) \(attempt=1, passed=\d+/\?, failure=timeout, runtime=`, output)
	assert.Contains(t, output, "TIMEOUT: test timed out after 1s — in TestFoo2")

	// Find the retry call (the second Foo test call)
	var fooCallCount int
	var retryCall *stubCall
	for i := range stub.calls {
		if strings.Contains(stub.calls[i].Name, ".test") {
			for _, arg := range stub.calls[i].Args {
				if strings.Contains(arg, "TestFoo") {
					fooCallCount++
					if fooCallCount == 2 {
						retryCall = &stub.calls[i]
					}
				}
			}
		}
	}
	require.NotNil(t, retryCall, "should have a retry test call for Foo tests")

	// Verify the retry uses -test.skip to skip TestFoo1 (which already passed)
	retryArgs := strings.Join(retryCall.Args, " ")
	assert.Contains(t, retryArgs, "-test.skip")
	assert.Contains(t, retryArgs, "TestFoo1") // TestFoo1 should be in the skip pattern
	// The -run pattern should still include all tests
	assert.Contains(t, retryArgs, "-test.run")
	assert.Contains(t, retryArgs, "TestFoo1")
	assert.Contains(t, retryArgs, "TestFoo2")
	assert.Contains(t, retryArgs, "TestFoo3")
}

func TestIntegration_TimeoutWithFailuresStillSkipsPassedTests(t *testing.T) {
	// This test verifies that when a timeout occurs AND there are test failures,
	// the retry still uses -test.skip for passed tests (not just retrying the failures).
	// This is the key bug fix: before, if len(failures) > 0 during timeout, we would
	// only retry the failed tests. Now we check for timeout first and skip passed tests.
	//
	// Scenario: TestFoo1 passes, TestFoo2 fails, TestFoo3 times out.
	// Before fix: retry would only run TestFoo2 (the explicit failure)
	// After fix: retry runs all tests but skips TestFoo1 (the passed test)
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass_fail_timeout.log"), ExitCode: 1}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}). // retry with skip
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	_, logs := runTestsWithStub(t, baseArgs(t), stub)

	assert.Equal(t, 4, stub.callCount())

	output := logs.String()
	// Verify timeout was detected (the key is "failure=timeout", not "failure=failed")
	assert.Regexp(t, `❌️ foo_test.go \(TestFoo1, TestFoo2, TestFoo3\) \(attempt=1, passed=\d+/\?, failure=timeout, runtime=`, output)

	// Find the retry call
	var fooCallCount int
	var retryCall *stubCall
	for i := range stub.calls {
		if strings.Contains(stub.calls[i].Name, ".test") {
			for _, arg := range stub.calls[i].Args {
				if strings.Contains(arg, "TestFoo") {
					fooCallCount++
					if fooCallCount == 2 {
						retryCall = &stub.calls[i]
					}
				}
			}
		}
	}
	require.NotNil(t, retryCall, "should have a retry test call for Foo tests")

	retryArgs := strings.Join(retryCall.Args, " ")

	// KEY ASSERTION: The retry must use -test.skip for TestFoo1 (which passed).
	// Before the fix, this would NOT have -test.skip because len(failures) > 0
	// caused buildRetryUnit to be used instead of buildRetryUnitExcluding.
	assert.Contains(t, retryArgs, "-test.skip", "timeout should use -test.skip for passed tests even when there are failures")
	assert.Contains(t, retryArgs, "TestFoo1", "TestFoo1 (passed) should be in skip pattern")

	// The -run pattern should still include all tests
	assert.Contains(t, retryArgs, "-test.run")
}

func TestIntegration_DataRaceTriggersRetry(t *testing.T) {
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_race.log"), ExitCode: 1}). // race
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}). // retry
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	_, logs := runTestsWithStub(t, baseArgs(t), stub)

	assert.Equal(t, 4, stub.callCount())

	// Verify log output shows crash (race treated as crash) and RACE alert
	output := logs.String()
	assert.Regexp(t, `❌️ foo_test.go \(TestFoo1, TestFoo2, TestFoo3\) \(attempt=1, passed=\d+/\?, failure=crash, runtime=`, output)
	assert.Contains(t, output, "RACE: Data race detected")
}

func TestIntegration_CompileFailure(t *testing.T) {
	compileErr := `# go.temporal.io/server/tools/testrunner2/testdata/fakepkg1
foo_test.go:10: undefined: x
`
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{Stdout: compileErr, ExitCode: 1})

	err, _ := runTestsWithStub(t, baseArgs(t), stub)

	require.Error(t, err)
	assert.Equal(t, 1, stub.callCount()) // Only compile
}

func TestIntegration_GroupByPackage(t *testing.T) {
	outDir := t.TempDir()
	args := []string{
		"--junitfile=" + filepath.Join(outDir, "report.xml"),
		"-coverprofile=" + filepath.Join(outDir, "cover.out"),
		"--log-dir=" + filepath.Join(outDir, "logs"),
		"--group-by=package",
		"./testdata/fakepkg1",
	}

	// With group-by=package, all tests run together so pattern matches all
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("all_pass.log"), ExitCode: 0})

	err, _ := runTestsWithStub(t, args, stub)

	require.NoError(t, err)
	assert.Equal(t, 2, stub.callCount()) // compile + all tests
}

func TestIntegration_GroupBySuite(t *testing.T) {
	outDir := t.TempDir()
	args := []string{
		"--junitfile=" + filepath.Join(outDir, "report.xml"),
		"-coverprofile=" + filepath.Join(outDir, "cover.out"),
		"--log-dir=" + filepath.Join(outDir, "logs"),
		"--group-by=suite",
		"--parallelism=1",
		"./testdata/fakepkg1",
	}

	// With group-by=suite, each test runs separately
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("TestFoo1", stubResponse{Stdout: fixture("foo1_pass.log"), ExitCode: 0}).
		onTest("TestFoo2", stubResponse{Stdout: fixture("foo2_pass.log"), ExitCode: 0}).
		onTest("TestFoo3", stubResponse{Stdout: fixture("foo3_pass.log"), ExitCode: 0}).
		onTest("TestBaz1", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	err, logs := runTestsWithStub(t, args, stub)

	require.NoError(t, err)
	assert.Equal(t, 5, stub.callCount()) // compile + 4 individual tests

	// Verify log output shows suite names in launch and completion lines
	output := logs.String()
	assert.Contains(t, output, "🚀 TestFoo1 (attempt 1)")
	assert.Contains(t, output, "🚀 TestFoo2 (attempt 1)")
	assert.Contains(t, output, "🚀 TestFoo3 (attempt 1)")
	assert.Contains(t, output, "🚀 TestBaz1 (attempt 1)")
	assert.Contains(t, output, "✅") // at least one success marker
	assert.Regexp(t, `✅ \[\d+/4\] TestFoo1`, output)
	assert.Regexp(t, `✅ \[\d+/4\] TestFoo2`, output)
	assert.Regexp(t, `✅ \[\d+/4\] TestFoo3`, output)
	assert.Regexp(t, `✅ \[\d+/4\] TestBaz1`, output)
}

func TestIntegration_VerifyTestRunArgs(t *testing.T) {
	stub := newExecStub(t).
		onCompile("fakepkg1", stubResponse{ExitCode: 0}).
		onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}).
		onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0})

	err, _ := runTestsWithStub(t, baseArgs(t), stub)
	require.NoError(t, err)

	// Find a test run call (not compile)
	var testCall *stubCall
	for i := range stub.calls {
		if strings.Contains(stub.calls[i].Name, ".test") {
			testCall = &stub.calls[i]
			break
		}
	}
	require.NotNil(t, testCall, "should have a test binary call")

	// Verify test run has expected args
	args := strings.Join(testCall.Args, " ")
	assert.Contains(t, args, "-test.v")
	assert.Contains(t, args, "-test.run")
}

func TestIntegration_Sharding(t *testing.T) {
	// Test that sharding distributes work across shards.
	// We use both fakepkg1 (foo_test.go, baz_test.go) and fakepkg2 (bar_test.go).
	// With 2 shards, files should be distributed based on consistent hashing.

	outDir := t.TempDir()
	baseArgs := []string{
		"--junitfile=" + filepath.Join(outDir, "report.xml"),
		"-coverprofile=" + filepath.Join(outDir, "cover.out"),
		"--log-dir=" + filepath.Join(outDir, "logs"),
		"--group-by=file",
		"--parallelism=1",
		"./testdata/fakepkg1",
		"./testdata/fakepkg2",
	}

	// Collect which tests run in each shard
	shardTests := make([][]string, 2)

	for shardIndex := range 2 {
		stub := newExecStub(t).
			onCompile("fakepkg1", stubResponse{ExitCode: 0}).
			onCompile("fakepkg2", stubResponse{ExitCode: 0}).
			onTest("Foo", stubResponse{Stdout: fixture("foo_pass.log"), ExitCode: 0}).
			onTest("Baz", stubResponse{Stdout: fixture("baz_pass.log"), ExitCode: 0}).
			onTest("Bar", stubResponse{Stdout: fixture("bar_pass.log"), ExitCode: 0})

		idx := shardIndex // capture for closure
		err, _ := runTestsWithConfig(t, baseArgs, stub, func(cfg *config) {
			cfg.totalShards = 2
			cfg.shardIndex = idx
		})
		require.NoError(t, err)

		// Record which tests ran in this shard
		for _, call := range stub.calls {
			for i, arg := range call.Args {
				if arg == "-test.run" && i+1 < len(call.Args) {
					shardTests[idx] = append(shardTests[idx], call.Args[i+1])
				}
			}
		}
	}

	// Verify shards have non-overlapping tests and together cover all tests
	allTests := append(shardTests[0], shardTests[1]...)
	assert.Len(t, allTests, 3, "all 3 test files should be covered across shards")

	// Verify no duplicates (each test in exactly one shard)
	seen := make(map[string]bool)
	for _, test := range allTests {
		assert.False(t, seen[test], "test %s should not appear in multiple shards", test)
		seen[test] = true
	}
}
