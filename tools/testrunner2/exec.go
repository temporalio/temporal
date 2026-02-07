package testrunner2

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// execFunc executes a command and returns the exit code.
type execFunc func(ctx context.Context, dir, name string, args, env []string, output io.Writer) int

// defaultExec runs a command and returns its exit code.
func defaultExec(ctx context.Context, dir, name string, args, env []string, output io.Writer) int {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = output
	cmd.Stderr = output
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		return nil
	}
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		return 1
	}
	return 0
}

// compileTestInput holds the input for compileTest.
type compileTestInput struct {
	pkg        string
	binaryPath string
	buildTags  string
	baseArgs   []string
	env        []string
	output     io.Writer
}

// commandResult holds the result of running a command.
type commandResult struct {
	exitCode int
}

// compileTest compiles a test package into a binary.
// If onCommand is non-nil, it is called with the command string before execution.
func compileTest(ctx context.Context, execFn execFunc, req compileTestInput, onCommand func(string)) commandResult {
	// Build compile args for `go test -c`
	var compileFlags []string
	hasCover := false
	for _, arg := range req.baseArgs {
		if arg == "-race" || strings.HasPrefix(arg, "-tags=") || strings.HasPrefix(arg, "-coverpkg=") {
			compileFlags = append(compileFlags, arg)
		}
		if arg == "-cover" || strings.HasPrefix(arg, "-coverprofile=") || strings.HasPrefix(arg, "-coverpkg=") {
			hasCover = true
		}
	}
	if hasCover {
		compileFlags = append(compileFlags, "-cover")
	}

	args := []string{"test", "-c"}
	args = append(args, compileFlags...)
	if req.buildTags != "" {
		args = append(args, "-tags="+req.buildTags)
	}
	args = append(args, "-o", req.binaryPath, req.pkg)

	command := fmt.Sprintf("go %s", strings.Join(args, " "))
	if onCommand != nil {
		onCommand(command)
	}

	exitCode := execFn(ctx, "", "go", args, req.env, req.output)

	return commandResult{exitCode: exitCode}
}

// executeTestInput holds the input for executeTest.
type executeTestInput struct {
	binary       string
	pkgDir       string
	tests        []testCase
	skipPattern  string // regex pattern for -test.skip (to skip passed tests on retry)
	timeout      time.Duration
	coverProfile string
	extraArgs    []string // args to pass after -args (e.g., -persistenceType=xxx)
	env          []string
	output       io.Writer
}

// runDirectGoTestInput holds the input for runDirectGoTest.
type runDirectGoTestInput struct {
	pkgs         []string      // package paths to test
	buildTags    string        // build tags
	race         bool          // enable race detector
	coverProfile string        // coverage profile path
	timeout      time.Duration // test timeout
	env          []string      // extra environment variables
	output       io.Writer     // where to write output
	runFilter    string        // -run pattern (to target specific tests on retry)
	skipFilter   string        // -skip pattern (to exclude passed tests on retry)
	extraArgs    []string      // extra args to pass through to go test (e.g., -shuffle)
}

// runDirectGoTest runs `go test` directly on packages without precompilation.
// If onCommand is non-nil, it is called with the command string before execution.
func runDirectGoTest(ctx context.Context, execFn execFunc, req runDirectGoTestInput, onCommand func(string)) commandResult {
	// Apply timeout buffer so Go's -timeout fires first with stacktrace
	if req.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.timeout+30*time.Second)
		defer cancel()
	}

	args := []string{"test", "-v"}
	if req.race {
		args = append(args, "-race")
	}
	if req.buildTags != "" {
		args = append(args, "-tags="+req.buildTags)
	}
	if req.timeout > 0 {
		args = append(args, fmt.Sprintf("-timeout=%s", req.timeout))
	}
	if req.coverProfile != "" {
		args = append(args, fmt.Sprintf("-coverprofile=%s", req.coverProfile))
	}
	if req.runFilter != "" {
		args = append(args, "-run", req.runFilter)
	}
	if req.skipFilter != "" {
		args = append(args, "-skip", req.skipFilter)
	}
	args = append(args, req.extraArgs...)
	args = append(args, req.pkgs...)

	command := fmt.Sprintf("go %s", strings.Join(args, " "))
	if onCommand != nil {
		onCommand(command)
	}

	exitCode := execFn(ctx, "", "go", args, req.env, req.output)

	return commandResult{exitCode: exitCode}
}

// executeTest runs a compiled test binary.
// If onCommand is non-nil, it is called with the command string before execution.
func executeTest(ctx context.Context, execFn execFunc, req executeTestInput, onCommand func(string)) commandResult {
	// Apply timeout buffer so Go's -timeout fires first with stacktrace
	if req.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.timeout+30*time.Second)
		defer cancel()
	}

	args := []string{
		"-test.v=test2json", // Use test2json mode to get streaming subtest pass/fail output
		"-test.run", testCasesToRunPattern(req.tests),
	}
	if req.skipPattern != "" {
		args = append(args, "-test.skip", req.skipPattern)
	}
	if req.timeout > 0 {
		args = append(args, fmt.Sprintf("-test.timeout=%s", req.timeout))
	}
	if req.coverProfile != "" {
		args = append(args, fmt.Sprintf("-test.coverprofile=%s", req.coverProfile))
	}
	// Append extra args (e.g., -persistenceType=xxx -persistenceDriver=xxx)
	args = append(args, req.extraArgs...)

	// Build command string for logging
	command := fmt.Sprintf("%s %s", req.binary, strings.Join(args, " "))

	if onCommand != nil {
		onCommand(command)
	}

	exitCode := execFn(ctx, req.pkgDir, req.binary, args, req.env, req.output)

	return commandResult{exitCode: exitCode}
}
