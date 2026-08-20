package testrunner

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"time"
)

type attemptSpec struct {
	number           int
	args             []string
	coverProfilePath string
}

func runAttempt(ctx context.Context, spec attemptSpec) attemptResult {
	args := spec.goTestArgs()
	log.Printf("starting test attempt #%d: go test %v", spec.number, strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "go", append([]string{"test"}, args...)...)
	// Bound the post-kill wait on output pipes so a leaked descendant that
	// inherited them cannot outlive the deadline meant to end this attempt.
	cmd.WaitDelay = 10 * time.Second
	recorder := newGoTestRecorder(os.Stdout)
	var stderr strings.Builder
	cmd.Stdout = recorder
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	cmd.Stdin = os.Stdin

	startedAt := time.Now()
	err := cmd.Run()
	process := classifyProcessResult(ctx, cmd, err, startedAt, time.Since(startedAt), stderr.String())
	return recorder.finish(process)
}

func (s attemptSpec) goTestArgs() []string {
	args := slices.Clone(s.args)
	hasJSON := false
	for i, arg := range args {
		if arg == "-args" {
			break
		}
		switch {
		case arg == "-json":
			hasJSON = true
		case strings.HasPrefix(arg, coverProfileFlag):
			// Each attempt writes a separate coverage profile for later merging.
			args[i] = coverProfileFlag + s.coverProfilePath
		default:
		}
	}
	if !hasJSON {
		args = append([]string{"-json"}, args...)
	}
	return args
}

func classifyProcessResult(
	ctx context.Context,
	cmd *exec.Cmd,
	err error,
	startedAt time.Time,
	duration time.Duration,
	stderr string,
) processResult {
	result := processResult{
		state:     processExited,
		startedAt: startedAt,
		duration:  duration,
		stderr:    stderr,
	}
	if err == nil {
		return result
	}
	result.exitCode = 1
	result.details = err.Error()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		result.state = processDeadlineExceeded
		result.details = ctx.Err().Error()
		return result
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		result.exitCode = exitError.ExitCode()
		if waitStatus, ok := exitError.Sys().(syscall.WaitStatus); ok && waitStatus.Signaled() {
			result.state = processSignaled
			result.details = waitStatus.Signal().String()
		}
		return result
	}
	if cmd.Process == nil {
		result.state = processStartFailed
		return result
	}
	if ctx.Err() != nil {
		result.state = processSignaled
		result.details = ctx.Err().Error()
		return result
	}
	result.state = processWaitFailed
	return result
}
