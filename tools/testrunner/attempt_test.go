package testrunner

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAttemptSpecGoTestArgs(t *testing.T) {
	spec := attemptSpec{
		args:             []string{"./...", "-coverprofile=old.cover.out", "-args", "value"},
		coverProfilePath: "attempt.cover.out",
	}
	require.Equal(t, []string{
		"-json", "./...", "-coverprofile=attempt.cover.out", "-args", "value",
	}, spec.goTestArgs())
	require.Equal(t, []string{"./...", "-coverprofile=old.cover.out", "-args", "value"}, spec.args)
}

func TestAttemptSpecGoTestArgsIgnoresTestBinaryFlags(t *testing.T) {
	spec := attemptSpec{
		args:             []string{"./...", "-args", "-json", "-coverprofile=binary.out"},
		coverProfilePath: "attempt.cover.out",
	}

	require.Equal(t, []string{
		"-json", "./...", "-args", "-json", "-coverprofile=binary.out",
	}, spec.goTestArgs())
}

func TestAttemptSpecGoTestArgsKeepsExistingJSONFlag(t *testing.T) {
	spec := attemptSpec{args: []string{"-json", "./..."}}
	require.Equal(t, []string{"-json", "./..."}, spec.goTestArgs())
}

func TestClassifyProcessResult(t *testing.T) {
	startedAt := time.Now()
	duration := time.Second

	result := classifyProcessResult(context.Background(), &exec.Cmd{}, nil, startedAt, duration, "stderr")
	require.Equal(t, processResult{
		state:     processExited,
		startedAt: startedAt,
		duration:  duration,
		stderr:    "stderr",
	}, result)

	deadlineContext, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result = classifyProcessResult(deadlineContext, &exec.Cmd{}, context.DeadlineExceeded, startedAt, duration, "")
	require.Equal(t, processDeadlineExceeded, result.state)
	require.Equal(t, 1, result.exitCode)

	result = classifyProcessResult(context.Background(), &exec.Cmd{}, errors.New("start failed"), startedAt, duration, "")
	require.Equal(t, processStartFailed, result.state)
	require.Equal(t, "start failed", result.details)

	command := exec.Command("sh", "-c", "exit 7")
	err := command.Run()
	require.Error(t, err)
	result = classifyProcessResult(context.Background(), command, err, startedAt, duration, "")
	require.Equal(t, processExited, result.state)
	require.Equal(t, 7, result.exitCode)

	command = exec.Command("sh", "-c", "kill -TERM $$")
	err = command.Run()
	require.Error(t, err)
	result = classifyProcessResult(context.Background(), command, err, startedAt, duration, "")
	require.Equal(t, processSignaled, result.state)
	require.Equal(t, "terminated", result.details)
}
