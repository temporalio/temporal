package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type mutationStatus string

const (
	mutationKilled   mutationStatus = "killed"
	mutationSurvived mutationStatus = "survived"
	mutationSkipped  mutationStatus = "skipped"
)

type mutationOutcome struct {
	record mutantRecord
	status mutationStatus
	reason string
	diff   string
	err    error
}

func executeMutant(ctx context.Context, worktreeDir string, cfg config, record mutantRecord) (outcome mutationOutcome) {
	outcome.record = record
	targetPath := filepath.Join(worktreeDir, filepath.FromSlash(record.file))
	original, err := os.ReadFile(targetPath)
	if err != nil {
		outcome.err = fmt.Errorf("read original source for mutant %d: %w", record.id, err)
		return outcome
	}
	info, err := os.Stat(targetPath)
	if err != nil {
		outcome.err = fmt.Errorf("stat original source for mutant %d: %w", record.id, err)
		return outcome
	}
	if err := replaceFile(targetPath, record.source, info.Mode()); err != nil {
		outcome.err = fmt.Errorf("write mutant %d: %w", record.id, err)
		return outcome
	}
	defer func() {
		if err := replaceFile(targetPath, original, info.Mode()); err != nil {
			outcome.err = errors.Join(outcome.err, fmt.Errorf("restore source after mutant %d: %w", record.id, err))
		}
	}()

	outcome.diff, err = mutationDiff(ctx, worktreeDir, record.file)
	if err != nil {
		outcome.err = fmt.Errorf("create diff for mutant %d: %w", record.id, err)
		return outcome
	}
	mutationCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	cmd := newCommand(mutationCtx, "go", testCommandArgs(cfg)...)
	cmd.Dir = worktreeDir
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	output, err := cmd.CombinedOutput()
	if mutationCtx.Err() != nil {
		outcome.status = mutationKilled
		outcome.reason = "test timeout"
		return outcome
	}
	if err == nil {
		outcome.status = mutationSurvived
		outcome.reason = "tests passed"
		return outcome
	}
	exitError, ok := errors.AsType[*exec.ExitError](err)
	if !ok {
		outcome.err = fmt.Errorf("run tests for mutant %d: %w", record.id, err)
		return outcome
	}
	if exitError.ExitCode() == 2 || isBuildFailureOutput(output) {
		outcome.status = mutationSkipped
		outcome.reason = "mutant did not compile"
		return outcome
	}
	outcome.status = mutationKilled
	outcome.reason = "tests failed"
	return outcome
}

func mutationDiff(ctx context.Context, worktreeDir string, relativePath string) (string, error) {
	cmd := newCommand(ctx, "git", "-C", worktreeDir, "diff", "--no-ext-diff", "--", filepath.FromSlash(relativePath))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git diff: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if len(output) == 0 {
		return "", errors.New("git diff produced no output")
	}
	return string(output), nil
}

func replaceFile(path string, contents []byte, mode os.FileMode) (retErr error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".mutationtest-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		removeErr := os.Remove(temporaryPath)
		if !errors.Is(removeErr, os.ErrNotExist) {
			retErr = errors.Join(retErr, removeErr)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func isBuildFailureOutput(output []byte) bool {
	text := string(output)
	return strings.Contains(text, "[build failed]") ||
		strings.Contains(text, "undefined:") ||
		strings.Contains(text, "syntax error:") ||
		strings.Contains(text, "could not import")
}
