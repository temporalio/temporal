package mutationtest

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func gitRepoRoot(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to determine git repo root: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func gitWorktreeAdd(ctx context.Context, repoRoot string, worktreeDir string, ref string) error {
	cmd := labeledCommand(ctx, "", "git", "-C", repoRoot, "worktree", "add", "--detach", worktreeDir, ref)
	return cmd.Run()
}

func gitWorktreePrune(ctx context.Context, repoRoot string) error {
	cmd := labeledCommand(ctx, "", "git", "-C", repoRoot, "worktree", "prune")
	return cmd.Run()
}

func gitWorktreeRemove(ctx context.Context, worktreeDir string) error {
	cmd := labeledCommand(ctx, "", "git", "worktree", "remove", "--force", worktreeDir)
	return cmd.Run()
}

func gitRestoreFile(ctx context.Context, repoRoot string, originalFile string) {
	relativePath, err := filepath.Rel(repoRoot, originalFile)
	if err != nil {
		return
	}
	cmd := labeledCommand(ctx, "", "git", "-C", repoRoot, "checkout", "--", filepath.ToSlash(relativePath))
	_ = cmd.Run()
}

func gitDiffNoIndex(ctx context.Context, repoRoot string, originalFile string, mutatedFile string) string {
	cmd := exec.CommandContext(ctx, "git", "-C", repoRoot, "diff", "--no-index", "--", originalFile, mutatedFile)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return string(output)
	}

	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok && exitErr.ExitCode() == 1 {
		return string(output)
	}
	return fmt.Sprintf("--- %s\n+++ %s\n", originalFile, mutatedFile)
}
