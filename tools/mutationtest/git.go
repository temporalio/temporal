package mutationtest

import (
	"context"
	"fmt"
	"os/exec"
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
