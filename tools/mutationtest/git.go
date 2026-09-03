package mutationtest

import (
	"context"
	"fmt"
	"strings"
)

func gitRepoRoot(ctx context.Context) (string, error) {
	cmd := newCommand(ctx, "git", "rev-parse", "--show-toplevel")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to determine git repo root: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func gitResolveRef(ctx context.Context, repoRoot string, ref string) (string, error) {
	cmd := newCommand(ctx, "git", "-C", repoRoot, "rev-parse", "--verify", ref+"^{commit}")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to resolve git ref %q: %w: %s", ref, err, strings.TrimSpace(string(output)))
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
