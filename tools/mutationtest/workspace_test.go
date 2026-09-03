package mutationtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecutionWorkspacesPinRefBeforeCreatingWorktrees(t *testing.T) {
	t.Parallel()

	repoDir := copySmokeFixture(t)
	runGitCommand(t, repoDir, "init", "--quiet")
	runGitCommand(t, repoDir, "add", ".")
	runGitCommand(t, repoDir, "-c", "user.name=Mutation Test", "-c", "user.email=mutation@example.com", "commit", "--quiet", "-m", "first")
	firstCommit := runGitCommand(t, repoDir, "rev-parse", "HEAD")

	workspaces := &executionWorkspaces{
		repoRoot: repoDir,
		runDir:   filepath.Join(repoDir, "output"),
		ref:      "HEAD",
	}
	require.NoError(t, os.MkdirAll(workspaces.runDir, 0o755))
	require.NoError(t, workspaces.add(t.Context()))
	defer func() {
		require.NoError(t, workspaces.close(context.WithoutCancel(t.Context())))
	}()

	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "value.go"), []byte("package smoke\n"), 0o644))
	runGitCommand(t, repoDir, "add", "value.go")
	runGitCommand(t, repoDir, "-c", "user.name=Mutation Test", "-c", "user.email=mutation@example.com", "commit", "--quiet", "-m", "second")
	require.NoError(t, workspaces.add(t.Context()))

	for _, worktreeDir := range workspaces.dirs {
		require.Equal(t, firstCommit, runGitCommand(t, worktreeDir, "rev-parse", "HEAD"))
	}
}
