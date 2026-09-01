package mutationtest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareTestFilesOnlyRenamesUnselectedTests(t *testing.T) {
	t.Parallel()

	worktreeDir := t.TempDir()
	selectedTest := filepath.Join(worktreeDir, "selected_test.go")
	excludedTest := filepath.Join(worktreeDir, "excluded_test.go")
	sourceFile := filepath.Join(worktreeDir, "source.go")

	require.NoError(t, os.WriteFile(selectedTest, []byte("package mutationtest\n"), 0o644))
	require.NoError(t, os.WriteFile(excludedTest, []byte("package mutationtest\n"), 0o644))
	require.NoError(t, os.WriteFile(sourceFile, []byte("package mutationtest\n"), 0o644))

	err := prepareTestFiles(worktreeDir, []string{"selected_test.go"})
	require.NoError(t, err)

	_, err = os.Stat(selectedTest)
	require.NoError(t, err)

	_, err = os.Stat(excludedTest)
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = os.Stat(excludedTest + ".excluded")
	require.NoError(t, err)

	_, err = os.Stat(sourceFile)
	require.NoError(t, err)
}

func TestPrepareRunDirPreservesUnrelatedFiles(t *testing.T) {
	t.Parallel()

	runDir := t.TempDir()
	unrelatedPath := filepath.Join(runDir, "keep.txt")
	require.NoError(t, os.WriteFile(unrelatedPath, []byte("keep me"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "summary.txt"), []byte("stale"), 0o644))

	preparedDir, err := prepareRunDir(t.Context(), runDir)
	require.NoError(t, err)
	require.Equal(t, runDir, preparedDir)

	contents, err := os.ReadFile(unrelatedPath)
	require.NoError(t, err)
	require.Equal(t, []byte("keep me"), contents)
	_, err = os.Stat(filepath.Join(runDir, "summary.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPrepareRunDirRejectsGitRepositoryRoot(t *testing.T) {
	t.Parallel()

	repoRoot := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(repoRoot, ".git"), 0o755))
	sentinelPath := filepath.Join(repoRoot, "tracked.go")
	require.NoError(t, os.WriteFile(sentinelPath, []byte("package tracked\n"), 0o644))

	_, err := prepareRunDir(t.Context(), repoRoot)
	require.Error(t, err)

	contents, readErr := os.ReadFile(sentinelPath)
	require.NoError(t, readErr)
	require.Equal(t, []byte("package tracked\n"), contents)
}
