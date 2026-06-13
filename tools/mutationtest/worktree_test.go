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
