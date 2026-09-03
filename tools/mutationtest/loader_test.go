package mutationtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadTargetsReturnsTypedCanonicalFiles(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repoDir := copySmokeFixture(t)

	targets, err := loadTargets(ctx, repoDir, []string{"value.go"}, "test_dep")
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.Equal(t, "example.com/smoke", targets[0].packagePath)
	require.Equal(t, "value.go", targets[0].relativePath)
	require.Equal(t, "example.com/smoke/value.go", targets[0].coveragePath)
	require.NotNil(t, targets[0].syntax)
	require.NotNil(t, targets[0].types)
	require.NotNil(t, targets[0].typesInfo)
}

func copySmokeFixture(t *testing.T) string {
	t.Helper()
	return copyFixture(t, "repo")
}

func copyFixture(t *testing.T, name string) string {
	t.Helper()
	repoDir := t.TempDir()
	require.NoError(t, os.CopyFS(repoDir, os.DirFS(filepath.Join("testdata", name))))
	require.NoError(t, os.Rename(filepath.Join(repoDir, "go.mod.txt"), filepath.Join(repoDir, "go.mod")))
	return repoDir
}
