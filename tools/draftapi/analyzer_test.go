package draftapi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS("testdata")))
	data, err := os.ReadFile("../../go.mod")
	require.NoError(t, err)
	mod, err := modfile.Parse("go.mod", data, nil)
	require.NoError(t, err)
	for _, replacement := range mod.Replace {
		if replacement.Old.Path == "go.temporal.io/api" {
			require.NoError(t, mod.DropReplace(replacement.Old.Path, replacement.Old.Version))
		}
	}
	require.NoError(t, mod.AddReplace("go.temporal.io/api", "", "./api", ""))
	data, err = mod.Format()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), data, 0600))
	data, err = os.ReadFile("../../go.sum")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.sum"), data, 0600))
	analysistest.Run(t, dir, Analyzer, "./usage", "./tests/...", "./common/testing/...")
}
