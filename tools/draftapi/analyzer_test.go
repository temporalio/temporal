package draftapi

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
	"golang.org/x/tools/go/analysis"
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
	analysistest.Run(t, dir, withoutPlacementErrors(), "./usage", "./tests/...", "./common/testing/...")
}

// withoutPlacementErrors is Analyzer without the placement rejection. The fixture keeps
// annotations on placements that declaration validation rejects, so that their usage
// classification stays tested. Other tests cover the rejection itself.
func withoutPlacementErrors() *analysis.Analyzer {
	const placement = "draft annotations are supported only on fields and messages"
	analyzer := *Analyzer
	analyzer.Run = func(pass *analysis.Pass) (any, error) {
		filtered := *pass
		filtered.Report = func(diagnostic analysis.Diagnostic) {
			if !strings.Contains(diagnostic.Message, placement) {
				pass.Report(diagnostic)
			}
		}
		filtered.ExportPackageFact = func(fact analysis.Fact) {
			metadata := fact.(*optionsFact)
			metadata.Errors = slices.DeleteFunc(metadata.Errors, func(err string) bool { return strings.Contains(err, placement) })
			if len(metadata.Options) > 0 || len(metadata.Errors) > 0 {
				pass.ExportPackageFact(metadata)
			}
		}
		return Analyzer.Run(&filtered)
	}
	return &analyzer
}
