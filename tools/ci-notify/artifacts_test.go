package cinotify

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReportableFailures(t *testing.T) {
	rows := []summaryRow{
		{Kind: "Failed", Name: "TestHistoryWorkflow (retry 1) (final)", Final: true},
		{Kind: "Failed", Name: "TestRetryFailure (retry 1)"},
		{Kind: summaryKindOOM, Name: "OOM prevention"},
	}

	require.Equal(t, []string{"TestHistoryWorkflow", "OOM"}, reportableFailures(rows))
}

func TestFailuresFromZip(t *testing.T) {
	dir := t.TempDir()
	zipPath := filepath.Join(dir, "artifact.zip")
	file, err := os.Create(zipPath)
	require.NoError(t, err)

	writer := zip.NewWriter(file)
	summaryFile, err := writer.Create("test-summary.json")
	require.NoError(t, err)
	_, err = summaryFile.Write([]byte(`{
  "rows": [
    {
      "kind": "Failed",
      "name": "TestMatchingWorkflow (final)",
      "final": true
    },
    {
      "kind": "OOM",
      "name": "OOM prevention"
    }
  ]
}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())

	failures, err := failuresFromZip(zipPath)
	require.NoError(t, err)
	require.Equal(t, []string{"TestMatchingWorkflow", "OOM"}, failures)
}
