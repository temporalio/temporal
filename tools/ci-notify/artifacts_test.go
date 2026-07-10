package cinotify

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinalFailures(t *testing.T) {
	rows := []summaryRow{
		{Name: "TestHistoryWorkflow (retry 1) (final)", Final: true},
		{Name: "TestRetryFailure (retry 1)"},
	}

	require.Equal(t, []string{"TestHistoryWorkflow"}, finalFailures(rows))
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
	    }
	  ]
	}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())

	failures, err := failuresFromZip(zipPath)
	require.NoError(t, err)
	require.Equal(t, []string{"TestMatchingWorkflow"}, failures)
}
