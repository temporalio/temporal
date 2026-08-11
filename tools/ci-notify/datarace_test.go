package cinotify

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/github"
)

func TestDataRacesFromRows(t *testing.T) {
	rows := []summaryRow{
		{Kind: "Failed", Name: "TestFoo (final)", Final: true},
		{
			Kind:    "DATA RACE",
			Name:    "DATA RACE: Data race detected — in go.temporal.io/server/service/history.TestBar",
			Details: "WARNING: DATA RACE\nRead at 0x00c0...",
		},
		{Kind: "OOM", Name: "OOM prevention"},
		{Kind: "PANIC", Name: "PANIC: boom"},
	}

	races := dataRacesFromRows(rows)

	require.Len(t, races, 1)
	require.Equal(t, "Data race detected — in go.temporal.io/server/service/history.TestBar", races[0].Location)
	require.Contains(t, races[0].Details, "WARNING: DATA RACE")
}

func TestDataRacesFromRowsNone(t *testing.T) {
	rows := []summaryRow{
		{Kind: "Failed", Name: "TestFoo (final)", Final: true},
		{Kind: "OOM", Name: "OOM prevention"},
	}
	require.Empty(t, dataRacesFromRows(rows))
}

func TestUniqueDataRaces(t *testing.T) {
	// The same race is reported by every shard/attempt that hit it.
	races := []DataRace{
		{Location: "TestBar", Details: "race body"},
		{Location: "TestBar", Details: "race body"},
		{Location: "TestBaz", Details: "race body"},  // same details, different location
		{Location: "TestBar", Details: "other body"}, // same location, different details
	}

	unique := uniqueDataRaces(races)

	require.Len(t, unique, 3)
	require.Equal(t, "TestBar", unique[0].Location)
	require.Equal(t, "TestBaz", unique[1].Location)
	require.Equal(t, "other body", unique[2].Details)
}

func TestSummaryRowsFromZipParsesDataRaceDetails(t *testing.T) {
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
      "kind": "DATA RACE",
      "name": "DATA RACE: Data race detected — in pkg.TestRacy",
      "details": "WARNING: DATA RACE\nWrite at 0x00c000"
    }
  ]
}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())

	rows, err := summaryRowsFromZip(zipPath)
	require.NoError(t, err)

	races := dataRacesFromRows(rows)
	require.Len(t, races, 1)
	require.Equal(t, "Data race detected — in pkg.TestRacy", races[0].Location)
	require.Contains(t, races[0].Details, "WARNING: DATA RACE")
}

func TestBuildDataRaceMessage(t *testing.T) {
	report := &DataRaceReport{
		Run: github.Run{
			HeadSHA: "abc1234567890defghijk",
			URL:     "https://github.com/temporalio/temporal/actions/runs/123456",
		},
		Author: "Test Author",
		Title:  "Some commit title",
		DataRaces: []DataRace{
			{Location: "in pkg.TestRacy", Details: "WARNING: DATA RACE\nstack..."},
		},
	}

	msg := BuildDataRaceMessage(report)

	require.Contains(t, msg.Text, "Data Race Detected on Main (1)")

	rendered := msg.RenderMarkdown()
	require.Contains(t, rendered, "Data Race Detected on Main Branch")
	require.Contains(t, rendered, "Test Author")
	require.Contains(t, rendered, "in pkg.TestRacy")
	require.Contains(t, rendered, "WARNING: DATA RACE")
	require.Contains(t, rendered, "abc1234") // short SHA link
}

func TestBuildDataRaceMessageUnknownAuthor(t *testing.T) {
	report := &DataRaceReport{
		Run:       github.Run{HeadSHA: "abc1234567890", URL: "https://example.com/run"},
		DataRaces: []DataRace{{Location: "in pkg.TestRacy", Details: "body"}},
	}

	require.Contains(t, BuildDataRaceMessage(report).RenderMarkdown(), "Unknown")
}

func TestTruncateDataRaceDetail(t *testing.T) {
	require.Equal(t, "short", truncateDataRaceDetail("short"))

	long := truncateDataRaceDetail(string(make([]byte, maxDataRaceDetail+10)))
	require.Contains(t, long, "truncated")
	require.Less(t, len(long), maxDataRaceDetail+len("\n… (truncated — see full output in job logs)")+10)
}
