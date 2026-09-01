package cinotify

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
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

	races := dataRacesFromRows(rows, "job-1")

	require.Len(t, races, 1)
	require.Equal(t, "service/history.TestBar", races[0].Location)
	require.Equal(t, "job-1", races[0].JobID)
	require.Contains(t, races[0].Details, "WARNING: DATA RACE")
}

func TestDataRacesFromRowsNone(t *testing.T) {
	rows := []summaryRow{
		{Kind: "Failed", Name: "TestFoo (final)", Final: true},
		{Kind: "OOM", Name: "OOM prevention"},
	}
	require.Empty(t, dataRacesFromRows(rows, "job-1"))
}

func TestDataRaceLocation(t *testing.T) {
	require.Equal(t,
		"service/history.TestBar",
		dataRaceLocation("DATA RACE: Data race detected — in go.temporal.io/server/service/history.TestBar"),
	)
	// A race not attributed to a temporal package keeps its qualified name.
	require.Equal(t,
		"example.com/foo.TestBaz",
		dataRaceLocation("DATA RACE: Data race detected — in example.com/foo.TestBaz"),
	)
	// A name without the "— in" locator falls back to the summary text.
	require.Equal(t, "Data race detected", dataRaceLocation("DATA RACE: Data race detected"))
}

func TestJobIDFromArtifactName(t *testing.T) {
	require.Equal(t, "12345", jobIDFromArtifactName("test-summary-json--999--12345--1--unit-test"))
	require.Empty(t, jobIDFromArtifactName("test-summary-json--999"))
}

func TestUniqueDataRaces(t *testing.T) {
	// The same race reported by two shards: identical source lines, but the race
	// detector prints different memory addresses and goroutine ids each run.
	shard1 := readWriteRaceDetails
	shard2 := strings.NewReplacer(
		"0x00c0001121e8", "0xdeadbeef",
		"goroutine 8", "goroutine 42",
		"goroutine 7", "goroutine 99",
	).Replace(shard1)

	// A race at a different source line is a distinct race.
	otherLine := strings.ReplaceAll(shard1, "mutable_state.go:130", "mutable_state.go:200")

	races := []DataRace{
		{Location: "service/history.TestMS", Details: shard1, JobID: "1"},
		{Location: "service/history.TestMS", Details: shard2, JobID: "2"}, // dup of shard1
		{Location: "service/history.TestMS", Details: otherLine},          // different line
		{Location: "service/matching.TestCache", Details: shard1},         // different test
	}

	unique := uniqueDataRaces(races)

	require.Len(t, unique, 3)
	require.Equal(t, "1", unique[0].JobID) // first occurrence wins
	require.Equal(t, "service/history.TestMS", unique[1].Location)
	require.Equal(t, "service/matching.TestCache", unique[2].Location)
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
      "name": "DATA RACE: Data race detected — in go.temporal.io/server/pkg.TestRacy",
      "details": "WARNING: DATA RACE\nWrite at 0x00c000"
    }
  ]
}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())

	rows, err := summaryRowsFromZip(zipPath)
	require.NoError(t, err)

	races := dataRacesFromRows(rows, "job-9")
	require.Len(t, races, 1)
	require.Equal(t, "pkg.TestRacy", races[0].Location)
	require.Contains(t, races[0].Details, "WARNING: DATA RACE")
}

// readWriteRaceDetails is a read/write race whose top frame is the race
// detector's runtime shim (which must be skipped) and whose source paths use the
// CI checkout prefix (which must be trimmed).
const readWriteRaceDetails = `==================
WARNING: DATA RACE
Read at 0x00c0001121e8 by goroutine 8:
  runtime.raceread()
      /usr/local/go/src/runtime/race_amd64.s:260 +0x21
  go.temporal.io/server/service/history.(*ms).Get()
      /home/runner/work/temporal/temporal/service/history/mutable_state.go:127 +0x2c

Previous write at 0x00c0001121e8 by goroutine 7:
  runtime.racewrite()
      /usr/local/go/src/runtime/race_amd64.s:269 +0x21
  go.temporal.io/server/service/history.(*ms).Set()
      /home/runner/work/temporal/temporal/service/history/mutable_state.go:130 +0x3c

Goroutine 8 (running) created at:
  go.temporal.io/server/service/history.TestMutableState()
      /home/runner/work/temporal/temporal/service/history/mutable_state_test.go:41 +0x88
==================`

// writeWriteRaceDetails is a race between two concurrent writes.
const writeWriteRaceDetails = `==================
WARNING: DATA RACE
Write at 0x00c000000180 by goroutine 7:
  go.temporal.io/server/service/matching.(*cache).put()
      /home/runner/work/temporal/temporal/service/matching/cache.go:88 +0x66

Previous write at 0x00c000000180 by goroutine 12:
  go.temporal.io/server/service/matching.(*cache).put()
      /home/runner/work/temporal/temporal/service/matching/cache.go:88 +0x66
==================`

func TestRaceSites(t *testing.T) {
	require.Equal(t, []string{
		"Read at (goroutine 8): service/history/mutable_state.go:127",
		"Previous write at (goroutine 7): service/history/mutable_state.go:130",
	}, raceSites(readWriteRaceDetails))

	// A write/write race is still a race; both writes must be surfaced.
	require.Equal(t, []string{
		"Write at (goroutine 7): service/matching/cache.go:88",
		"Previous write at (goroutine 12): service/matching/cache.go:88",
	}, raceSites(writeWriteRaceDetails))

	// Unparseable details yield no sites; the caller falls back to location + link.
	require.Empty(t, raceSites("some unrelated text"))
}

func TestBuildDataRaceMessageLinksToJob(t *testing.T) {
	report := &DataRaceReport{
		Run: github.Run{
			DatabaseID: 123456,
			HeadSHA:    "abc1234567890defghijk",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123456",
		},
		Author: "Test Author",
		Title:  "Some commit title",
		DataRaces: []DataRace{
			{Location: "service/history.TestMutableState", Details: readWriteRaceDetails, JobID: "789"},
		},
	}

	rendered := BuildDataRaceMessage(report).RenderMarkdown()

	require.Contains(t, rendered, "Data Race Detected on Main Branch")
	require.Contains(t, rendered, "Test Author")
	require.Contains(t, rendered, "service/history.TestMutableState")
	require.Contains(t, rendered, "Read at (goroutine 8): service/history/mutable_state.go:127")
	require.Contains(t, rendered, "Previous write at (goroutine 7): service/history/mutable_state.go:130")
	require.Contains(t, rendered, "abc1234") // short SHA link
	// Links to the specific job, not just the top-level run.
	require.Contains(t, rendered, "actions/runs/123456/job/789")
	// Runtime shim frames and raw stacktrace noise are not dumped into Slack.
	require.NotContains(t, rendered, "race_amd64.s")
}

func TestBuildDataRaceMessageFallsBackToRunLink(t *testing.T) {
	report := &DataRaceReport{
		Run:       github.Run{DatabaseID: 123456, HeadSHA: "abc1234567890"},
		DataRaces: []DataRace{{Location: "pkg.TestRacy", Details: "unparseable"}}, // no JobID
	}

	rendered := BuildDataRaceMessage(report).RenderMarkdown()

	require.Contains(t, rendered, "Unknown") // author
	require.Contains(t, rendered, "pkg.TestRacy")
	require.Contains(t, rendered, "actions/runs/123456")
	require.NotContains(t, rendered, "/job/")
}
