package cinotify

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
)

// DataRace is a single data race detected by the Go race detector during a CI
// run. The test-runner surfaces these into the JUnit ALERTS suite and the
// test-summary.json artifact (see tools/testrunner/junit.go).
type DataRace struct {
	// Location is a human-readable description of where the race was detected,
	// e.g. "Data race detected — in go.temporal.io/server/service/history.TestFoo".
	Location string
	// Details is the race detector's report (stacktraces), possibly truncated.
	Details string
}

// DataRaceReport aggregates the data races found in a single CI run along with
// the commit context needed to attribute and link them.
type DataRaceReport struct {
	Run       github.Run
	Author    string
	Title     string
	DataRaces []DataRace
}

// BuildDataRaceReport fetches the run's test summaries, extracts any data races,
// and enriches them with commit metadata. It returns a report with an empty
// DataRaces slice when no races were detected.
func BuildDataRaceReport(runID string) (*DataRaceReport, error) {
	run, err := getWorkflowRun(runID)
	if err != nil {
		return nil, err
	}

	races, err := getDataRaces(context.Background(), run.DatabaseID)
	if err != nil {
		return nil, err
	}

	report := &DataRaceReport{
		Run:       *run,
		Title:     run.DisplayTitle,
		DataRaces: races,
	}

	// Commit metadata is best-effort: a missing author must not suppress the alert.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if commit, err := github.GetCommit(ctx, temporalRepository, run.HeadSHA); err == nil {
		report.Author = commit.Commit.Author.Name
		if title := commit.Title(); title != "" {
			report.Title = title
		}
	}

	return report, nil
}

// getDataRaces downloads the run's test-summary artifacts and returns the unique
// set of data races reported across all of them.
func getDataRaces(ctx context.Context, runID int64) ([]DataRace, error) {
	var races []DataRace
	err := forEachSummaryZip(ctx, runID, func(zipPath string) {
		rows, err := summaryRowsFromZip(zipPath)
		if err != nil {
			return
		}
		races = append(races, dataRacesFromRows(rows)...)
	})
	if err != nil {
		return nil, err
	}
	return uniqueDataRaces(races), nil
}

// dataRacesFromRows extracts data race rows from a parsed test summary.
func dataRacesFromRows(rows []summaryRow) []DataRace {
	var races []DataRace
	for _, row := range rows {
		if row.Kind != summaryKindDataRace {
			continue
		}
		races = append(races, DataRace{
			Location: dataRaceLocation(row.Name),
			Details:  row.Details,
		})
	}
	return races
}

// dataRaceLocation strips the redundant "DATA RACE: " prefix that the test-runner
// prepends to alert names, leaving just the descriptive location.
func dataRaceLocation(name string) string {
	return strings.TrimSpace(strings.TrimPrefix(name, summaryKindDataRace+":"))
}

// uniqueDataRaces removes duplicate races (the same race is reported by every
// shard/attempt that hit it) while preserving first-seen order.
func uniqueDataRaces(races []DataRace) []DataRace {
	seen := make(map[string]struct{}, len(races))
	var unique []DataRace
	for _, race := range races {
		key := fmt.Sprintf("%s\n%s", race.Location, race.Details)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, race)
	}
	return unique
}
