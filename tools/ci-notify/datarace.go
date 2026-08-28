package cinotify

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
)

// maxRaceSites caps how many conflicting-access lines we surface per race in
// case the test-runner merged several race reports into one alert.
const maxRaceSites = 6

var (
	// raceAccessRegex matches a race detector access header. The current access
	// is "Read at"/"Write at"; the conflicting one is "Previous read at"/
	// "Previous write at". At least one is always a write, but a race can be
	// write/write, so both kinds must be matched.
	raceAccessRegex = regexp.MustCompile(`^(Read|Write|Previous read|Previous write) at 0x[0-9a-fA-F]+ by (goroutine \d+|main goroutine)`)
	// raceFileLineRegex matches a "<path>.go:<line>" stack frame location.
	raceFileLineRegex = regexp.MustCompile(`(\S+\.go):(\d+)`)
	// repoPathPrefixRegex strips the CI checkout prefix (e.g.
	// /home/runner/work/temporal/temporal/) so paths read as service/history/….
	repoPathPrefixRegex = regexp.MustCompile(`^.*/temporal/temporal/`)
)

// temporalModulePrefix is trimmed from race locations so we show, e.g.,
// "service/history.TestFoo" instead of the fully-qualified package path.
const temporalModulePrefix = "go.temporal.io/server/"

// DataRace is a single data race detected by the Go race detector during a CI
// run. The test-runner surfaces these into the JUnit ALERTS suite and the
// test-summary.json artifact (see tools/testrunner/junit.go).
type DataRace struct {
	// Location is the package-qualified test where the race was detected,
	// e.g. "service/history.TestFoo".
	Location string
	// Details is the race detector's report (stacktraces), possibly truncated.
	Details string
	// JobID is the GitHub Actions job that reported the race, used to link
	// directly to the offending job. Empty when it can't be determined.
	JobID string
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
	err := forEachSummaryZip(ctx, runID, func(artifactName, zipPath string) {
		rows, err := summaryRowsFromZip(zipPath)
		if err != nil {
			return
		}
		races = append(races, dataRacesFromRows(rows, jobIDFromArtifactName(artifactName))...)
	})
	if err != nil {
		return nil, err
	}
	return uniqueDataRaces(races), nil
}

// dataRacesFromRows extracts data race rows from a parsed test summary, tagging
// each with the job that produced the artifact.
func dataRacesFromRows(rows []summaryRow, jobID string) []DataRace {
	var races []DataRace
	for _, row := range rows {
		if row.Kind != summaryKindDataRace {
			continue
		}
		races = append(races, DataRace{
			Location: dataRaceLocation(row.Name),
			Details:  row.Details,
			JobID:    jobID,
		})
	}
	return races
}

// dataRaceLocation reduces a test-runner alert name like
// "DATA RACE: Data race detected — in go.temporal.io/server/service/history.TestFoo"
// to just the package-qualified test, e.g. "service/history.TestFoo".
func dataRaceLocation(name string) string {
	loc := strings.TrimSpace(strings.TrimPrefix(name, summaryKindDataRace+":"))
	if _, after, ok := strings.Cut(loc, "— in "); ok {
		loc = strings.TrimSpace(after)
	}
	return strings.TrimPrefix(loc, temporalModulePrefix)
}

// raceSite is one conflicting memory access from a race detector report.
type raceSite struct {
	access    string // "Read", "Write", "Previous read", or "Previous write"
	goroutine string // "goroutine 8" or "main goroutine"
	location  string // "service/history/mutable_state.go:127"
}

// parseRaceSites reduces a race detector report to the conflicting memory
// accesses and the source line each occurred at. It handles read/write and
// write/write races. Returns nil when the report can't be parsed, so callers can
// degrade to just the location and job link.
func parseRaceSites(details string) []raceSite {
	lines := strings.Split(details, "\n")
	var sites []raceSite
	for i, line := range lines {
		m := raceAccessRegex.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}
		if loc := firstGoFrame(lines[i+1:]); loc != "" {
			sites = append(sites, raceSite{access: m[1], goroutine: m[2], location: loc})
			if len(sites) == maxRaceSites {
				break
			}
		}
	}
	return sites
}

// raceSites formats the conflicting accesses for display, e.g.
//
//	Read at (goroutine 8): service/history/mutable_state.go:127
//	Previous write at (goroutine 7): service/history/mutable_state.go:130
func raceSites(details string) []string {
	sites := parseRaceSites(details)
	out := make([]string, 0, len(sites))
	for _, s := range sites {
		out = append(out, fmt.Sprintf("%s at (%s): %s", s.access, s.goroutine, s.location))
	}
	return out
}

// raceAffectedLines returns the sorted, de-duplicated source lines involved in a
// race. Unlike the raw report, this excludes memory addresses and goroutine ids
// (which differ every run), so it identifies a race stably across shards.
func raceAffectedLines(details string) []string {
	sites := parseRaceSites(details)
	locations := make([]string, 0, len(sites))
	for _, s := range sites {
		locations = append(locations, s.location)
	}
	slices.Sort(locations)
	return slices.Compact(locations)
}

// firstGoFrame returns "<path>.go:<line>" for the first application stack frame
// following an access header, skipping the race detector's runtime shim frames
// (e.g. runtime.raceread in race_amd64.s). It stops at the blank line that ends
// the access's stack.
func firstGoFrame(lines []string) string {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			return "" // end of this access's stack; no application frame found
		}
		m := raceFileLineRegex.FindStringSubmatch(trimmed)
		if m == nil || strings.Contains(m[1], "/src/runtime/") {
			continue // function-name line, assembly shim, or Go runtime internals
		}
		return trimRepoPath(m[1]) + ":" + m[2]
	}
	return ""
}

// trimRepoPath strips the CI checkout prefix from an absolute source path.
func trimRepoPath(path string) string {
	return repoPathPrefixRegex.ReplaceAllString(path, "")
}

// jobIDFromArtifactName extracts the job ID from a test-summary artifact name of
// the form "test-summary-json--<run_id>--<job_id>--<run_attempt>--<suffix>".
// Returns "" when the name doesn't carry a job ID.
func jobIDFromArtifactName(name string) string {
	parts := strings.Split(name, "--")
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// uniqueDataRaces removes duplicate races (the same race is reported by every
// shard/attempt that hit it) while preserving first-seen order. Races are keyed
// by their test and affected source lines rather than the raw report, so the
// same race still de-duplicates despite the differing memory addresses and
// goroutine ids the detector prints each run.
func uniqueDataRaces(races []DataRace) []DataRace {
	seen := make(map[string]struct{}, len(races))
	var unique []DataRace
	for _, race := range races {
		key := raceFingerprint(race)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, race)
	}
	return unique
}

// raceFingerprint is a stable identity for a race: its test plus the set of
// affected source lines. Falls back to the raw report when no source lines can
// be parsed, so unparseable reports aren't over-merged.
func raceFingerprint(race DataRace) string {
	lines := raceAffectedLines(race.Details)
	if len(lines) == 0 {
		return race.Location + "\n" + race.Details
	}
	return race.Location + "\n" + strings.Join(lines, "\n")
}
