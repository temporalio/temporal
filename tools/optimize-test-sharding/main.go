package optimizetestsharding

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/tools/common/github"
	"go.temporal.io/server/tools/common/junit"
)

const (
	defaultLevel                 = 2 // 1 means shard by suite, 2 means shard by test
	downloadAttempts             = 3
	downloadRetryInitialInterval = 5 * time.Second
)

var retrySuffixRe = regexp.MustCompile(` \(retry \d+\)$`)

func Main() error {
	shards := flag.Int("shards", 0, "Number of shards (required)")
	tries := flag.Int("tries", 10000, "Number of tries")
	workflow := flag.String("workflow", "", "GitHub Actions workflow name to fetch artifacts from (uses gh CLI)")
	artifactPattern := flag.String("artifact-pattern", "", "Artifact name pattern for gh run download")
	days := flag.Int("days", 7, "Number of days of successful runs to sample")
	runs := flag.Int("runs", 200, "Maximum number of successful runs to download")
	branch := flag.String("branch", "main", "Branch to find successful runs on")
	event := flag.String("event", "push", "Event type to filter runs by")
	saltFile := flag.String("file", "", "Path to the salt file to read/update (required)")
	threshold := flag.Float64("threshold", 0, "Minimum improvement ratio (e.g. 0.05 for 5%) required to update the salt file")
	flag.Usage = func() {
		log.Printf("Usage: %s [options]", os.Args[0])
		log.Print("Optimizes the salt selection for test sharding.")
		log.Print("Uses -workflow and -artifact-pattern to download JUnit XML files")
		log.Print("from recent successful GitHub Actions runs via gh CLI.")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *shards < 1 {
		return errors.New("-shards is required and must be >= 1")
	}
	if *tries < 1 {
		return errors.New("-tries must be >= 1")
	}
	if *workflow == "" || *artifactPattern == "" {
		return errors.New("-workflow and -artifact-pattern are required")
	}
	if *saltFile == "" {
		return errors.New("-file is required")
	}
	if *days < 1 {
		return errors.New("-days must be >= 1")
	}
	if *runs < 1 {
		return errors.New("-runs must be >= 1")
	}

	currentSaltBytes, err := os.ReadFile(*saltFile)
	if err != nil {
		return fmt.Errorf("reading salt file: %w", err)
	}
	currentSalt := strings.TrimSpace(string(currentSaltBytes))
	log.Printf("Current salt: %s", currentSalt)

	created := ">=" + time.Now().UTC().AddDate(0, 0, -*days).Format(time.DateOnly)
	dir, err := downloadArtifacts(*workflow, *artifactPattern, *branch, *event, created, *runs)
	if err != nil {
		return fmt.Errorf("downloading artifacts: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	tmap, err := loadTestData(dir)
	if err != nil {
		return fmt.Errorf("loading test data: %w", err)
	}

	log.Printf("Loaded %d unique test names", len(tmap))

	smap := aggregateRuns(tmap)
	log.Printf("Aggregated to %d entries for sharding", len(smap))

	var totalTime float64
	for _, t := range smap {
		totalTime += t
	}
	log.Printf("Total test time: %.1fs", totalTime)

	bestSalt, bestMax := optimizeShardingSalt(smap, *shards, *tries)
	log.Printf("Best salt: %s (max shard: %.1fs, ideal: %.1fs)",
		bestSalt, bestMax, totalTime/float64(*shards))

	// Log per-shard breakdown for the winning salt.
	totals := shardTotals(smap, *shards, bestSalt)
	for i, t := range totals {
		log.Printf("  shard %d: %.1fs", i, t)
	}

	currentMax := maxShardTime(smap, *shards, currentSalt)
	improvement := (currentMax - bestMax) / currentMax
	log.Printf("Current salt %q max shard: %.1fs, improvement: %.1f%%", currentSalt, currentMax, improvement*100)

	if improvement < *threshold {
		log.Printf("Improvement %.1f%% is below threshold %.1f%%, keeping current salt", improvement*100, *threshold*100)
		return nil
	}

	if err := os.WriteFile(*saltFile, []byte(bestSalt+"\n"), 0644); err != nil {
		return fmt.Errorf("writing salt file: %w", err)
	}
	log.Printf("Updated salt file %s to %s", *saltFile, bestSalt)
	return nil
}

// downloadArtifacts uses gh CLI to find the latest successful runs and download
// matching artifacts. Returns the path to a temp directory containing the files.
func downloadArtifacts(workflow, artifactPattern, branch, event, created string, limit int) (string, error) {
	runIDs, err := findLatestRuns(workflow, branch, event, created, limit)
	if err != nil {
		return "", err
	}

	dir, err := os.MkdirTemp("", "test-sharding-*")
	if err != nil {
		return "", err
	}

	var downloaded int
	for _, runID := range runIDs {
		log.Printf("Downloading artifacts from run %s", runID)
		runDir := filepath.Join(dir, runID)
		if err := downloadRunArtifacts(
			context.Background(),
			runID,
			artifactPattern,
			runDir,
			downloadRetryInitialInterval,
			github.RunDownload,
		); err != nil {
			_ = os.RemoveAll(dir)
			return "", err
		}
		downloaded++
	}
	if downloaded == 0 {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("no runs had artifacts matching %q", artifactPattern)
	}

	return dir, nil
}

type artifactDownloader func(context.Context, string, github.RunDownloadOptions) error

func downloadRunArtifacts(
	ctx context.Context,
	runID string,
	artifactPattern string,
	runDir string,
	retryInterval time.Duration,
	download artifactDownloader,
) error {
	attempt := 0
	policy := backoff.NewExponentialRetryPolicy(retryInterval).WithMaximumAttempts(downloadAttempts)
	err := backoff.ThrottleRetryContext(ctx, func(ctx context.Context) error {
		attempt++
		if attempt > 1 {
			log.Printf("Retrying artifact download for run %s (attempt %d/%d)", runID, attempt, downloadAttempts)
		}
		if err := os.RemoveAll(runDir); err != nil {
			return err
		}
		return download(ctx, runID, github.RunDownloadOptions{
			Pattern: artifactPattern,
			Dir:     runDir,
		})
	}, policy, nil)
	if err != nil {
		_ = os.RemoveAll(runDir)
		return fmt.Errorf("downloading artifacts from run %s failed after %d attempts: %w", runID, attempt, err)
	}
	return nil
}

func findLatestRuns(workflow, branch, event, created string, limit int) ([]string, error) {
	runs, err := github.ListRuns(context.Background(), github.RunListOptions{
		Workflow: workflow,
		Event:    event,
		Branch:   branch,
		Status:   "success",
		Created:  created,
		Limit:    limit,
	})
	if err != nil {
		return nil, fmt.Errorf("gh run list: %w", err)
	}
	if len(runs) == 0 {
		return nil, fmt.Errorf("no successful runs found for workflow %s on %s/%s", workflow, branch, event)
	}

	ids := make([]string, len(runs))
	for i, r := range runs {
		ids[i] = strconv.FormatInt(r.DatabaseID, 10)
	}
	return ids, nil
}

// loadTestData returns a map of test names to per-run durations in seconds.
func loadTestData(dir string) (map[string][]float64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	tmap := make(map[string][]float64)
	var runs int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runDir := filepath.Join(dir, entry.Name())
		units, err := loadRunData(runDir)
		if err != nil {
			return nil, fmt.Errorf("loading run %s: %w", entry.Name(), err)
		}
		if len(units) == 0 {
			return nil, fmt.Errorf("run %s produced no test sharding units", entry.Name())
		}
		for name, seconds := range units {
			tmap[name] = append(tmap[name], seconds)
		}
		runs++
	}
	if runs == 0 {
		return nil, errors.New("no test run directories found")
	}
	log.Printf("Loaded test data from %d run(s)", runs)
	return tmap, nil
}

// loadRunData combines the separately uploaded shard and database artifacts into one run. Their
// durations are additive; only retry attempts within an individual artifact collapse to a max.
func loadRunData(dir string) (map[string]float64, error) {
	type junitFile struct {
		path       string
		artifact   string
		attempt    int
		recognized bool
	}

	var found []junitFile
	latestAttempts := make(map[string]int)
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".xml") {
			relative, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			artifact, attempt, recognized := junitArtifactAttempt(relative)
			found = append(found, junitFile{
				path:       path,
				artifact:   artifact,
				attempt:    attempt,
				recognized: recognized,
			})
			if recognized {
				latestAttempts[artifact] = max(latestAttempts[artifact], attempt)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(found) == 0 {
		return nil, errors.New("no XML files found")
	}

	files := make([]string, 0, len(found))
	for _, file := range found {
		if !file.recognized || file.attempt == latestAttempts[file.artifact] {
			files = append(files, file.path)
		}
	}
	log.Printf("Found %d XML file(s) in %s", len(files), filepath.Base(dir))

	units := make(map[string]float64)
	for _, filename := range files {
		cases, err := processJUnitReport(filename)
		if err != nil {
			return nil, fmt.Errorf("processing %s: %w", filename, err)
		}
		for name, seconds := range shardingUnits(cases) {
			units[name] += seconds
		}
	}
	return units, nil
}

func junitArtifactAttempt(path string) (string, int, bool) {
	artifactDir := strings.SplitN(path, string(filepath.Separator), 2)[0]
	parts := strings.Split(artifactDir, "--")
	if len(parts) < 5 || parts[0] != "junit-xml" {
		return "", 0, false
	}
	attempt, err := strconv.Atoi(parts[3])
	if err != nil || attempt < 1 {
		return "", 0, false
	}
	return strings.Join(parts[4:], "--"), attempt, true
}

func processJUnitReport(filename string) ([]junit.Testcase, error) {
	testsuites, err := junit.Read(filename)
	if err != nil {
		return nil, err
	}

	var cases []junit.Testcase
	for _, suite := range testsuites.Suites {
		cases = append(cases, suite.Testcases...)
	}

	return cases, nil
}

func normalizeTestName(name string) string {
	name = strings.TrimSuffix(name, " (final)")
	return retrySuffixRe.ReplaceAllString(name, "")
}

// shardingUnits reduces one artifact to the depth-2 names hashed by the functional test runtime.
// Skipped cases did not consume time on this shard, retry attempts represent one test run, and
// parent entries report the sum of their subtests, so retaining any of them would double-count.
func shardingUnits(cases []junit.Testcase) map[string]float64 {
	observed := make(map[string]float64, len(cases))
	for _, tc := range cases {
		if tc.Skipped != nil {
			continue
		}
		name := normalizeTestName(tc.Name)
		if name == "" {
			continue
		}
		seconds, err := strconv.ParseFloat(tc.Time, 64)
		if err != nil || seconds < 0 {
			seconds = 0
		}
		observed[name] = max(observed[name], seconds)
	}

	names := slices.Sorted(maps.Keys(observed))
	units := make(map[string]float64, len(observed))
	for i, name := range names {
		if i+1 < len(names) && strings.HasPrefix(names[i+1], name+"/") {
			continue
		}
		parts := strings.Split(name, "/")
		unit := strings.Join(parts[:min(len(parts), defaultLevel)], "/")
		units[unit] += observed[name]
	}
	return units
}

func aggregateRuns(tmap map[string][]float64) map[string]float64 {
	smap := make(map[string]float64)

	for name, times := range tmap {
		// Use the median observed duration so one slow run cannot dominate the salt.
		slices.Sort(times)
		middle := len(times) / 2
		if len(times)%2 == 1 {
			smap[name] = times[middle]
		} else {
			smap[name] = (times[middle-1] + times[middle]) / 2
		}
	}

	return smap
}

func optimizeShardingSalt(smap map[string]float64, shards, tries int) (string, float64) {
	var (
		bestSalt   string
		bestMax    float64
		bestSpread float64
	)

	for s := range tries {
		saltStr := fmt.Sprintf("-salt-%d", s)
		totals := shardTotals(smap, shards, saltStr)
		busiest := slices.Max(totals)
		spread := busiest - slices.Min(totals)
		if bestSalt != "" && (busiest > bestMax || busiest == bestMax && spread >= bestSpread) {
			continue
		}
		bestSalt = saltStr
		bestMax = busiest
		bestSpread = spread
	}

	return bestSalt, bestMax
}

func shardTotals(smap map[string]float64, shards int, salt string) []float64 {
	totals := make([]float64, shards)
	for _, testName := range slices.Sorted(maps.Keys(smap)) {
		testTime := smap[testName]
		idx := int(farm.Fingerprint32([]byte(testName+salt))) % shards
		totals[idx] += testTime
	}
	return totals
}

func maxShardTime(smap map[string]float64, shards int, salt string) float64 {
	return slices.Max(shardTotals(smap, shards, salt))
}
