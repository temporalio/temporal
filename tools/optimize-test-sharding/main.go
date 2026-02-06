package optimizetestsharding

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/dgryski/go-farm"
	"github.com/jstemmer/go-junit-report/v2/junit"
)

const (
	defaultLevel = 2 // 1 means shard by suite, 2 means shard by test
)

var testNameRe = regexp.MustCompile(`^(.*?)\s*\(.*\)$`)

func Main() (string, error) {
	shards := flag.Int("shards", 0, "Number of shards (required)")
	tries := flag.Int("tries", 10000, "Number of tries")
	workflow := flag.String("workflow", "", "GitHub Actions workflow name to fetch artifacts from (uses gh CLI)")
	artifactPattern := flag.String("artifact-pattern", "", "Artifact name pattern for gh run download")
	runs := flag.Int("runs", 5, "Number of recent successful runs to download")
	branch := flag.String("branch", "main", "Branch to find successful runs on")
	event := flag.String("event", "push", "Event type to filter runs by")
	flag.Usage = func() {
		log.Printf("Usage: %s [options]", os.Args[0])
		log.Print("Optimizes the salt selection for test sharding.")
		log.Print("Uses -workflow and -artifact-pattern to download JUnit XML files")
		log.Print("from recent successful GitHub Actions runs via gh CLI.")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *shards < 1 {
		return "", errors.New("-shards is required and must be >= 1")
	}
	if *workflow == "" || *artifactPattern == "" {
		return "", errors.New("-workflow and -artifact-pattern are required")
	}

	dir, err := downloadArtifacts(*workflow, *artifactPattern, *branch, *event, *runs)
	if err != nil {
		return "", fmt.Errorf("downloading artifacts: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	tmap, err := loadTestData(dir)
	if err != nil {
		return "", fmt.Errorf("loading test data: %w", err)
	}

	log.Printf("Loaded %d unique test names", len(tmap))

	smap := aggregateByLevel(tmap)
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

	return bestSalt, nil
}

// downloadArtifacts uses gh CLI to find the latest successful runs and download
// matching artifacts. Returns the path to a temp directory containing the files.
func downloadArtifacts(workflow, artifactPattern, branch, event string, limit int) (string, error) {
	runIDs, err := findLatestRuns(workflow, branch, event, limit)
	if err != nil {
		return "", err
	}

	dir, err := os.MkdirTemp("", "test-sharding-*")
	if err != nil {
		return "", err
	}

	for _, runID := range runIDs {
		log.Printf("Downloading artifacts from run %s", runID)
		cmd := exec.Command("gh", "run", "download", runID,
			"--pattern", artifactPattern,
			"--dir", filepath.Join(dir, runID))
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			_ = os.RemoveAll(dir)
			return "", fmt.Errorf("gh run download %s: %w", runID, err)
		}
	}

	return dir, nil
}

type ghRun struct {
	DatabaseID int `json:"databaseId"`
}

func findLatestRuns(workflow, branch, event string, limit int) ([]string, error) {
	cmd := exec.Command("gh", "run", "list",
		"--workflow="+workflow,
		"--event="+event,
		"--branch="+branch,
		"--status=success",
		"--limit="+strconv.Itoa(limit),
		"--json=databaseId")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("gh run list: %w", err)
	}

	var runs []ghRun
	if err := json.Unmarshal(out, &runs); err != nil {
		return nil, fmt.Errorf("parsing gh output: %w", err)
	}
	if len(runs) == 0 {
		return nil, fmt.Errorf("no successful runs found for workflow %s on %s/%s", workflow, branch, event)
	}

	ids := make([]string, len(runs))
	for i, r := range runs {
		ids[i] = strconv.Itoa(r.DatabaseID)
	}
	return ids, nil
}

// loadTestData returns a map of test names to durations in seconds.
func loadTestData(dir string) (map[string][]float64, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".xml") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("no XML files found")
	}
	log.Printf("Found %d XML file(s)", len(files))

	tmap := make(map[string][]float64)
	for _, filename := range files {
		if err := processJUnitReport(filename, tmap); err != nil {
			return nil, fmt.Errorf("processing %s: %w", filename, err)
		}
	}
	return tmap, nil
}

func processJUnitReport(filename string, tmap map[string][]float64) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	var testsuites junit.Testsuites
	if err := xml.NewDecoder(file).Decode(&testsuites); err != nil {
		return err
	}

	for _, suite := range testsuites.Suites {
		for _, tc := range suite.Testcases {
			duration, err := strconv.ParseFloat(tc.Time, 64)
			if err != nil {
				continue
			}
			name := tc.Name
			if m := testNameRe.FindStringSubmatch(name); m != nil {
				name = m[1]
			}
			tmap[name] = append(tmap[name], duration)
		}
	}

	return nil
}

func aggregateByLevel(tmap map[string][]float64) map[string]float64 {
	smap := make(map[string]float64)

	for name, times := range tmap {
		// Only include entries at the target depth (e.g. "Suite/Test" at level 2).
		if strings.Count(name, "/")+1 != defaultLevel {
			continue
		}

		// Sum all observed durations across runs.
		var total float64
		for _, t := range times {
			total += t
		}
		smap[name] = total
	}

	return smap
}

func optimizeShardingSalt(smap map[string]float64, shards, tries int) (string, float64) {
	bestSalt := ""
	bestMax := math.MaxFloat64

	for s := range tries {
		saltStr := fmt.Sprintf("-salt-%d", s)
		m := maxShardTime(smap, shards, saltStr)
		if m < bestMax {
			bestSalt = saltStr
			bestMax = m
		}
	}

	return bestSalt, bestMax
}

func shardTotals(smap map[string]float64, shards int, salt string) []float64 {
	totals := make([]float64, shards)
	for testName, testTime := range smap {
		idx := int(farm.Fingerprint32([]byte(testName+salt))) % shards
		totals[idx] += testTime
	}
	return totals
}

func maxShardTime(smap map[string]float64, shards int, salt string) float64 {
	return slices.Max(shardTotals(smap, shards, salt))
}
