package optimizetestsharding

import (
	"archive/zip"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
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

func Run() {
	shards := flag.Int("shards", 0, "Number of shards (required)")
	tries := flag.Int("tries", 10000, "Number of tries")
	output := flag.String("output", "", "Write salt to this file instead of stdout")
	workflow := flag.String("workflow", "", "GitHub Actions workflow name to fetch artifacts from (uses gh CLI)")
	artifactPattern := flag.String("artifact-pattern", "", "Artifact name pattern for gh run download")
	runs := flag.Int("runs", 5, "Number of recent successful runs to download")
	threshold := flag.Float64("threshold", 0.05, "Only update salt if improvement exceeds this fraction (e.g. 0.05 = 5%)")
	branch := flag.String("branch", "main", "Branch to find successful runs on")
	event := flag.String("event", "push", "Event type to filter runs by")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: "+os.Args[0]+" [options] [FILES...]\n\n")
		fmt.Fprint(os.Stderr, "Optimizes the salt selection for test sharding.\n\n")
		fmt.Fprint(os.Stderr, "Provide JUnit XML files as arguments, or use -workflow and -artifact-pattern\n")
		fmt.Fprint(os.Stderr, "to download them from the latest successful GitHub Actions run via gh CLI.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *shards < 1 {
		fmt.Fprint(os.Stderr, "Error: -shards is required and must be >= 1\n")
		os.Exit(1) //nolint:revive // deep-exit: this is the entrypoint
	}

	var paths []string
	if flag.NArg() > 0 {
		paths = flag.Args()
	} else if *workflow != "" && *artifactPattern != "" {
		dir, err := downloadArtifacts(*workflow, *artifactPattern, *branch, *event, *runs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error downloading artifacts: %v\n", err)
			os.Exit(1) //nolint:revive // deep-exit: this is the entrypoint
		}
		defer func() { _ = os.RemoveAll(dir) }()
		paths = []string{dir}
	} else {
		flag.Usage()
		os.Exit(1) //nolint:revive // deep-exit: this is the entrypoint
	}

	tmap, err := loadTestData(paths)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading test data: %v\n", err)
		os.Exit(1) //nolint:revive // deep-exit: this is the entrypoint
	}

	fmt.Fprintf(os.Stderr, "Loaded %d unique test names\n", len(tmap))

	smap := aggregateByLevel(tmap)
	fmt.Fprintf(os.Stderr, "Aggregated to %d entries for sharding\n", len(smap))

	var totalTime float64
	for _, t := range smap {
		totalTime += t
	}
	fmt.Fprintf(os.Stderr, "Total test time: %.1fs\n", totalTime)

	bestSalt, bestMax := optimizeShardingSalt(smap, *shards, *tries)
	fmt.Fprintf(os.Stderr, "Best salt: %s (max shard: %.1fs, ideal: %.1fs)\n",
		bestSalt, bestMax, totalTime/float64(*shards))

	// Log per-shard breakdown for the winning salt.
	totals := shardTotals(smap, *shards, bestSalt)
	for i, t := range totals {
		fmt.Fprintf(os.Stderr, "  shard %d: %.1fs\n", i, t)
	}

	if *output != "" {
		// Read current salt and check if the improvement is meaningful.
		if current, err := os.ReadFile(*output); err == nil {
			currentSalt := strings.TrimSpace(string(current))
			if currentSalt != "" {
				currentMax := maxShardTime(smap, *shards, currentSalt)
				improvement := (currentMax - bestMax) / currentMax
				fmt.Fprintf(os.Stderr, "Current salt: %s (max shard: %.1fs)\n", currentSalt, currentMax)
				fmt.Fprintf(os.Stderr, "Improvement: %.2f%%\n", improvement*100)
				if improvement < *threshold {
					fmt.Fprintf(os.Stderr, "Improvement below threshold (%.0f%%), keeping current salt\n", *threshold*100)
					return
				}
			}
		}
		if err := os.WriteFile(*output, []byte(bestSalt), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing output file: %v\n", err)
			os.Exit(1) //nolint:revive // deep-exit: this is the entrypoint
		}
		fmt.Fprintf(os.Stderr, "Wrote %s to %s\n", bestSalt, *output)
	} else {
		fmt.Println(bestSalt)
	}
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
		fmt.Fprintf(os.Stderr, "Downloading artifacts from run %s\n", runID)
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

func loadTestData(paths []string) (map[string][]float64, error) {
	files, err := resolveFiles(paths)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("no files found")
	}
	fmt.Fprintf(os.Stderr, "Found %d XML file(s)\n", len(files))

	tmap := make(map[string][]float64)
	for _, filename := range files {
		if strings.HasSuffix(filename, ".zip") {
			if err := processZip(filename, tmap); err != nil {
				return nil, fmt.Errorf("processing zip file %s: %w", filename, err)
			}
		} else if strings.HasSuffix(filename, ".xml") {
			if err := processXML(filename, tmap); err != nil {
				return nil, fmt.Errorf("processing xml file %s: %w", filename, err)
			}
		} else {
			return nil, fmt.Errorf("unknown file type: %s", filename)
		}
	}

	return tmap, nil
}

// resolveFiles expands glob patterns and walks directories to find XML/ZIP files.
func resolveFiles(paths []string) ([]string, error) {
	var globbed []string
	for _, pattern := range paths {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid glob pattern %s: %w", pattern, err)
		}
		globbed = append(globbed, matches...)
	}

	var files []string
	for _, f := range globbed {
		info, err := os.Stat(f)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			if err := filepath.Walk(f, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && strings.HasSuffix(path, ".xml") {
					files = append(files, path)
				}
				return nil
			}); err != nil {
				return nil, err
			}
		} else {
			files = append(files, f)
		}
	}
	return files, nil
}

func processZip(filename string, tmap map[string][]float64) error {
	r, err := zip.OpenReader(filename)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	for _, f := range r.File {
		if !strings.HasSuffix(f.Name, ".xml") {
			continue
		}
		if err := processZipEntry(f, tmap); err != nil {
			return err
		}
	}

	return nil
}

func processZipEntry(f *zip.File, tmap map[string][]float64) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return processXMLReader(rc, tmap)
}

func processXML(filename string, tmap map[string][]float64) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return processXMLReader(file, tmap)
}

func processXMLReader(reader io.Reader, tmap map[string][]float64) error {
	var testsuites junit.Testsuites
	if err := xml.NewDecoder(reader).Decode(&testsuites); err != nil {
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
