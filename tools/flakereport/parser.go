package flakereport

import (
	"encoding/xml"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
)

var finalRegex = regexp.MustCompile(`\s*\(final\)$`)
var trailingSuffixRegex = regexp.MustCompile(`\s*\([^)]+\)$`)

// parseJUnitFile reads and parses a single JUnit XML file
func parseJUnitFile(filePath string) (*junit.Testsuites, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("Warning: Failed to close file %s: %v\n", filePath, err)
		}
	}()

	var testsuites junit.Testsuites
	decoder := xml.NewDecoder(file)
	if err := decoder.Decode(&testsuites); err != nil {
		// Try parsing as a single testsuite
		if _, seekErr := file.Seek(0, 0); seekErr != nil {
			return nil, fmt.Errorf("failed to seek file %s: %w", filePath, seekErr)
		}
		var testsuite junit.Testsuite
		decoder = xml.NewDecoder(file)
		if err := decoder.Decode(&testsuite); err != nil {
			return nil, fmt.Errorf("failed to parse JUnit XML %s: %w", filePath, err)
		}
		testsuites.Suites = []junit.Testsuite{testsuite}
	}

	return &testsuites, nil
}

// topLevelTestName extracts the suite/top-level test name from a test name.
// For "TestSuiteV0/TestMethod" returns "TestSuiteV0".
// For "TestFoo" (no slash) returns "TestFoo".
func topLevelTestName(name string) string {
	if idx := strings.IndexByte(name, '/'); idx >= 0 {
		return name[:idx]
	}
	return name
}

// isGoTestSuite returns true if the name looks like a Go testify suite name
// (starts with "Test" and contains "Suite").
// e.g. TestDeploymentVersionSuiteV0, TestFunctionalSuite
func isGoTestSuite(name string) bool {
	return strings.HasPrefix(name, "Test") && strings.Contains(name, "Suite")
}

// extractFailures extracts all test failures from parsed JUnit data
// Filters for: passed = false AND skipped = false (matching tringa SQL query)
func extractFailures(suites *junit.Testsuites, artifactName string, runID int64, timestamp time.Time) []TestFailure {
	var failures []TestFailure

	// Parse artifact name for run_id and job_id
	_, jobID := parseArtifactName(artifactName)

	for _, suite := range suites.Suites {
		for _, testcase := range suite.Testcases {
			// Filter: failure present AND not skipped
			if testcase.Failure != nil && testcase.Skipped == nil {
				failure := TestFailure{
					ClassName:  testcase.Classname,
					Name:       testcase.Name,
					SuiteName:  topLevelTestName(testcase.Name),
					ArtifactID: artifactName,
					RunID:      runID,
					JobID:      jobID,
					Timestamp:  timestamp,
				}
				failures = append(failures, failure)
			}
		}
	}

	return failures
}

// extractAllTestRuns extracts all test runs (including successes) from parsed JUnit data
// Used for calculating failure rates
func extractAllTestRuns(suites *junit.Testsuites, runID int64) []TestRun {
	var runs []TestRun

	for _, suite := range suites.Suites {
		for _, testcase := range suite.Testcases {
			run := TestRun{
				SuiteName: topLevelTestName(testcase.Name),
				Name:      testcase.Name,
				Failed:    testcase.Failure != nil,
				Skipped:   testcase.Skipped != nil,
				RunID:     runID,
			}
			runs = append(runs, run)
		}
	}

	return runs
}

// normalizeTestName strips all trailing parenthesized suffixes from test names,
// e.g. "(retry 1)", "(final)", "(timeout)".
func normalizeTestName(name string) string {
	for {
		stripped := trailingSuffixRegex.ReplaceAllString(name, "")
		if stripped == name {
			return name
		}
		name = stripped
	}
}

// groupFailuresByTest groups failures by normalized test name
func groupFailuresByTest(failures []TestFailure) map[string][]TestFailure {
	grouped := make(map[string][]TestFailure)

	for _, failure := range failures {
		normalizedName := normalizeTestName(failure.Name)
		grouped[normalizedName] = append(grouped[normalizedName], failure)
	}

	return grouped
}

// countTestRuns counts total runs (including successes) by normalized test name
func countTestRuns(allRuns []TestRun) map[string]int {
	counts := make(map[string]int)

	for _, run := range allRuns {
		// Only count non-skipped tests
		if !run.Skipped {
			normalizedName := normalizeTestName(run.Name)
			counts[normalizedName]++
		}
	}

	return counts
}

// classifyFailure returns "timeout", "crash", or "flaky" based on the test name.
// Uses Contains (not HasSuffix) so it works on both raw and normalized names.
func classifyFailure(name string) string {
	lower := strings.ToLower(name)
	if strings.Contains(lower, "(timeout)") {
		return "timeout"
	}
	if strings.Contains(lower, "(crash)") {
		return "crash"
	}
	return "flaky"
}

// classifyFailures separates failures into categories.
// Classifies using the raw failure name (not the normalized key) since
// normalizeTestName strips suffixes like "(timeout)".
func classifyFailures(grouped map[string][]TestFailure) (flaky, timeout, crash map[string][]TestFailure) {
	flaky = make(map[string][]TestFailure)
	timeout = make(map[string][]TestFailure)
	crash = make(map[string][]TestFailure)

	for testName, failures := range grouped {
		switch classifyFailure(failures[0].Name) {
		case "timeout":
			timeout[testName] = failures
		case "crash":
			crash[testName] = failures
		case "flaky":
			flaky[testName] = failures
		default:
			panic("unknown failure classification: " + classifyFailure(failures[0].Name)) //nolint:forbidigo
		}
	}

	return flaky, timeout, crash
}

// convertToReports converts grouped failures to TestReport slice
// testRunCounts maps test name to total number of runs (including successes)
func convertToReports(grouped map[string][]TestFailure, testRunCounts map[string]int, repo string, maxLinks int) []TestReport {
	var reports []TestReport

	for testName, failures := range grouped {
		totalRuns := testRunCounts[testName]
		if totalRuns == 0 {
			totalRuns = len(failures) // Fallback if we don't have run counts
		}

		// Find most recent failure
		var lastFailure time.Time
		for _, f := range failures {
			if f.Timestamp.After(lastFailure) {
				lastFailure = f.Timestamp
			}
		}

		report := TestReport{
			TestName:     testName,
			FailureCount: len(failures),
			TotalRuns:    totalRuns,
			LastFailure:  lastFailure,
			GitHubURLs:   make([]string, 0, maxLinks),
		}

		// Add up to maxLinks URLs
		for i := 0; i < len(failures) && i < maxLinks; i++ {
			failure := failures[i]
			runIDStr := strconv.FormatInt(failure.RunID, 10)
			url := buildGitHubURL(repo, runIDStr, failure.JobID)
			report.GitHubURLs = append(report.GitHubURLs, url)
		}

		reports = append(reports, report)
	}

	// Sort by failure rate descending (most problematic tests first)
	sort.Slice(reports, func(i, j int) bool {
		ri := float64(reports[i].FailureCount) / float64(reports[i].TotalRuns)
		rj := float64(reports[j].FailureCount) / float64(reports[j].TotalRuns)
		return ri > rj
	})

	return reports
}

// isFinalRetry returns true if the test name has the "(final)" suffix,
// indicating the test runner exhausted all retries.
func isFinalRetry(testName string) bool {
	return finalRegex.MatchString(testName)
}

// analyzeArtifactForCIBreakers analyzes a single artifact for CI breakers.
// A test is a CI breaker if it has a failure with the "(final)" suffix,
// meaning the test runner exhausted all retries.
func analyzeArtifactForCIBreakers(artifactID string, artifactFailures []TestFailure) map[string][]TestFailure {
	ciBreakers := make(map[string][]TestFailure)

	for _, failure := range artifactFailures {
		if !isFinalRetry(failure.Name) {
			continue
		}
		normalized := normalizeTestName(failure.Name)
		ciBreakers[normalized] = append(ciBreakers[normalized], failure)
	}

	for testName := range ciBreakers {
		fmt.Printf("  CI BREAKER: %s (artifact %s)\n", testName, artifactID)
	}

	return ciBreakers
}

// convertCIBreakersToReports converts CI breaker failures to TestReport slice
// Includes the count of how many CI runs each test broke
func convertCIBreakersToReports(grouped map[string][]TestFailure, ciBreakCounts map[string]int, repo string, maxLinks int) []TestReport {
	var reports []TestReport

	for testName, failures := range grouped {
		var lastFailure time.Time
		for _, f := range failures {
			if f.Timestamp.After(lastFailure) {
				lastFailure = f.Timestamp
			}
		}

		report := TestReport{
			TestName:     testName,
			FailureCount: len(failures),
			CIRunsBroken: ciBreakCounts[testName],
			LastFailure:  lastFailure,
			GitHubURLs:   make([]string, 0, maxLinks),
		}

		// Add up to maxLinks URLs
		for i := 0; i < len(failures) && i < maxLinks; i++ {
			failure := failures[i]
			runIDStr := strconv.FormatInt(failure.RunID, 10)
			url := buildGitHubURL(repo, runIDStr, failure.JobID)
			report.GitHubURLs = append(report.GitHubURLs, url)
		}

		reports = append(reports, report)
	}

	// Sort by number of CI runs broken descending (most problematic tests first)
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].CIRunsBroken != reports[j].CIRunsBroken {
			return reports[i].CIRunsBroken > reports[j].CIRunsBroken
		}
		return reports[i].FailureCount > reports[j].FailureCount
	})

	return reports
}

// identifyCIBreakers finds tests that failed their final retry in a CI job.
// A test breaks CI if it has a failure with the "(final)" suffix in an artifact.
// Returns: ciBreakers map and count of how many artifacts each test broke.
func identifyCIBreakers(failures []TestFailure) (map[string][]TestFailure, map[string]int) {
	// Group failures by artifact ID first
	byArtifact := make(map[string][]TestFailure)
	for _, failure := range failures {
		byArtifact[failure.ArtifactID] = append(byArtifact[failure.ArtifactID], failure)
	}

	fmt.Println("\n=== CI Breaker Analysis ===")
	fmt.Printf("Total failures to analyze: %d\n", len(failures))
	fmt.Printf("Grouped into %d artifacts\n", len(byArtifact))

	// Track tests that broke CI
	ciBreakers := make(map[string][]TestFailure)
	ciBreakCount := make(map[string]int)

	// Analyze each artifact for CI breakers
	for artifactID, artifactFailures := range byArtifact {
		breakersInArtifact := analyzeArtifactForCIBreakers(artifactID, artifactFailures)

		// Aggregate results
		for testName, failures := range breakersInArtifact {
			ciBreakers[testName] = append(ciBreakers[testName], failures...)
			ciBreakCount[testName]++
		}
	}

	fmt.Printf("Unique tests that broke CI: %d\n", len(ciBreakers))

	return ciBreakers, ciBreakCount
}

// generateSuiteReports creates per-suite flake breakdown from all failures and test runs.
// Suite flake rate = % of workflow runs where the suite had at least one non-retry failure.
func generateSuiteReports(allFailures []TestFailure, allTestRuns []TestRun) []SuiteReport {
	// Track unique workflow runs per suite (denominator)
	suiteRuns := make(map[string]map[int64]bool)
	for _, run := range allTestRuns {
		if run.Skipped || !isGoTestSuite(run.SuiteName) {
			continue
		}
		if suiteRuns[run.SuiteName] == nil {
			suiteRuns[run.SuiteName] = make(map[int64]bool)
		}
		suiteRuns[run.SuiteName][run.RunID] = true
	}

	// Track workflow runs with non-retry failures per suite (numerator)
	suiteFailedRuns := make(map[string]map[int64]bool)
	suiteLastFailure := make(map[string]time.Time)
	for _, failure := range allFailures {
		if !isGoTestSuite(failure.SuiteName) {
			continue
		}
		// Only report the original, complete run
		if normalizeTestName(failure.Name) != failure.Name {
			continue
		}
		if suiteFailedRuns[failure.SuiteName] == nil {
			suiteFailedRuns[failure.SuiteName] = make(map[int64]bool)
		}
		suiteFailedRuns[failure.SuiteName][failure.RunID] = true
		if failure.Timestamp.After(suiteLastFailure[failure.SuiteName]) {
			suiteLastFailure[failure.SuiteName] = failure.Timestamp
		}
	}

	var reports []SuiteReport
	for suiteName, runIDs := range suiteRuns {
		failedRuns := len(suiteFailedRuns[suiteName])
		if failedRuns == 0 {
			continue
		}
		totalRuns := len(runIDs)
		flakeRate := float64(failedRuns) / float64(totalRuns) * 100.0
		reports = append(reports, SuiteReport{
			SuiteName:   suiteName,
			FlakeRate:   flakeRate,
			FailedRuns:  failedRuns,
			TotalRuns:   totalRuns,
			LastFailure: suiteLastFailure[suiteName],
		})
	}

	sort.Slice(reports, func(i, j int) bool {
		return reports[i].SuiteName < reports[j].SuiteName
	})

	return reports
}
