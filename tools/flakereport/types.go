package flakereport

import "time"

// TestFailure represents a single test failure extracted from JUnit XML
type TestFailure struct {
	ClassName  string    // Test class/module name
	Name       string    // Test function name
	SuiteName  string    // Top-level test suite name
	ArtifactID string    // Artifact identifier from GitHub
	RunID      int64     // GitHub Actions run ID
	JobID      string    // GitHub Actions job ID (or "unknown")
	Timestamp  time.Time // When the workflow run was created
}

// TestRun represents a test execution (success or failure)
type TestRun struct {
	SuiteName string // Top-level test suite name
	Name      string // Test name
	Failed    bool   // Whether the test failed
	Skipped   bool   // Whether the test was skipped
	RunID     int64  // Workflow run ID
}

// TestReport represents aggregated failures for a single test
type TestReport struct {
	TestName     string    // Normalized test name (retry suffix stripped)
	FailureCount int       // Total number of failures
	TotalRuns    int       // Total number of times this test ran (including successes)
	CIRunsBroken int       // Number of CI runs this test broke (for CI breakers only)
	GitHubURLs   []string  // Up to max_links failure URLs
	LastFailure  time.Time // Timestamp of the most recent failure
}

// SuiteReport represents aggregated flake data for a test suite
type SuiteReport struct {
	SuiteName   string    // Test suite name from JUnit XML
	FlakeRate   float64   // Percentage of runs with at least one non-retry failure
	FailedRuns  int       // Number of runs with at least one non-retry failure
	TotalRuns   int       // Total number of workflow runs where this suite appeared
	LastFailure time.Time // Timestamp of the most recent failure
}

// ReportSummary contains all processed report data
type ReportSummary struct {
	FlakyTests         []TestReport
	Timeouts           []TestReport  // Tests ending with "(timeout)"
	Crashes            []TestReport  // Tests containing "crash"
	CIBreakers         []TestReport  // Tests that failed all retries (3x) in a single job
	Suites             []SuiteReport // Per-suite flake breakdown
	TotalFailures      int           // Total raw failure count
	TotalTestRuns      int           // Total test executions (all tests, all runs)
	OverallFailureRate float64       // Overall failures per 1000 test runs
	TotalFlakyCount    int           // Total flaky tests (not just top 10)
	TotalWorkflowRuns  int           // Total workflow runs analyzed
	SuccessfulRuns     int           // Workflow runs that succeeded
}

// FailedTestRecord represents a single test failure for the failures.json analytics export
type FailedTestRecord struct {
	SuiteName   string `json:"suite_name"`
	TestName    string `json:"test_name"`
	FailureDate string `json:"failure_date"`
	Link        string `json:"link"`
	FailureType string `json:"failure_type"`
}

// WorkflowRun represents a GitHub Actions workflow run
type WorkflowRun struct {
	ID         int64     `json:"id"`
	Number     int       `json:"run_number"`
	CreatedAt  time.Time `json:"created_at"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion"`
	HeadBranch string    `json:"head_branch"`
}

// WorkflowArtifact represents a downloadable artifact
type WorkflowArtifact struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	Expired   bool      `json:"expired"`
}

// ArtifactsResponse represents the GitHub API response for artifacts
type ArtifactsResponse struct {
	TotalCount int                `json:"total_count"`
	Artifacts  []WorkflowArtifact `json:"artifacts"`
}
