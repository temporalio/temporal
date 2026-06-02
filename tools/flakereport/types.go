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
	MatrixName string    // DB config name from artifact name (e.g. "sqlite", "cassandra")
	Timestamp  time.Time // When the workflow run was created
}

// TestRun represents a test execution (success or failure)
type TestRun struct {
	SuiteName  string // Top-level test suite name
	Name       string // Test name
	Failed     bool   // Whether the test failed
	Skipped    bool   // Whether the test was skipped
	RunID      int64  // Workflow run ID
	JobID      string // GitHub Actions job ID (unique per matrix job/shard)
	MatrixName string // DB config name from artifact name (e.g. "sqlite", "cassandra")
}

// TestReport represents aggregated failures for a single test
type TestReport struct {
	TestName     string    // Normalized test name (retry suffix stripped)
	FailureCount int       // Total number of failures
	TotalRuns    int       // Total number of times this test ran (including successes)
	GitHubURLs   []string  // Up to max_links failure URLs
	LastFailure  time.Time // Timestamp of the most recent failure
}

// SuiteReport represents aggregated flake data for a test suite
type SuiteReport struct {
	SuiteName   string    // Test suite name from JUnit XML
	FlakeRate   float64   // Percentage of job executions with at least one non-retry failure
	FailedRuns  int       // Number of job executions with at least one non-retry failure
	TotalRuns   int       // Total number of job executions where this suite appeared
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
	HeadSHA    string    `json:"head_sha"`
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

// CommitObservation holds aggregated pass/fail data for a single (test, commit) pair.
type CommitObservation struct {
	CommitSHA     string
	CommitIdx     int     // chronological index (0 = oldest)
	Prior         float64 // prior weight (1.0 = uniform; adjusted by heuristics)
	HeuristicNote string  // reason for prior adjustment, if any
	Passes        int
	Fails         int
}

// BisectResult is one candidate culprit commit with its posterior probability.
type BisectResult struct {
	CommitSHA     string
	CommitIdx     int
	Probability   float64 // posterior P(this commit introduced the flakiness)
	PassesBefore  int
	FailsBefore   int
	PassesAfter   int
	FailsAfter    int
	CommitTitle   string
	CommitAuthor  string
	CommitDate    string // formatted date of the commit, e.g. "2024-01-15"
	HeuristicNote string // e.g. "only touches .github/ — deprioritized"
}

// TestBisectReport is the full bisect output for a single test.
type TestBisectReport struct {
	TestName    string
	TopSuspects []BisectResult // sorted by Probability descending
	TotalObs    int            // total observations (pass + fail) used
	Skipped     bool           // true if below signal or confidence threshold
}

// CommitMeta holds changed-file info fetched from the GitHub API.
// GET /repos/{owner}/{repo}/commits/{sha}
type CommitMeta struct {
	SHA         string
	Title       string
	Author      string
	CommittedAt time.Time
	Files       []string // relative paths of changed files
}

// BisectConfig holds configuration for a bisect analysis run.
type BisectConfig struct {
	Repo           string
	TopN           int // max tests to analyze; 0 = all qualifying tests
	MinFailures    int
	MinRuns        int
	MinProbability float64 // only report tests whose top suspect exceeds this (0–1); 0 = report all
}
