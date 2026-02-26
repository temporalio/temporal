package flakereport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeTestName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "test with retry suffix",
			input:    "TestSomething (retry 1)",
			expected: "TestSomething",
		},
		{
			name:     "test with retry suffix and extra spaces",
			input:    "TestSomething  (retry 5)",
			expected: "TestSomething",
		},
		{
			name:     "test without retry suffix",
			input:    "TestSomething",
			expected: "TestSomething",
		},
		{
			name:     "test with retry in name but not suffix",
			input:    "TestRetry",
			expected: "TestRetry",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeTestName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseArtifactName(t *testing.T) {
	tests := []struct {
		name          string
		artifactName  string
		expectedRunID string
		expectedJobID string
	}{
		{
			name:          "valid artifact name",
			artifactName:  "test-results--12345678--87654321--junit",
			expectedRunID: "12345678",
			expectedJobID: "87654321",
		},
		{
			name:          "artifact name with empty job id",
			artifactName:  "test-results--12345678----junit",
			expectedRunID: "12345678",
			expectedJobID: "unknown",
		},
		{
			name:          "invalid artifact name",
			artifactName:  "test-results",
			expectedRunID: "unknown",
			expectedJobID: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runID, jobID := parseArtifactName(tt.artifactName)
			assert.Equal(t, tt.expectedRunID, runID)
			assert.Equal(t, tt.expectedJobID, jobID)
		})
	}
}

func TestBuildGitHubURL(t *testing.T) {
	tests := []struct {
		name        string
		repo        string
		runID       string
		jobID       string
		expectedURL string
	}{
		{
			name:        "with job ID",
			repo:        "temporalio/temporal",
			runID:       "12345678",
			jobID:       "87654321",
			expectedURL: "https://github.com/temporalio/temporal/actions/runs/12345678/job/87654321",
		},
		{
			name:        "without job ID",
			repo:        "temporalio/temporal",
			runID:       "12345678",
			jobID:       "unknown",
			expectedURL: "https://github.com/temporalio/temporal/actions/runs/12345678",
		},
		{
			name:        "with empty job ID",
			repo:        "temporalio/temporal",
			runID:       "12345678",
			jobID:       "",
			expectedURL: "https://github.com/temporalio/temporal/actions/runs/12345678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := buildGitHubURL(tt.repo, tt.runID, tt.jobID)
			assert.Equal(t, tt.expectedURL, url)
		})
	}
}

func TestClassifyFailures(t *testing.T) {
	// Simulate the real flow: raw failures → groupFailuresByTest → classifyFailures.
	// groupFailuresByTest normalizes names, stripping suffixes like (timeout), (retry N).
	failures := []TestFailure{
		{Name: "TestNormal"},
		{Name: "TestNormal"},
		{Name: "TestFlaky"},
		{Name: "TestFlaky"},
		{Name: "TestFlaky"},
		{Name: "TestFlaky"},
		{Name: "TestFlaky"},
		{Name: "TestTimeout (timeout)"},
		{Name: "TestTimeout (timeout)"},
		{Name: "TestTimeout (timeout) (retry 1)"},
		{Name: "TestFoo (crash)"},
		{Name: "TestFoo (crash)"},
	}

	grouped := groupFailuresByTest(failures)
	flaky, timeout, crash := classifyFailures(grouped)

	// TestFlaky should be in flaky
	assert.Contains(t, flaky, "TestFlaky")
	assert.Len(t, flaky["TestFlaky"], 5)

	// TestTimeout should be in timeout (normalized key has no suffix)
	assert.Contains(t, timeout, "TestTimeout")
	assert.Len(t, timeout["TestTimeout"], 3)

	// TestFoo (crash) should be in crash (normalized key has no suffix)
	assert.Contains(t, crash, "TestFoo")
	assert.Len(t, crash["TestFoo"], 2)

	// TestNormal should be in flaky
	assert.Contains(t, flaky, "TestNormal")
	assert.Len(t, flaky["TestNormal"], 2)
}

func TestGenerateSuiteReports(t *testing.T) {
	now := time.Now()
	twoDaysAgo := now.Add(-48 * time.Hour)
	oneDayAgo := now.Add(-24 * time.Hour)

	failures := []TestFailure{
		// SuiteA: original failure in run 1
		{Name: "TestFoo", SuiteName: "TestFunctionalSuiteA", RunID: 1, Timestamp: twoDaysAgo},
		// SuiteA: retry failure in run 1 (should be excluded from suite flake rate)
		{Name: "TestFoo (retry 1)", SuiteName: "TestFunctionalSuiteA", RunID: 1, Timestamp: twoDaysAgo},
		// SuiteA: original failure in run 2
		{Name: "TestBar", SuiteName: "TestFunctionalSuiteA", RunID: 2, Timestamp: oneDayAgo},
		// SuiteB: original failure in run 1
		{Name: "TestBaz", SuiteName: "TestFunctionalSuiteB", RunID: 1, Timestamp: twoDaysAgo},
	}

	allRuns := []TestRun{
		// SuiteA present in runs 1, 2, 3
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 1},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo (retry 1)", RunID: 1},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestBar", RunID: 2},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 3},
		// SuiteB present in runs 1, 2
		{SuiteName: "TestFunctionalSuiteB", Name: "TestBaz", RunID: 1},
		{SuiteName: "TestFunctionalSuiteB", Name: "TestBaz", RunID: 2},
		// Skipped test should not count
		{SuiteName: "TestFunctionalSuiteC", Name: "TestSkipped", RunID: 1, Skipped: true},
		// Non-suite names should be filtered out
		{SuiteName: "DATA RACE", Name: "DATA RACE: detected", RunID: 1},
		{SuiteName: "TestStandalone", Name: "TestStandalone", RunID: 1},
		// Suite with no failures should be excluded
		{SuiteName: "TestHealthySuite", Name: "TestOk", RunID: 1},
		{SuiteName: "TestHealthySuite", Name: "TestOk", RunID: 2},
	}

	reports := generateSuiteReports(failures, allRuns)

	// Should have SuiteA and SuiteB (SuiteC is all skipped)
	require.Len(t, reports, 2)

	// Sorted by suite name
	require.Equal(t, "TestFunctionalSuiteA", reports[0].SuiteName)
	require.Equal(t, "TestFunctionalSuiteB", reports[1].SuiteName)

	// SuiteA: 2 failed runs (run 1 and run 2) out of 3 total runs
	require.Equal(t, 2, reports[0].FailedRuns)
	require.Equal(t, 3, reports[0].TotalRuns)
	require.InDelta(t, 66.7, reports[0].FlakeRate, 0.1)
	require.Equal(t, oneDayAgo, reports[0].LastFailure)

	// SuiteB: 1 failed run (run 1) out of 2 total runs
	require.Equal(t, 1, reports[1].FailedRuns)
	require.Equal(t, 2, reports[1].TotalRuns)
	require.InDelta(t, 50.0, reports[1].FlakeRate, 0.1)
	require.Equal(t, twoDaysAgo, reports[1].LastFailure)
}

func TestNormalizeTestNameFinal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "retry with final suffix",
			input:    "TestFoo (retry 2) (final)",
			expected: "TestFoo",
		},
		{
			name:     "no suffix",
			input:    "TestFoo",
			expected: "TestFoo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, normalizeTestName(tt.input))
		})
	}
}

func TestIsFinalRetry(t *testing.T) {
	require.True(t, isFinalRetry("TestFoo (retry 2) (final)"))
	require.True(t, isFinalRetry("TestFoo/SubTest (retry 1) (final)"))
	require.False(t, isFinalRetry("TestFoo"))
	require.False(t, isFinalRetry("TestFoo (retry 1)"))
	require.False(t, isFinalRetry("TestFinal"))
}
