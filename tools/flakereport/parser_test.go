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
		name               string
		artifactName       string
		expectedRunID      string
		expectedJobID      string
		expectedMatrixName string
	}{
		{
			name:               "functional test artifact",
			artifactName:       "junit-xml--22373551837--64609560060--1--integration-0--Integration--functional-test",
			expectedRunID:      "22373551837",
			expectedJobID:      "64609560060",
			expectedMatrixName: "integration-0",
		},
		{
			name:               "unit test artifact",
			artifactName:       "junit-xml--22373551837--64609560061--1--unit-test",
			expectedRunID:      "22373551837",
			expectedJobID:      "64609560061",
			expectedMatrixName: "unknown",
		},
		{
			name:               "integration test artifact",
			artifactName:       "junit-xml--22373551837--64609560062--1--integration-test",
			expectedRunID:      "22373551837",
			expectedJobID:      "64609560062",
			expectedMatrixName: "unknown",
		},
		{
			name:               "artifact name with empty job id",
			artifactName:       "junit-xml--22373551837----1--unit-test",
			expectedRunID:      "22373551837",
			expectedJobID:      "unknown",
			expectedMatrixName: "unknown",
		},
		{
			name:               "invalid artifact name",
			artifactName:       "test-results",
			expectedRunID:      "unknown",
			expectedJobID:      "unknown",
			expectedMatrixName: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runID, jobID, matrixName := parseArtifactName(tt.artifactName)
			assert.Equal(t, tt.expectedRunID, runID)
			assert.Equal(t, tt.expectedJobID, jobID)
			assert.Equal(t, tt.expectedMatrixName, matrixName)
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

func TestFilterParentTests(t *testing.T) {
	makeFailures := func(names ...string) map[string][]TestFailure {
		m := make(map[string][]TestFailure, len(names))
		for _, n := range names {
			m[n] = []TestFailure{{Name: n}}
		}
		return m
	}

	t.Run("removes parent when subtests observed", func(t *testing.T) {
		grouped := makeFailures("TestFooSuite", "TestFooSuite/TestBar")
		counts := map[string]int{
			"TestFooSuite/TestBar": 10,
			"TestFooSuite/TestBaz": 20,
		}
		filterParentTests(grouped, counts)
		require.NotContains(t, grouped, "TestFooSuite")
		require.Contains(t, grouped, "TestFooSuite/TestBar")
	})

	t.Run("keeps parent when no subtests observed", func(t *testing.T) {
		grouped := makeFailures("TestStandalone")
		counts := map[string]int{"TestStandalone": 5}
		filterParentTests(grouped, counts)
		require.Contains(t, grouped, "TestStandalone")
	})

	t.Run("keeps subtest entry regardless", func(t *testing.T) {
		grouped := makeFailures("TestFooSuite/TestBar")
		counts := map[string]int{"TestFooSuite/TestBar": 10}
		filterParentTests(grouped, counts)
		require.Contains(t, grouped, "TestFooSuite/TestBar")
	})
}

func TestGenerateSuiteReports(t *testing.T) {
	now := time.Now()
	twoDaysAgo := now.Add(-48 * time.Hour)
	oneDayAgo := now.Add(-24 * time.Hour)

	// Each CI run has 2 DB configs ("db-a" and "db-b"), and within each config
	// the suite is spread across 2 shards (distinct JobIDs). The denominator
	// should count unique (RunID × MatrixName) pairs, not raw JobIDs, so shards
	// within the same run+config collapse into one entry.
	failures := []TestFailure{
		// SuiteA: failure in run 1, db-a (shard job 101)
		{Name: "TestFoo", SuiteName: "TestFunctionalSuiteA", RunID: 1, JobID: "101", MatrixName: "db-a", Timestamp: twoDaysAgo},
		// SuiteA: retry failure — should be excluded from suite flake rate
		{Name: "TestFoo (retry 1)", SuiteName: "TestFunctionalSuiteA", RunID: 1, JobID: "101", MatrixName: "db-a", Timestamp: twoDaysAgo},
		// SuiteA: failure in run 2, db-b (shard job 204)
		{Name: "TestBar", SuiteName: "TestFunctionalSuiteA", RunID: 2, JobID: "204", MatrixName: "db-b", Timestamp: oneDayAgo},
		// SuiteB: failure in run 1, db-a
		{Name: "TestBaz", SuiteName: "TestFunctionalSuiteB", RunID: 1, JobID: "101", MatrixName: "db-a", Timestamp: twoDaysAgo},
	}

	allRuns := []TestRun{
		// SuiteA: runs 1–3, each with db-a (shards 101,102) and db-b (shards 103,104 / 203,204 / 303,304)
		// → 3 runs × 2 DB configs = 6 unique (run, config) pairs
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 1, JobID: "101", MatrixName: "db-a"},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 1, JobID: "102", MatrixName: "db-a"}, // same (run=1, db-a) pair
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo (retry 1)", RunID: 1, JobID: "101", MatrixName: "db-a"},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 1, JobID: "103", MatrixName: "db-b"},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 1, JobID: "104", MatrixName: "db-b"}, // same (run=1, db-b) pair
		{SuiteName: "TestFunctionalSuiteA", Name: "TestBar", RunID: 2, JobID: "203", MatrixName: "db-a"},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestBar", RunID: 2, JobID: "204", MatrixName: "db-b"},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 3, JobID: "301", MatrixName: "db-a"},
		{SuiteName: "TestFunctionalSuiteA", Name: "TestFoo", RunID: 3, JobID: "303", MatrixName: "db-b"},
		// SuiteB present in runs 1, 2, single DB config each
		{SuiteName: "TestFunctionalSuiteB", Name: "TestBaz", RunID: 1, JobID: "101", MatrixName: "db-a"},
		{SuiteName: "TestFunctionalSuiteB", Name: "TestBaz", RunID: 2, JobID: "203", MatrixName: "db-a"},
		// Skipped test should not count
		{SuiteName: "TestFunctionalSuiteC", Name: "TestSkipped", RunID: 1, Skipped: true},
		// Non-suite names should be filtered out
		{SuiteName: "DATA RACE", Name: "DATA RACE: detected", RunID: 1},
		{SuiteName: "TestStandalone", Name: "TestStandalone", RunID: 1},
		// Suite with no failures should be excluded
		{SuiteName: "TestHealthySuite", Name: "TestOk", RunID: 1, MatrixName: "db-a"},
		{SuiteName: "TestHealthySuite", Name: "TestOk", RunID: 2, MatrixName: "db-a"},
	}

	reports := generateSuiteReports(failures, allRuns)

	// Should have SuiteA and SuiteB (SuiteC is all skipped)
	require.Len(t, reports, 2)

	// Sorted by suite name
	require.Equal(t, "TestFunctionalSuiteA", reports[0].SuiteName)
	require.Equal(t, "TestFunctionalSuiteB", reports[1].SuiteName)

	// SuiteA: 2 failed (run,config) pairs out of 6 total
	// Failed: (run=1, db-a) and (run=2, db-b); retry on (run=1, db-a) doesn't add a new key
	require.Equal(t, 2, reports[0].FailedRuns)
	require.Equal(t, 6, reports[0].TotalRuns)
	require.InDelta(t, 33.3, reports[0].FlakeRate, 0.1)
	require.Equal(t, oneDayAgo, reports[0].LastFailure)

	// SuiteB: 1 failed (run,config) pair out of 2 total
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
