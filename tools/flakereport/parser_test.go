package flakereport

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	grouped := map[string][]TestFailure{
		"TestNormal": {
			{Name: "TestNormal"}, {Name: "TestNormal"},
		},
		"TestFlaky": {
			{Name: "TestFlaky"}, {Name: "TestFlaky"},
			{Name: "TestFlaky"}, {Name: "TestFlaky"},
			{Name: "TestFlaky"},
		},
		"TestTimeout (timeout)": {
			{Name: "TestTimeout (timeout)"}, {Name: "TestTimeout (timeout)"},
		},
		"TestCrash": {
			{Name: "TestCrash"}, {Name: "TestCrash"},
		},
	}

	// Modify the test name to include "crash"
	grouped["TestCrashHandler"] = []TestFailure{
		{Name: "TestCrashHandler"}, {Name: "TestCrashHandler"},
	}
	delete(grouped, "TestCrash")

	flaky, timeout, crash := classifyFailures(grouped)

	// TestFlaky should be in flaky (>3 failures)
	assert.Contains(t, flaky, "TestFlaky")
	assert.Len(t, flaky["TestFlaky"], 5)

	// TestTimeout should be in timeout
	assert.Contains(t, timeout, "TestTimeout (timeout)")
	assert.Len(t, timeout["TestTimeout (timeout)"], 2)

	// TestCrashHandler should be in crash
	assert.Contains(t, crash, "TestCrashHandler")
	assert.Len(t, crash["TestCrashHandler"], 2)

	// TestNormal should not be in flaky (only 2 failures)
	assert.NotContains(t, flaky, "TestNormal")
}
