package cinotify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/github"
)

func TestBuildFailureMessage(t *testing.T) {
	report := &FailureReport{
		Run: github.Run{
			Name:       "All Tests",
			HeadBranch: "main",
			HeadSHA:    "abc1234567890defghijk",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123456",
			CreatedAt:  time.Now(),
		},
		FailedJobs: []github.Job{
			{
				Name:       "test-job-1",
				Conclusion: "failure",
				URL:        "https://github.com/temporalio/temporal/actions/runs/123456/job/1",
			},
			{
				Name:       "test-job-2",
				Conclusion: "failure",
				URL:        "https://github.com/temporalio/temporal/actions/runs/123456/job/2",
			},
		},
		Failures:  []string{"TestHistoryWorkflow", "TestMatchingWorkflow"},
		TotalJobs: 5,
	}

	msg := BuildFailureMessage(report)

	require.NotNil(t, msg, "BuildFailureMessage returned nil")
	assert.Contains(t, msg.Text, "CI Failed")
	assert.NotEmpty(t, msg.Blocks, "Message should have at least one block")

	// Check that the first block contains the header
	require.NotNil(t, msg.Blocks[0].Text)
	assert.Contains(t, msg.Blocks[0].Text.Text, "CI Failed")
	require.NotNil(t, msg.Blocks[1].Text)
	assert.Contains(t, msg.Blocks[1].Text.Text, "TestHistoryWorkflow")
	assert.Contains(t, msg.Blocks[1].Text.Text, "TestMatchingWorkflow")
}

func TestFormatMessageForDebug(t *testing.T) {
	report := &FailureReport{
		Run: github.Run{
			Name:       "All Tests",
			HeadBranch: "main",
			HeadSHA:    "abc1234567890defghijk",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123456",
		},
		FailedJobs: []github.Job{{Name: "test-job-1", Conclusion: "failure"}},
		Failures:   []string{"TestHistoryWorkflow"},
		TotalJobs:  5,
	}

	output := FormatMessageForDebug(report)

	assert.Contains(t, output, "CI Failed")
	assert.NotContains(t, output, "Workflow:")
	assert.NotContains(t, output, "Branch:")
	assert.NotContains(t, output, "Commit:")
	assert.Contains(t, output, "test-job-1")
	assert.Contains(t, output, "TestHistoryWorkflow")
}

func TestSlackMessageStructure(t *testing.T) {
	report := &FailureReport{
		Run: github.Run{
			Name:       "All Tests",
			HeadBranch: "main",
			HeadSHA:    "abc1234567890",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123",
		},
		FailedJobs: []github.Job{
			{Name: "job1", URL: "http://example.com/job1"},
		},
		TotalJobs: 3,
	}

	msg := BuildFailureMessage(report)

	// Verify we have the expected number of blocks
	// Header, Jobs List, Link = 3 blocks
	assert.Len(t, msg.Blocks, 3)

	require.NotNil(t, msg.Blocks[1].Text)
	assert.Contains(t, msg.Blocks[1].Text.Text, "*Failed jobs (1/3):*")
}

func TestBuildFailureMessageLimitsFailures(t *testing.T) {
	report := &FailureReport{
		Run: github.Run{
			URL: "https://github.com/temporalio/temporal/actions/runs/123",
		},
		FailedJobs: []github.Job{
			{Name: "job1", URL: "http://example.com/job1"},
		},
		Failures: []string{
			"Test01",
			"Test02",
			"Test03",
			"Test04",
			"Test05",
			"Test06",
		},
		TotalJobs: 3,
	}

	msg := BuildFailureMessage(report)

	require.Len(t, msg.Blocks, 4)
	require.NotNil(t, msg.Blocks[1].Text)
	assert.Contains(t, msg.Blocks[1].Text.Text, "*Failures (6):*")
	assert.Contains(t, msg.Blocks[1].Text.Text, "Test05")
	assert.NotContains(t, msg.Blocks[1].Text.Text, "Test06")
}

func TestIsFailedJobExcludesTestStatus(t *testing.T) {
	require.True(t, isFailedJob(github.Job{
		Name:       "Functional test (sqlite, shard1)",
		Conclusion: github.ConclusionFailure,
	}))
	require.False(t, isFailedJob(github.Job{
		Name:       testStatusJobName,
		Conclusion: github.ConclusionFailure,
	}))
	require.False(t, isFailedJob(github.Job{
		Name:       "Functional test (sqlite, shard1)",
		Conclusion: github.ConclusionSuccess,
	}))
}

func TestFilterCompleted(t *testing.T) {
	tests := []struct {
		name     string
		runs     []github.Run
		expected int
	}{
		{
			name:     "empty slice",
			runs:     []github.Run{},
			expected: 0,
		},
		{
			name: "all completed",
			runs: []github.Run{
				{Conclusion: "success"},
				{Conclusion: "failure"},
			},
			expected: 2,
		},
		{
			name: "mixed with in-progress",
			runs: []github.Run{
				{Conclusion: "success"},
				{Conclusion: ""}, // in-progress
				{Conclusion: "failure"},
			},
			expected: 2,
		},
		{
			name: "with cancelled and skipped",
			runs: []github.Run{
				{Conclusion: "success"},
				{Conclusion: "cancelled"},
				{Conclusion: "skipped"},
				{Conclusion: "failure"},
			},
			expected: 2,
		},
		{
			name: "only in-progress",
			runs: []github.Run{
				{Conclusion: ""},
				{Conclusion: ""},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterCompleted(tt.runs)
			assert.Len(t, result, tt.expected)
		})
	}
}

func TestSlowestRuns(t *testing.T) {
	report := &DigestReport{
		Runs: []github.Run{
			{DisplayTitle: "medium", Duration: 20 * time.Minute},
			{DisplayTitle: "ignored", Duration: 0},
			{DisplayTitle: "slowest", Duration: 45 * time.Minute},
			{DisplayTitle: "fastest", Duration: 5 * time.Minute},
			{DisplayTitle: "second slowest", Duration: 30 * time.Minute},
		},
	}

	result := report.slowestRuns(3)

	require.Len(t, result, 3)
	require.Equal(t, "slowest", result[0].DisplayTitle)
	require.Equal(t, "second slowest", result[1].DisplayTitle)
	require.Equal(t, "medium", result[2].DisplayTitle)
}
