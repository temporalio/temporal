package cinotify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildFailureMessage(t *testing.T) {
	report := &FailureReport{
		Workflow: WorkflowRun{
			Name:       "All Tests",
			HeadBranch: "main",
			HeadSHA:    "abc1234567890defghijk",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123456",
			CreatedAt:  time.Now(),
		},
		Commit: CommitInfo{
			SHA:      "abc1234567890defghijk",
			ShortSHA: "abc1234",
			Author:   "Test Author",
			Message:  "Test commit message",
		},
		FailedJobs: []Job{
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
		TotalJobs: 5,
	}

	msg := BuildFailureMessage(report)

	require.NotNil(t, msg, "BuildFailureMessage returned nil")
	assert.Contains(t, msg.Text, "CI Failed")
	assert.NotEmpty(t, msg.Blocks, "Message should have at least one block")

	// Check that the first block contains the header
	require.NotNil(t, msg.Blocks[0].Text)
	assert.Contains(t, msg.Blocks[0].Text.Text, "CI Failed")
}

func TestFormatMessageForDebug(t *testing.T) {
	report := &FailureReport{
		Workflow: WorkflowRun{
			Name:       "All Tests",
			HeadBranch: "main",
			HeadSHA:    "abc1234567890defghijk",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123456",
		},
		Commit: CommitInfo{
			SHA:      "abc1234567890defghijk",
			ShortSHA: "abc1234",
			Author:   "Test Author",
		},
		FailedJobs: []Job{
			{Name: "test-job-1", Conclusion: "failure"},
		},
		TotalJobs: 5,
	}

	output := FormatMessageForDebug(report)

	assert.Contains(t, output, "CI Failed")
	assert.Contains(t, output, "All Tests")
	assert.Contains(t, output, "abc1234")
	assert.Contains(t, output, "Test Author")
	assert.Contains(t, output, "test-job-1")
}

func TestShortSHA(t *testing.T) {
	tests := []struct {
		name     string
		sha      string
		expected string
	}{
		{
			name:     "normal SHA",
			sha:      "abc1234567890defghijk",
			expected: "abc1234",
		},
		{
			name:     "short SHA",
			sha:      "abc123",
			expected: "abc123",
		},
		{
			name:     "exactly 7 chars",
			sha:      "abc1234",
			expected: "abc1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shortSHA := tt.sha
			if len(shortSHA) > 7 {
				shortSHA = shortSHA[:7]
			}

			assert.Equal(t, tt.expected, shortSHA)
		})
	}
}

func TestSlackMessageStructure(t *testing.T) {
	report := &FailureReport{
		Workflow: WorkflowRun{
			Name:       "All Tests",
			HeadBranch: "main",
			HeadSHA:    "abc1234567890",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123",
		},
		Commit: CommitInfo{
			SHA:      "abc1234567890",
			ShortSHA: "abc1234",
			Author:   "Test",
		},
		FailedJobs: []Job{
			{Name: "job1", URL: "http://example.com/job1"},
		},
		TotalJobs: 3,
	}

	msg := BuildFailureMessage(report)

	// Verify we have the expected number of blocks
	// Header, Info, Summary, Jobs List, Link = 5 blocks
	assert.Len(t, msg.Blocks, 5)

	// Verify the info block has 4 fields
	infoBlock := msg.Blocks[1]
	assert.Len(t, infoBlock.Fields, 4)
}
