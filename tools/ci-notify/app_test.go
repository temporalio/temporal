package cinotify

import (
	"strings"
	"testing"
	"time"
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

	if msg == nil {
		t.Fatal("BuildFailureMessage returned nil")
	}

	if !strings.Contains(msg.Text, "CI Failed") {
		t.Errorf("Message text should contain 'CI Failed', got: %s", msg.Text)
	}

	if len(msg.Blocks) == 0 {
		t.Error("Message should have at least one block")
	}

	// Check that the first block contains the header
	if msg.Blocks[0].Text == nil || !strings.Contains(msg.Blocks[0].Text.Text, "CI Failed") {
		t.Error("First block should be the header with 'CI Failed'")
	}
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

	if !strings.Contains(output, "CI Failed") {
		t.Error("Debug output should contain 'CI Failed'")
	}

	if !strings.Contains(output, "All Tests") {
		t.Error("Debug output should contain workflow name")
	}

	if !strings.Contains(output, "abc1234") {
		t.Error("Debug output should contain short SHA")
	}

	if !strings.Contains(output, "Test Author") {
		t.Error("Debug output should contain author")
	}

	if !strings.Contains(output, "test-job-1") {
		t.Error("Debug output should contain failed job name")
	}
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

			if shortSHA != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, shortSHA)
			}
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
	if len(msg.Blocks) != 5 {
		t.Errorf("Expected 5 blocks, got %d", len(msg.Blocks))
	}

	// Verify the info block has 4 fields
	infoBlock := msg.Blocks[1]
	if len(infoBlock.Fields) != 4 {
		t.Errorf("Info block should have 4 fields, got %d", len(infoBlock.Fields))
	}
}
