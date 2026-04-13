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
				CIBreakers: []string{"TestSuite/TestCaseA"},
			},
			{
				Name:       "test-job-2",
				Conclusion: "failure",
				URL:        "https://github.com/temporalio/temporal/actions/runs/123456/job/2",
				CIBreakers: []string{"TestSuite/TestCaseB"},
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
			{
				Name:       "test-job-1",
				Conclusion: "failure",
				URL:        "https://github.com/temporalio/temporal/actions/runs/123456/job/1",
				CIBreakers: []string{"TestSuite/TestCaseA"},
			},
		},
		TotalJobs: 5,
	}

	output := FormatMessageForDebug(report)

	assert.Contains(t, output, "CI Failed")
	assert.Contains(t, output, "All Tests")
	assert.Contains(t, output, "abc1234")
	assert.Contains(t, output, "Test Author")
	assert.Contains(t, output, "test-job-1")
	assert.Contains(t, output, "◦ `TestSuite/TestCaseA`")
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

func TestBuildFailureMessageIncludesCIBreakers(t *testing.T) {
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
			{
				Name:       "job1",
				URL:        "http://example.com/job1",
				CIBreakers: []string{"TestSuite/TestCaseA", "TestSuite/TestCaseB"},
			},
		},
		TotalJobs: 3,
	}

	msg := BuildFailureMessage(report)

	require.NotNil(t, msg)
	require.NotNil(t, msg.Blocks[3].Text)
	assert.Contains(t, msg.Blocks[3].Text.Text, "◦ `TestSuite/TestCaseA`")
	assert.Contains(t, msg.Blocks[3].Text.Text, "◦ `TestSuite/TestCaseB`")
}

func TestBuildFailureMessageSkipsCIBreakersWhenTooManyForJob(t *testing.T) {
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
			{
				Name: "job1",
				URL:  "http://example.com/job1",
				CIBreakers: []string{
					"TestSuite/TestCase1",
					"TestSuite/TestCase2",
					"TestSuite/TestCase3",
					"TestSuite/TestCase4",
					"TestSuite/TestCase5",
					"TestSuite/TestCase6",
				},
			},
		},
		TotalJobs: 3,
	}

	msg := BuildFailureMessage(report)

	require.NotNil(t, msg)
	require.NotNil(t, msg.Blocks[3].Text)
	assert.Contains(t, msg.Blocks[3].Text.Text, "• <http://example.com/job1|job1>")
	assert.NotContains(t, msg.Blocks[3].Text.Text, "◦ `TestSuite/TestCase1`")
}

func TestExtractCIBreakers(t *testing.T) {
	logOutput := `
some unrelated line
- TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument (retry 1) (final)
go.temporal.io/server/tests.TestQueryWorkflow_NonStickyMultiPageHistory (retry 2) (final)
  * TestOtherSuite/TestSomething (final)
duplicate TestOtherSuite/TestSomething (final)
not a test line (final)
`

	assert.Equal(t,
		[]string{
			"TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument",
			"go.temporal.io/server/tests.TestQueryWorkflow_NonStickyMultiPageHistory",
			"TestOtherSuite/TestSomething",
		},
		extractCIBreakers(logOutput),
	)
}

func TestFilterCompleted(t *testing.T) {
	tests := []struct {
		name     string
		runs     []WorkflowRunSummary
		expected int
	}{
		{
			name:     "empty slice",
			runs:     []WorkflowRunSummary{},
			expected: 0,
		},
		{
			name: "all completed",
			runs: []WorkflowRunSummary{
				{Conclusion: "success"},
				{Conclusion: "failure"},
			},
			expected: 2,
		},
		{
			name: "mixed with in-progress",
			runs: []WorkflowRunSummary{
				{Conclusion: "success"},
				{Conclusion: ""}, // in-progress
				{Conclusion: "failure"},
			},
			expected: 2,
		},
		{
			name: "with cancelled and skipped",
			runs: []WorkflowRunSummary{
				{Conclusion: "success"},
				{Conclusion: "cancelled"},
				{Conclusion: "skipped"},
				{Conclusion: "failure"},
			},
			expected: 2,
		},
		{
			name: "only in-progress",
			runs: []WorkflowRunSummary{
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
