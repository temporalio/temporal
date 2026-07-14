package cinotify

import (
	"testing"
	"time"

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

	require.Equal(t, &SlackMessage{
		Text: "CI Failed on Main",
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: ":rotating_light: *CI Failed on Main Branch* :rotating_light:",
				},
			},
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: "*Failures (2):* `TestHistoryWorkflow`, `TestMatchingWorkflow`",
				},
			},
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: "*Failed jobs (2/5):* " +
						"<https://github.com/temporalio/temporal/actions/runs/123456/job/1|test-job-1>, " +
						"<https://github.com/temporalio/temporal/actions/runs/123456/job/2|test-job-2>",
				},
			},
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: "<https://github.com/temporalio/temporal/actions/runs/123456|View Run>",
				},
			},
		},
	}, BuildFailureMessage(report))
}

func TestFormatMessageForDebug(t *testing.T) {
	report := &FailureReport{
		Run: github.Run{
			Name:       "All Tests",
			HeadBranch: "main",
			HeadSHA:    "abc1234567890defghijk",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123456",
		},
		FailedJobs: []github.Job{{
			Name:       "test-job-1",
			Conclusion: "failure",
			URL:        "https://github.com/temporalio/temporal/actions/runs/123456/job/1",
		}},
		Failures:  []string{"TestHistoryWorkflow"},
		TotalJobs: 5,
	}

	require.Equal(t, "🚨 CI Failed on Main Branch 🚨\n\n"+
		"Failures (1): `TestHistoryWorkflow`\n\n"+
		"Failed jobs (1/5): test-job-1 (https://github.com/temporalio/temporal/actions/runs/123456/job/1)\n"+
		"\nView Run: https://github.com/temporalio/temporal/actions/runs/123456\n",
		FormatMessageForDebug(report))
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

	require.Equal(t, &SlackMessage{
		Text: "CI Failed on Main",
		Blocks: []SlackBlock{
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: ":rotating_light: *CI Failed on Main Branch* :rotating_light:",
				},
			},
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: "*Failed jobs (1/3):* <http://example.com/job1|job1>",
				},
			},
			{
				Type: "section",
				Text: &SlackText{
					Type: "mrkdwn",
					Text: "<https://github.com/temporalio/temporal/actions/runs/123|View Run>",
				},
			},
		},
	}, BuildFailureMessage(report))
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
	require.Equal(t, SlackBlock{
		Type: "section",
		Text: &SlackText{
			Type: "mrkdwn",
			Text: "*Failures (6):* `Test01`, `Test02`, `Test03`, `Test04`, `Test05`",
		},
	}, msg.Blocks[1])
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
		expected []github.Run
	}{
		{
			name: "empty slice",
			runs: []github.Run{},
		},
		{
			name: "all completed",
			runs: []github.Run{
				{Conclusion: "success"},
				{Conclusion: "failure"},
			},
			expected: []github.Run{
				{Conclusion: "success"},
				{Conclusion: "failure"},
			},
		},
		{
			name: "mixed with in-progress",
			runs: []github.Run{
				{Conclusion: "success"},
				{Conclusion: ""}, // in-progress
				{Conclusion: "failure"},
			},
			expected: []github.Run{
				{Conclusion: "success"},
				{Conclusion: "failure"},
			},
		},
		{
			name: "with cancelled and skipped",
			runs: []github.Run{
				{Conclusion: "success"},
				{Conclusion: "cancelled"},
				{Conclusion: "skipped"},
				{Conclusion: "failure"},
			},
			expected: []github.Run{
				{Conclusion: "success"},
				{Conclusion: "failure"},
			},
		},
		{
			name: "only in-progress",
			runs: []github.Run{
				{Conclusion: ""},
				{Conclusion: ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, filterCompleted(tt.runs))
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

	require.Equal(t, []github.Run{
		{DisplayTitle: "slowest", Duration: 45 * time.Minute},
		{DisplayTitle: "second slowest", Duration: 30 * time.Minute},
		{DisplayTitle: "medium", Duration: 20 * time.Minute},
	}, report.slowestRuns(3))
}
