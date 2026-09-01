package cinotify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/github"
	"go.temporal.io/server/tools/common/slack"
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

	require.Equal(t, &slack.Message{
		Text: "CI Failed on Main",
		Blocks: []slack.Block{
			{
				Type: "section",
				Text: &slack.Text{
					Type: "mrkdwn",
					Text: ":rotating_light: *CI Failed on Main Branch* :rotating_light:",
				},
			},
			{
				Type: "section",
				Text: &slack.Text{
					Type: "mrkdwn",
					Text: "*Failures (2):* `TestHistoryWorkflow`, `TestMatchingWorkflow`",
				},
			},
			{
				Type: "section",
				Text: &slack.Text{
					Type: "mrkdwn",
					Text: "*Failed jobs (2/5):* " +
						"<https://github.com/temporalio/temporal/actions/runs/123456/job/1|test-job-1>, " +
						"<https://github.com/temporalio/temporal/actions/runs/123456/job/2|test-job-2>",
				},
			},
			{
				Type: "section",
				Text: &slack.Text{
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

	require.Equal(t, &slack.Message{
		Text: "CI Failed on Main",
		Blocks: []slack.Block{
			{
				Type: "section",
				Text: &slack.Text{
					Type: "mrkdwn",
					Text: ":rotating_light: *CI Failed on Main Branch* :rotating_light:",
				},
			},
			{
				Type: "section",
				Text: &slack.Text{
					Type: "mrkdwn",
					Text: "*Failed jobs (1/3):* <http://example.com/job1|job1>",
				},
			},
			{
				Type: "section",
				Text: &slack.Text{
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
	require.Equal(t, slack.Block{
		Type: "section",
		Text: &slack.Text{
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

func TestSplitRunsByPeriodUsesAdjacentNonOverlappingWindows(t *testing.T) {
	end := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	currentStart := end.AddDate(0, 0, -7)
	previousStart := currentStart.AddDate(0, 0, -7)
	runs := []github.Run{
		{DatabaseID: 1, CreatedAt: previousStart.Add(-time.Nanosecond)},
		{DatabaseID: 2, CreatedAt: previousStart},
		{DatabaseID: 3, CreatedAt: currentStart.Add(-time.Nanosecond)},
		{DatabaseID: 4, CreatedAt: currentStart},
		{DatabaseID: 5, CreatedAt: end.Add(-time.Nanosecond)},
		{DatabaseID: 6, CreatedAt: end},
	}

	current, previous := splitRunsByPeriod(runs, previousStart, currentStart, end)

	require.Equal(t, []github.Run{runs[3], runs[4]}, current)
	require.Equal(t, []github.Run{runs[1], runs[2]}, previous)
}

func TestSummarizeDigestPeriodUsesOnlyCompletedRunsAndPositiveDurations(t *testing.T) {
	runs := []github.Run{
		{Conclusion: github.ConclusionSuccess, Duration: 10 * time.Minute},
		{Conclusion: github.ConclusionSuccess, Duration: 20 * time.Minute},
		{Conclusion: github.ConclusionFailure, Duration: 30 * time.Minute},
		{Conclusion: github.ConclusionFailure},
		{Conclusion: "cancelled", Duration: 15 * time.Minute},
	}

	period := summarizeDigestPeriod(runs)

	require.Equal(t, 4, period.TotalRuns)
	require.Equal(t, 2, period.SuccessfulRuns)
	require.Equal(t, 2, period.FailedRuns)
	require.InDelta(t, 50.0, period.SuccessRate, 0.001)
	require.Equal(t, 3, period.DurationSamples)
	require.Equal(t, 20*time.Minute, period.AverageDuration)
	require.Equal(t, 20*time.Minute, period.MedianDuration)
	require.InDelta(t, 100.0/3.0, period.Under20MinutesPercent, 0.001)
	require.InDelta(t, 200.0/3.0, period.Under25MinutesPercent, 0.001)
	require.InDelta(t, 200.0/3.0, period.Under30MinutesPercent, 0.001)
	require.Equal(t, runs[:4], period.Runs)
}

func TestBuildDigestAtFetchesBothPeriodsOnce(t *testing.T) {
	end := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	currentStart := end.AddDate(0, 0, -7)
	previousStart := currentStart.AddDate(0, 0, -7)
	fetchCount := 0
	fetchRuns := func(branch, workflowName string, since time.Time) ([]github.Run, error) {
		fetchCount++
		require.Equal(t, "main", branch)
		require.Equal(t, "All Tests", workflowName)
		require.Equal(t, previousStart, since)
		return []github.Run{
			{CreatedAt: previousStart, Conclusion: github.ConclusionFailure, Duration: 20 * time.Minute},
			{CreatedAt: currentStart, Conclusion: github.ConclusionSuccess, Duration: 18 * time.Minute},
		}, nil
	}

	report, err := buildDigestAt("main", "All Tests", 7, end, fetchRuns)

	require.NoError(t, err)
	require.Equal(t, 1, fetchCount)
	require.Equal(t, 1, report.TotalRuns)
	require.Equal(t, 1, report.SuccessfulRuns)
	require.Equal(t, 1, report.Previous.TotalRuns)
	require.Equal(t, 1, report.Previous.FailedRuns)
}

func TestSlowestRuns(t *testing.T) {
	report := &DigestReport{
		DigestPeriod: DigestPeriod{
			Runs: []github.Run{
				{DisplayTitle: "medium", Duration: 20 * time.Minute},
				{DisplayTitle: "ignored", Duration: 0},
				{DisplayTitle: "slowest", Duration: 45 * time.Minute},
				{DisplayTitle: "fastest", Duration: 5 * time.Minute},
				{DisplayTitle: "second slowest", Duration: 30 * time.Minute},
			},
		},
	}

	require.Equal(t, []github.Run{
		{DisplayTitle: "slowest", Duration: 45 * time.Minute},
		{DisplayTitle: "second slowest", Duration: 30 * time.Minute},
		{DisplayTitle: "medium", Duration: 20 * time.Minute},
	}, report.slowestRuns(3))
}

func TestBuildSuccessReportMessageShowsComparisonsAndCombinedRunCounts(t *testing.T) {
	report := &DigestReport{
		Branch:    "main",
		StartDate: time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC),
		EndDate:   time.Date(2026, time.September, 8, 0, 0, 0, 0, time.UTC),
		DigestPeriod: DigestPeriod{
			TotalRuns:       50,
			SuccessfulRuns:  39,
			FailedRuns:      11,
			SuccessRate:     78.0,
			DurationSamples: 50,
			AverageDuration: 24 * time.Minute,
			MedianDuration:  23 * time.Minute,
		},
		Previous: DigestPeriod{
			TotalRuns:       40,
			SuccessRate:     74.8,
			DurationSamples: 40,
			AverageDuration: 26 * time.Minute,
			MedianDuration:  23 * time.Minute,
		},
	}

	message := BuildSuccessReportMessage(report)

	require.Equal(t, []slack.Text{
		{Type: "mrkdwn", Text: "*Success Rate:*\n78.0% (↑ 3.2 pp)"},
		{Type: "mrkdwn", Text: "*Failed Runs:*\n11/50"},
		{Type: "mrkdwn", Text: "*Average Duration:*\n24m0s (↓ 2m0s)"},
		{Type: "mrkdwn", Text: "*Median Duration:*\n23m0s (— no change)"},
	}, message.Blocks[2].Fields)
}

func TestBuildSuccessReportMessageShowsUnavailableComparisonsWithoutSamples(t *testing.T) {
	tests := []struct {
		name     string
		current  DigestPeriod
		previous DigestPeriod
		want     []slack.Text
	}{
		{
			name: "previous period has no samples",
			current: DigestPeriod{
				TotalRuns:       10,
				FailedRuns:      2,
				SuccessRate:     80,
				DurationSamples: 10,
				AverageDuration: 20 * time.Minute,
				MedianDuration:  19 * time.Minute,
			},
			want: []slack.Text{
				{Type: "mrkdwn", Text: "*Success Rate:*\n80.0% (N/A)"},
				{Type: "mrkdwn", Text: "*Failed Runs:*\n2/10"},
				{Type: "mrkdwn", Text: "*Average Duration:*\n20m0s (N/A)"},
				{Type: "mrkdwn", Text: "*Median Duration:*\n19m0s (N/A)"},
			},
		},
		{
			name: "current period has no samples",
			previous: DigestPeriod{
				TotalRuns:       10,
				SuccessRate:     80,
				DurationSamples: 10,
				AverageDuration: 20 * time.Minute,
				MedianDuration:  19 * time.Minute,
			},
			want: []slack.Text{
				{Type: "mrkdwn", Text: "*Success Rate:*\n0.0% (N/A)"},
				{Type: "mrkdwn", Text: "*Failed Runs:*\n0/0"},
				{Type: "mrkdwn", Text: "*Average Duration:*\n0s (N/A)"},
				{Type: "mrkdwn", Text: "*Median Duration:*\n0s (N/A)"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := &DigestReport{DigestPeriod: tt.current, Previous: tt.previous}

			message := BuildSuccessReportMessage(report)

			require.Equal(t, tt.want, message.Blocks[2].Fields)
		})
	}
}

func TestFormatReportForDebugShowsComparisonsAndCombinedRunCounts(t *testing.T) {
	report := &DigestReport{
		Branch: "main",
		DigestPeriod: DigestPeriod{
			TotalRuns:       50,
			SuccessfulRuns:  36,
			FailedRuns:      14,
			SuccessRate:     72,
			DurationSamples: 50,
			AverageDuration: 26 * time.Minute,
			MedianDuration:  23 * time.Minute,
		},
		Previous: DigestPeriod{
			TotalRuns:       40,
			SuccessRate:     78,
			DurationSamples: 40,
			AverageDuration: 24 * time.Minute,
			MedianDuration:  23 * time.Minute,
		},
	}

	output := FormatReportForDebug(report)

	require.Contains(t, output, "Metrics:\n"+
		"  Success Rate: 72.0% (↓ 6.0 pp)\n"+
		"  Failed Runs: 14/50\n"+
		"  Average Duration: 26m0s (↑ 2m0s)\n"+
		"  Median Duration: 23m0s (— no change)\n")
	require.NotContains(t, output, "Total Runs:")
	require.NotContains(t, output, "Successful Runs:")
}
