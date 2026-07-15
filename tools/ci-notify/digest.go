package cinotify

import (
	"context"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/server/tools/common/github"
)

// filterCompleted removes workflow runs that are not completed
func filterCompleted(runs []github.Run) []github.Run {
	var completed []github.Run
	for _, run := range runs {
		// Only include runs with a conclusion (success or failure)
		if run.Conclusion == github.ConclusionSuccess || run.Conclusion == github.ConclusionFailure {
			completed = append(completed, run)
		}
	}
	return completed
}

// calculateAverage computes the mean duration
func calculateAverage(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	var total time.Duration
	for _, d := range durations {
		total += d
	}
	return total / time.Duration(len(durations))
}

// calculateMedian computes the median duration
func calculateMedian(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}

	// Make a copy to avoid modifying original
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	slices.Sort(sorted)

	n := len(sorted)
	if n%2 == 0 {
		// Even number of elements: average of middle two
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	// Odd number of elements: middle element
	return sorted[n/2]
}

// formatDuration formats a duration in human-readable form
func formatDuration(d time.Duration) string {
	return d.Round(time.Second).String()
}

// calculatePercentUnder calculates the percentage of durations under a threshold
func calculatePercentUnder(durations []time.Duration, threshold time.Duration) float64 {
	if len(durations) == 0 {
		return 0.0
	}

	var count int
	for _, d := range durations {
		if d < threshold {
			count++
		}
	}

	return (float64(count) / float64(len(durations))) * 100
}

// getWorkflowRuns fetches workflow runs for a branch within a time range.
func getWorkflowRuns(branch, workflowName string, since time.Time) ([]github.Run, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Format the date as YYYY-MM-DD for gh CLI
	sinceDate := since.Format("2006-01-02")

	runs, err := github.ListRuns(ctx, github.RunListOptions{
		Branch:   branch,
		Workflow: workflowName,
		Created:  ">=" + sinceDate,
		Limit:    1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow runs: %w", err)
	}

	// Calculate duration for each run (actual execution time, not including queue time)
	for i := range runs {
		if !runs[i].StartedAt.IsZero() && !runs[i].UpdatedAt.IsZero() {
			runs[i].Duration = runs[i].UpdatedAt.Sub(runs[i].StartedAt)
		}
	}

	return runs, nil
}

// BuildDigest builds a digest report for the specified time range
func BuildDigest(branch, workflowName string, days int) (*DigestReport, error) {
	// Calculate the start date
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -days)

	// Fetch workflow runs
	runs, err := getWorkflowRuns(branch, workflowName, startDate)
	if err != nil {
		return nil, err
	}

	// Filter to only completed runs
	completedRuns := filterCompleted(runs)

	// Count successes and failures
	var successCount, failureCount int
	var durations []time.Duration

	for _, run := range completedRuns {
		switch run.Conclusion {
		case github.ConclusionSuccess:
			successCount++
		default:
			failureCount++
		}

		if run.Duration > 0 {
			durations = append(durations, run.Duration)
		}
	}

	totalRuns := len(completedRuns)
	successRate := 0.0
	if totalRuns > 0 {
		successRate = (float64(successCount) / float64(totalRuns)) * 100
	}

	// Calculate duration percentiles
	under20 := calculatePercentUnder(durations, 20*time.Minute)
	under25 := calculatePercentUnder(durations, 25*time.Minute)
	under30 := calculatePercentUnder(durations, 30*time.Minute)

	return &DigestReport{
		Branch:                branch,
		WorkflowName:          workflowName,
		StartDate:             startDate,
		EndDate:               endDate,
		TotalRuns:             totalRuns,
		SuccessfulRuns:        successCount,
		FailedRuns:            failureCount,
		SuccessRate:           successRate,
		AverageDuration:       calculateAverage(durations),
		MedianDuration:        calculateMedian(durations),
		Under20MinutesPercent: under20,
		Under25MinutesPercent: under25,
		Under30MinutesPercent: under30,
		Runs:                  completedRuns,
	}, nil
}
