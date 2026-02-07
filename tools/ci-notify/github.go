package cinotify

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// GetWorkflowRun fetches workflow run details using gh CLI
func GetWorkflowRun(runID string) (*WorkflowRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "gh", "run", "view", runID, "--json",
		"conclusion,name,headBranch,headSha,url,displayTitle,event,createdAt,jobs")

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("failed to get workflow run: %w (stderr: %s)", err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("failed to get workflow run: %w", err)
	}

	var run WorkflowRun
	if err := json.Unmarshal(output, &run); err != nil {
		return nil, fmt.Errorf("failed to parse workflow run: %w", err)
	}

	return &run, nil
}

// GetCommitAuthor fetches commit author using gh CLI
func GetCommitAuthor(sha string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "gh", "api",
		fmt.Sprintf("repos/temporalio/temporal/commits/%s", sha),
		"--jq", ".commit.author.name")

	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get commit author: %w", err)
	}

	return strings.TrimSpace(string(output)), nil
}

// BuildFailureReport aggregates all failure information
func BuildFailureReport(runID string) (*FailureReport, error) {
	run, err := GetWorkflowRun(runID)
	if err != nil {
		return nil, err
	}

	author, err := GetCommitAuthor(run.HeadSHA)
	if err != nil {
		// Non-fatal: use unknown if we can't get author
		author = "Unknown"
	}

	shortSHA := run.HeadSHA
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}

	commit := CommitInfo{
		SHA:      run.HeadSHA,
		ShortSHA: shortSHA,
		Author:   author,
		Message:  run.DisplayTitle,
	}

	// Identify failed jobs
	var failedJobs []Job
	for _, job := range run.Jobs {
		if job.Conclusion == "failure" {
			failedJobs = append(failedJobs, job)
		}
	}

	return &FailureReport{
		Workflow:   *run,
		Commit:     commit,
		FailedJobs: failedJobs,
		TotalJobs:  len(run.Jobs),
	}, nil
}

// filterCompleted removes workflow runs that are not completed
func filterCompleted(runs []WorkflowRunSummary) []WorkflowRunSummary {
	var completed []WorkflowRunSummary
	for _, run := range runs {
		// Only include runs with a conclusion (success or failure)
		if run.Conclusion == "success" || run.Conclusion == "failure" {
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
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

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

// GetWorkflowRuns fetches workflow runs for a branch within a time range
func GetWorkflowRuns(branch, workflowName string, since time.Time) ([]WorkflowRunSummary, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Format the date as YYYY-MM-DD for gh CLI
	sinceDate := since.Format("2006-01-02")

	cmd := exec.CommandContext(ctx, "gh", "run", "list",
		"--branch", branch,
		"--workflow", workflowName,
		"--created", ">="+sinceDate,
		"--limit", "1000",
		"--json", "conclusion,name,event,createdAt,startedAt,updatedAt,headSha,displayTitle,url")

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("failed to get workflow runs: %w (stderr: %s)", err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("failed to get workflow runs: %w", err)
	}

	var runs []WorkflowRunSummary
	if err := json.Unmarshal(output, &runs); err != nil {
		return nil, fmt.Errorf("failed to parse workflow runs: %w", err)
	}

	// Calculate duration for each run
	for i := range runs {
		if !runs[i].CreatedAt.IsZero() && !runs[i].UpdatedAt.IsZero() {
			runs[i].Duration = runs[i].UpdatedAt.Sub(runs[i].CreatedAt)
		}
	}

	return runs, nil
}

// BuildSuccessReport builds a success report for the specified time range
func BuildSuccessReport(branch, workflowName string, days int) (*SuccessReport, error) {
	// Calculate the start date
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -days)

	// Fetch workflow runs
	runs, err := GetWorkflowRuns(branch, workflowName, startDate)
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
		case "success":
			successCount++
		case "failure":
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

	return &SuccessReport{
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
