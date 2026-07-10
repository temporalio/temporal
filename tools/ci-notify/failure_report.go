package cinotify

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/tools/common/github"
)

const testStatusJobName = "Test Status"

// getWorkflowRun fetches workflow run details for failure notifications.
func getWorkflowRun(runID string) (*github.Run, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := github.ViewRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow run: %w", err)
	}

	return &run, nil
}

// BuildFailureReport aggregates all failure information
func BuildFailureReport(runID string) (*FailureReport, error) {
	run, err := getWorkflowRun(runID)
	if err != nil {
		return nil, err
	}

	// Identify failed jobs
	var failedJobs []github.Job
	for _, job := range run.Jobs {
		if isFailedJob(job) {
			failedJobs = append(failedJobs, job)
		}
	}

	failures, err := getFailures(context.Background(), *run, runID)
	if err != nil {
		failures = nil
	}

	return &FailureReport{
		Run:        *run,
		FailedJobs: failedJobs,
		Failures:   failures,
		TotalJobs:  len(run.Jobs),
	}, nil
}

func isFailedJob(job github.Job) bool {
	return job.Conclusion == github.ConclusionFailure && job.Name != testStatusJobName
}
