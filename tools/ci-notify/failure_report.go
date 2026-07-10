package cinotify

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/tools/common/github"
)

// getWorkflowRun fetches workflow run details for failure notifications.
func getWorkflowRun(runID string) (*WorkflowRun, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var run WorkflowRun
	if err := github.RunView(ctx, runID, []string{
		"conclusion",
		"name",
		"headBranch",
		"headSha",
		"url",
		"displayTitle",
		"event",
		"createdAt",
		"jobs",
	}, &run); err != nil {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	commitMeta, err := github.GetCommit(ctx, "temporalio/temporal", run.HeadSHA)
	author := commitMeta.Author
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
		if job.Conclusion == ConclusionFailure {
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
