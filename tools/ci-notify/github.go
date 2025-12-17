package cinotify

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
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
