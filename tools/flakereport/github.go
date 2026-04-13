package flakereport

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	sharedgithub "go.temporal.io/server/tools/shared/github"
)

// fetchWorkflowRuns retrieves all completed workflow runs within a date range.
// since is the oldest bound (inclusive); until is the newest bound (zero means open-ended).
// Implements proper pagination to fix the 100-run limit bug.
func fetchWorkflowRuns(ctx context.Context, repo string, workflowID int64, branch string, since, until time.Time) ([]WorkflowRun, error) {
	var allRuns []WorkflowRun

	createdFilter := ">=" + since.Format("2006-01-02")
	if !until.IsZero() {
		createdFilter = since.Format("2006-01-02") + ".." + until.Format("2006-01-02")
	}
	fmt.Printf("Fetching workflow runs created %s...\n", createdFilter)

	page := 1
	for {
		ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)

		cmd := exec.CommandContext(ctxTimeout, "gh", "api",
			fmt.Sprintf("/repos/%s/actions/workflows/%d/runs?branch=%s&created=%s&per_page=100&page=%d",
				repo, workflowID, branch, createdFilter, page),
		)

		output, err := cmd.Output()
		cancel() // Cancel context immediately after command completes

		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				return nil, fmt.Errorf("gh api failed (page %d): %w\nstderr: %s", page, err, string(exitErr.Stderr))
			}
			return nil, fmt.Errorf("failed to execute gh command (page %d): %w", page, err)
		}

		var response struct {
			WorkflowRuns []WorkflowRun `json:"workflow_runs"`
		}

		if err := json.Unmarshal(output, &response); err != nil {
			return nil, fmt.Errorf("failed to parse workflow runs response (page %d): %w", page, err)
		}

		if len(response.WorkflowRuns) == 0 {
			break
		}

		allRuns = append(allRuns, response.WorkflowRuns...)
		fmt.Printf("Fetched page %d: %d runs (total: %d)\n", page, len(response.WorkflowRuns), len(allRuns))

		// If we got fewer than 100 results, this is the last page
		if len(response.WorkflowRuns) < 100 {
			break
		}

		page++
	}

	fmt.Printf("Total workflow runs fetched: %d\n", len(allRuns))
	return allRuns, nil
}

// fetchRunArtifacts retrieves all artifacts for a specific workflow run
func fetchRunArtifacts(ctx context.Context, repo string, runID int64) ([]WorkflowArtifact, error) {
	artifacts, err := sharedgithub.ListRunArtifacts(ctx, repo, runID)
	if err != nil {
		return nil, err
	}

	var testArtifacts []WorkflowArtifact
	for _, artifact := range sharedgithub.FilterArtifactsContaining(artifacts, "junit") {
		testArtifacts = append(testArtifacts, WorkflowArtifact{
			ID:      artifact.ID,
			Name:    artifact.Name,
			Expired: artifact.Expired,
		})
	}

	return testArtifacts, nil
}

// parseArtifactName extracts run_id, job_id, and matrix_name from artifact name.
// Functional tests: junit-xml--{run_id}--{job_id}--{run_attempt}--{matrix_name}--{display_name}--functional-test
// Unit/integration:  junit-xml--{run_id}--{job_id}--{run_attempt}--unit-test
// Returns: runID, jobID, matrixName ("unknown" for fields that are absent or unparseable)
func parseArtifactName(artifactName string) (runID string, jobID string, matrixName string) {
	parts := strings.Split(artifactName, "--")
	if len(parts) < 3 {
		return "unknown", "unknown", "unknown"
	}

	runID = parts[1]

	jobID = parts[2]
	if jobID == "" {
		jobID = "unknown"
	}

	// Functional test artifacts carry a matrix name (DB config) at parts[4].
	// Unit/integration artifacts have only 5 parts where parts[4] is the test type
	// (e.g. "unit-test"), not a matrix name. Functional artifacts have >=7 parts.
	if len(parts) >= 6 {
		matrixName = parts[4]
	} else {
		matrixName = "unknown"
	}

	return runID, jobID, matrixName
}

// buildGitHubURL constructs GitHub Actions URL from run/job IDs
// If jobID == "unknown": https://github.com/{repo}/actions/runs/{runID}
// Otherwise: https://github.com/{repo}/actions/runs/{runID}/job/{jobID}
func buildGitHubURL(repo, runID, jobID string) string {
	baseURL := fmt.Sprintf("https://github.com/%s/actions/runs/%s", repo, runID)
	if jobID != "unknown" && jobID != "" {
		return fmt.Sprintf("%s/job/%s", baseURL, jobID)
	}
	return baseURL
}

// fetchCommitMeta fetches commit title, author, and changed file list for a single commit SHA.
// Uses: GET /repos/{owner}/{repo}/commits/{sha}
func fetchCommitMeta(ctx context.Context, repo, sha string) (CommitMeta, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctxTimeout, "gh", "api",
		fmt.Sprintf("/repos/%s/commits/%s", repo, sha),
	)

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return CommitMeta{SHA: sha}, fmt.Errorf("gh api failed for commit %s: %w\nstderr: %s", sha, err, string(exitErr.Stderr))
		}
		return CommitMeta{SHA: sha}, fmt.Errorf("failed to execute gh command for commit %s: %w", sha, err)
	}

	var response struct {
		SHA    string `json:"sha"`
		Commit struct {
			Message string `json:"message"`
			Author  struct {
				Name string    `json:"name"`
				Date time.Time `json:"date"`
			} `json:"author"`
		} `json:"commit"`
		Files []struct {
			Filename string `json:"filename"`
		} `json:"files"`
	}

	if err := json.Unmarshal(output, &response); err != nil {
		return CommitMeta{SHA: sha}, fmt.Errorf("failed to parse commit response for %s: %w", sha, err)
	}

	// Extract just the first line of the commit message as the title
	title := response.Commit.Message
	if idx := strings.IndexByte(title, '\n'); idx >= 0 {
		title = title[:idx]
	}

	files := make([]string, 0, len(response.Files))
	for _, f := range response.Files {
		files = append(files, f.Filename)
	}

	return CommitMeta{
		SHA:         sha,
		Title:       title,
		Author:      response.Commit.Author.Name,
		CommittedAt: response.Commit.Author.Date,
		Files:       files,
	}, nil
}
