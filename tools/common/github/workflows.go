package github

import (
	"context"
	"fmt"
	"time"
)

// WorkflowRun represents a GitHub Actions workflow run from the REST API.
type WorkflowRun struct {
	ID         int64     `json:"id"`
	Number     int       `json:"run_number"`
	CreatedAt  time.Time `json:"created_at"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion"`
	HeadBranch string    `json:"head_branch"`
	HeadSHA    string    `json:"head_sha"`
}

// ListWorkflowRunsOptions configures workflow-run listing through the REST API.
type ListWorkflowRunsOptions struct {
	Branch  string
	Created string
}

// ListWorkflowRuns retrieves workflow runs through the GitHub Actions REST API.
func ListWorkflowRuns(ctx context.Context, repo string, workflowID int64, opts ListWorkflowRunsOptions) ([]WorkflowRun, error) {
	var allRuns []WorkflowRun

	page := 1
	for {
		var response struct {
			WorkflowRuns []WorkflowRun `json:"workflow_runs"`
		}
		path := fmt.Sprintf("/repos/%s/actions/workflows/%d/runs?branch=%s&created=%s&per_page=100&page=%d",
			repo, workflowID, opts.Branch, opts.Created, page)
		if err := apiJSON(ctx, path, &response); err != nil {
			return nil, fmt.Errorf("failed to fetch workflow runs page %d: %w", page, err)
		}

		if len(response.WorkflowRuns) == 0 {
			break
		}

		allRuns = append(allRuns, response.WorkflowRuns...)
		if len(response.WorkflowRuns) < 100 {
			break
		}

		page++
	}

	return allRuns, nil
}
