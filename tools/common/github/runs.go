package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// RunView executes `gh run view` and decodes the JSON response into out.
func RunView(ctx context.Context, runID string, fields []string, out any) error {
	args := []string{"run", "view", runID}
	if len(fields) > 0 {
		args = append(args, "--json", strings.Join(fields, ","))
	}

	output, err := commandOutput(ctx, defaultTimeout, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(output, out); err != nil {
		return fmt.Errorf("failed to parse gh run view response for %s: %w", runID, err)
	}
	return nil
}

// RunListOptions configures `gh run list`.
type RunListOptions struct {
	Branch   string
	Workflow string
	Event    string
	Status   string
	Created  string
	Limit    int
}

// RunList executes `gh run list` and decodes the JSON response into out.
func RunList(ctx context.Context, opts RunListOptions, fields []string, out any) error {
	args := []string{"run", "list"}
	if opts.Branch != "" {
		args = append(args, "--branch", opts.Branch)
	}
	if opts.Workflow != "" {
		args = append(args, "--workflow", opts.Workflow)
	}
	if opts.Event != "" {
		args = append(args, "--event", opts.Event)
	}
	if opts.Status != "" {
		args = append(args, "--status", opts.Status)
	}
	if opts.Created != "" {
		args = append(args, "--created", opts.Created)
	}
	if opts.Limit > 0 {
		args = append(args, "--limit", strconv.Itoa(opts.Limit))
	}
	if len(fields) > 0 {
		args = append(args, "--json", strings.Join(fields, ","))
	}

	output, err := commandOutput(ctx, defaultTimeout, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(output, out); err != nil {
		return fmt.Errorf("failed to parse gh run list response: %w", err)
	}
	return nil
}

// RunDownloadOptions configures `gh run download`.
type RunDownloadOptions struct {
	Pattern string
	Dir     string
}

// RunDownload executes `gh run download`.
func RunDownload(ctx context.Context, runID string, opts RunDownloadOptions) error {
	args := []string{"run", "download", runID}
	if opts.Pattern != "" {
		args = append(args, "--pattern", opts.Pattern)
	}
	if opts.Dir != "" {
		args = append(args, "--dir", opts.Dir)
	}

	return commandRun(ctx, defaultTimeout, args...)
}

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
		if err := getJSON(ctx, path, &response); err != nil {
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
