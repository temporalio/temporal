package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Conclusion represents the conclusion status of a workflow run or job.
type Conclusion string

const (
	ConclusionSuccess Conclusion = "success"
	ConclusionFailure Conclusion = "failure"
)

// Run represents a GitHub Actions workflow run returned by the gh CLI.
type Run struct {
	DatabaseID         int64         `json:"databaseId"`
	Number             int           `json:"number"`
	Name               string        `json:"name"`
	WorkflowName       string        `json:"workflowName"`
	WorkflowDatabaseID int64         `json:"workflowDatabaseId"`
	Status             string        `json:"status"`
	Conclusion         Conclusion    `json:"conclusion"`
	HeadBranch         string        `json:"headBranch"`
	HeadSHA            string        `json:"headSha"`
	URL                string        `json:"url"`
	DisplayTitle       string        `json:"displayTitle"`
	Event              string        `json:"event"`
	CreatedAt          time.Time     `json:"createdAt"`
	StartedAt          time.Time     `json:"startedAt"`
	UpdatedAt          time.Time     `json:"updatedAt"`
	Duration           time.Duration `json:"-"`
	Jobs               []Job         `json:"jobs"`
}

// ShortSHA returns the first seven characters of the run's commit SHA.
func (r Run) ShortSHA() string {
	if len(r.HeadSHA) > 7 {
		return r.HeadSHA[:7]
	}
	return r.HeadSHA
}

type workflowRunResponse struct {
	ID         int64      `json:"id"`
	Number     int        `json:"run_number"`
	Name       string     `json:"name"`
	Status     string     `json:"status"`
	Conclusion Conclusion `json:"conclusion"`
	HeadBranch string     `json:"head_branch"`
	HeadSHA    string     `json:"head_sha"`
	URL        string     `json:"html_url"`
	Event      string     `json:"event"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	WorkflowID int64      `json:"workflow_id"`
}

// Job represents a single job in a GitHub Actions workflow run returned by the gh CLI.
type Job struct {
	Name        string     `json:"name"`
	Conclusion  Conclusion `json:"conclusion"`
	Status      string     `json:"status"`
	StartedAt   string     `json:"startedAt"`
	CompletedAt string     `json:"completedAt"`
	URL         string     `json:"url"`
}

// ViewRun retrieves workflow run details through the gh CLI.
func ViewRun(ctx context.Context, runID string) (Run, error) {
	fields := []string{
		"databaseId",
		"number",
		"conclusion",
		"name",
		"workflowName",
		"workflowDatabaseId",
		"status",
		"headBranch",
		"headSha",
		"url",
		"displayTitle",
		"event",
		"createdAt",
		"startedAt",
		"updatedAt",
		"jobs",
	}

	var run Run
	if err := runJSON(ctx, []string{"view", runID}, fields, &run); err != nil {
		return Run{}, fmt.Errorf("failed to get workflow run: %w", err)
	}
	return run, nil
}

func runJSON(ctx context.Context, args []string, fields []string, out any) error {
	cmdArgs := append([]string{"run"}, args...)
	if len(fields) > 0 {
		cmdArgs = append(cmdArgs, "--json", strings.Join(fields, ","))
	}

	output, err := commandOutput(ctx, defaultTimeout, cmdArgs...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(output, out); err != nil {
		return fmt.Errorf("failed to parse gh run response: %w", err)
	}
	return nil
}

// RunListOptions configures `gh run list`.
type RunListOptions struct {
	Repo     string
	Branch   string
	Workflow string
	Event    string
	Status   string
	Created  string
	Limit    int
	All      bool
}

// WorkflowRunListOptions configures paginated workflow-run listing.
type WorkflowRunListOptions struct {
	Repo       string
	WorkflowID int64
	Branch     string
	Created    string
}

// ListRuns retrieves workflow runs through the gh CLI.
func ListRuns(ctx context.Context, opts RunListOptions) ([]Run, error) {
	fields := []string{
		"databaseId",
		"number",
		"conclusion",
		"name",
		"workflowName",
		"workflowDatabaseId",
		"status",
		"event",
		"createdAt",
		"startedAt",
		"updatedAt",
		"headSha",
		"displayTitle",
		"url",
	}

	var runs []Run
	if err := runJSON(ctx, append([]string{"list"}, runListArgs(opts)...), fields, &runs); err != nil {
		return nil, fmt.Errorf("failed to list workflow runs: %w", err)
	}
	return runs, nil
}

// ListWorkflowRuns retrieves all workflow runs for a workflow through the gh CLI.
func ListWorkflowRuns(ctx context.Context, opts WorkflowRunListOptions) ([]Run, error) {
	var allRuns []Run

	page := 1
	for {
		var response struct {
			WorkflowRuns []workflowRunResponse `json:"workflow_runs"`
		}
		path := fmt.Sprintf(
			"/repos/%s/actions/workflows/%d/runs?branch=%s&created=%s&per_page=100&page=%d",
			opts.Repo,
			opts.WorkflowID,
			opts.Branch,
			opts.Created,
			page,
		)
		if err := getJSON(ctx, path, &response); err != nil {
			return nil, fmt.Errorf("failed to fetch workflow runs page %d: %w", page, err)
		}

		if len(response.WorkflowRuns) == 0 {
			break
		}

		for _, run := range response.WorkflowRuns {
			allRuns = append(allRuns, run.toRun())
		}
		if len(response.WorkflowRuns) < 100 {
			break
		}

		page++
	}

	return allRuns, nil
}

func (r workflowRunResponse) toRun() Run {
	return Run{
		DatabaseID:         r.ID,
		Number:             r.Number,
		Name:               r.Name,
		WorkflowName:       r.Name,
		WorkflowDatabaseID: r.WorkflowID,
		Status:             r.Status,
		Conclusion:         r.Conclusion,
		HeadBranch:         r.HeadBranch,
		HeadSHA:            r.HeadSHA,
		URL:                r.URL,
		Event:              r.Event,
		CreatedAt:          r.CreatedAt,
		UpdatedAt:          r.UpdatedAt,
	}
}

func runListArgs(opts RunListOptions) []string {
	var args []string
	if opts.Repo != "" {
		args = append(args, "--repo", opts.Repo)
	}
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
	if opts.All {
		args = append(args, "--all")
	}
	return args
}

// RunDownloadOptions configures `gh run download`.
type RunDownloadOptions struct {
	Repo    string
	Pattern string
	Dir     string
}

// RunDownload executes `gh run download`.
func RunDownload(ctx context.Context, runID string, opts RunDownloadOptions) error {
	args := []string{"run", "download", runID}
	if opts.Repo != "" {
		args = append(args, "--repo", opts.Repo)
	}
	if opts.Pattern != "" {
		args = append(args, "--pattern", opts.Pattern)
	}
	if opts.Dir != "" {
		args = append(args, "--dir", opts.Dir)
	}

	return commandRun(ctx, defaultTimeout, args...)
}
