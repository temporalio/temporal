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
