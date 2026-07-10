package github

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const defaultTimeout = 30 * time.Second

// API executes `gh api` and decodes the JSON response into out.
func API(ctx context.Context, path string, out any) error {
	output, err := commandOutput(ctx, defaultTimeout, "api", path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(output, out); err != nil {
		return fmt.Errorf("failed to parse gh api response for %s: %w", path, err)
	}
	return nil
}

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

func commandOutput(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctxTimeout, "gh", args...)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("gh %s failed: %w\nstderr: %s", strings.Join(args, " "), err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("failed to execute gh %s: %w", strings.Join(args, " "), err)
	}
	return output, nil
}
