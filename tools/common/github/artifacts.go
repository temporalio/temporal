package github

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Artifact represents a downloadable GitHub Actions artifact.
type Artifact struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	Expired   bool      `json:"expired"`
}

// ListRunArtifacts retrieves artifacts for a GitHub Actions workflow run.
func ListRunArtifacts(ctx context.Context, repo string, runID int64) ([]Artifact, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctxTimeout, "gh", "api",
		fmt.Sprintf("/repos/%s/actions/runs/%d/artifacts?per_page=100", repo, runID),
	)

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("gh api failed for run %d: %w\nstderr: %s", runID, err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("failed to execute gh command for run %d: %w", runID, err)
	}

	var response struct {
		Artifacts []Artifact `json:"artifacts"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		return nil, fmt.Errorf("failed to parse artifacts response for run %d: %w", runID, err)
	}

	return response.Artifacts, nil
}

// DownloadArtifact downloads a single GitHub Actions artifact zip file.
func DownloadArtifact(ctx context.Context, repo string, artifactID int64, outputDir string) (string, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	zipPath := filepath.Join(outputDir, fmt.Sprintf("artifact-%d.zip", artifactID))

	cmd := exec.CommandContext(ctxTimeout, "gh", "api",
		fmt.Sprintf("/repos/%s/actions/artifacts/%d/zip", repo, artifactID),
	)

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("failed to download artifact %d: %w\nstderr: %s", artifactID, err, string(exitErr.Stderr))
		}
		return "", fmt.Errorf("failed to download artifact %d: %w", artifactID, err)
	}

	if err := os.WriteFile(zipPath, output, 0644); err != nil {
		return "", fmt.Errorf("failed to write artifact zip %d: %w", artifactID, err)
	}

	return zipPath, nil
}
