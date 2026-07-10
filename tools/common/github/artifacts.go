package github

import (
	"context"
	"fmt"
	"os"
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
	var response struct {
		Artifacts []Artifact `json:"artifacts"`
	}
	if err := API(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d/artifacts?per_page=100", repo, runID), &response); err != nil {
		return nil, err
	}

	return response.Artifacts, nil
}

// DownloadArtifact downloads a single GitHub Actions artifact zip file.
func DownloadArtifact(ctx context.Context, repo string, artifactID int64, outputDir string) (string, error) {
	zipPath := filepath.Join(outputDir, fmt.Sprintf("artifact-%d.zip", artifactID))

	output, err := commandOutput(ctx, 60*time.Second, "api", fmt.Sprintf("/repos/%s/actions/artifacts/%d/zip", repo, artifactID))
	if err != nil {
		return "", fmt.Errorf("failed to download artifact %d: %w", artifactID, err)
	}

	if err := os.WriteFile(zipPath, output, 0644); err != nil {
		return "", fmt.Errorf("failed to write artifact zip %d: %w", artifactID, err)
	}

	return zipPath, nil
}
