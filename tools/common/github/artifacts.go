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
	var artifacts []Artifact

	page := 1
	for {
		var response struct {
			Artifacts []Artifact `json:"artifacts"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs/%d/artifacts?per_page=100&page=%d", repo, runID, page)
		if err := getJSON(ctx, path, &response); err != nil {
			return nil, fmt.Errorf("failed to fetch artifacts page %d for run %d: %w", page, runID, err)
		}

		if len(response.Artifacts) == 0 {
			break
		}

		artifacts = append(artifacts, response.Artifacts...)
		if len(response.Artifacts) < 100 {
			break
		}

		page++
	}

	return artifacts, nil
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
