package github

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const artifactDownloadTimeout = 60 * time.Second

// Artifact represents a downloadable GitHub Actions artifact.
type Artifact struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	Expired   bool      `json:"expired"`
}

// ListRunArtifacts retrieves artifacts for a GitHub Actions workflow run.
func ListRunArtifacts(ctx context.Context, repo string, githubActionsRunID int64) ([]Artifact, error) {
	var artifacts []Artifact

	page := 1
	for {
		var response struct {
			Artifacts []Artifact `json:"artifacts"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs/%d/artifacts?per_page=100&page=%d", repo, githubActionsRunID, page)
		if err := getJSON(ctx, path, &response); err != nil {
			return nil, fmt.Errorf("failed to fetch artifacts page %d for GitHub Actions run %d: %w", page, githubActionsRunID, err)
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
	path := fmt.Sprintf("/repos/%s/actions/artifacts/%d/zip", repo, artifactID)
	downloadCtx, cancel := context.WithTimeout(ctx, artifactDownloadTimeout)
	defer cancel()
	response, err := get(downloadCtx, path)
	if err != nil {
		return "", fmt.Errorf("failed to download artifact %d: %w", artifactID, err)
	}
	defer func() { _ = response.Body.Close() }()

	tempFile, err := os.CreateTemp(outputDir, fmt.Sprintf("artifact-%d-*.zip", artifactID))
	if err != nil {
		return "", fmt.Errorf("failed to create artifact %d file: %w", artifactID, err)
	}
	tempPath := tempFile.Name()
	defer func() { _ = os.Remove(tempPath) }()

	if _, err := io.Copy(tempFile, response.Body); err != nil {
		_ = tempFile.Close()
		return "", fmt.Errorf("failed to write artifact %d: %w", artifactID, err)
	}
	if err := tempFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close artifact %d file: %w", artifactID, err)
	}

	zipPath := filepath.Join(outputDir, fmt.Sprintf("artifact-%d.zip", artifactID))
	if err := os.Rename(tempPath, zipPath); err != nil {
		return "", fmt.Errorf("failed to finalize artifact %d: %w", artifactID, err)
	}

	return zipPath, nil
}
