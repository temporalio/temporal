package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var fallbackToken struct {
	sync.Once
	value string
	err   error
}

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
	path := fmt.Sprintf("/repos/%s/actions/artifacts/%d/zip", repo, artifactID)
	return downloadArtifact(ctx, defaultAPIClient, path, artifactID, outputDir)
}

func apiToken(ctx context.Context) (string, error) {
	if token := os.Getenv("GH_TOKEN"); token != "" {
		return token, nil
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		return token, nil
	}

	fallbackToken.Do(func() {
		output, err := commandOutput(ctx, defaultTimeout, "auth", "token")
		fallbackToken.value = strings.TrimSpace(string(output))
		fallbackToken.err = err
	})
	if fallbackToken.err != nil {
		return "", fmt.Errorf("failed to get GitHub token: %w", fallbackToken.err)
	}
	if fallbackToken.value == "" {
		return "", fmt.Errorf("GitHub token is empty")
	}
	return fallbackToken.value, nil
}

func downloadArtifact(
	ctx context.Context,
	client *apiClient,
	path string,
	artifactID int64,
	outputDir string,
) (_ string, retErr error) {
	resp, err := client.get(ctx, path)
	if err != nil {
		return "", fmt.Errorf("failed to download artifact %d: %w", artifactID, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close artifact %d response: %w", artifactID, err)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return "", fmt.Errorf("failed to download artifact %d: GitHub returned %s: %s", artifactID, resp.Status, strings.TrimSpace(string(body)))
	}

	tempFile, err := os.CreateTemp(outputDir, fmt.Sprintf("artifact-%d-*.zip", artifactID))
	if err != nil {
		return "", fmt.Errorf("failed to create artifact %d file: %w", artifactID, err)
	}
	tempPath := tempFile.Name()
	defer func() { _ = os.Remove(tempPath) }()

	if _, err := io.Copy(tempFile, resp.Body); err != nil {
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
