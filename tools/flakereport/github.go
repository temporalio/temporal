package flakereport

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// fetchWorkflowRuns retrieves all completed workflow runs within date range
// Implements proper pagination to fix the 100-run limit bug
func fetchWorkflowRuns(ctx context.Context, repo string, workflowID int64, branch string, sinceDays int) ([]WorkflowRun, error) {
	var allRuns []WorkflowRun
	sinceDate := time.Now().AddDate(0, 0, -sinceDays).Format("2006-01-02")

	fmt.Printf("Fetching workflow runs since %s...\n", sinceDate)

	page := 1
	for {
		ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)

		cmd := exec.CommandContext(ctxTimeout, "gh", "api",
			fmt.Sprintf("/repos/%s/actions/workflows/%d/runs?branch=%s&created=>=%s&per_page=100&page=%d",
				repo, workflowID, branch, sinceDate, page),
		)

		output, err := cmd.Output()
		cancel() // Cancel context immediately after command completes

		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				return nil, fmt.Errorf("gh api failed (page %d): %w\nstderr: %s", page, err, string(exitErr.Stderr))
			}
			return nil, fmt.Errorf("failed to execute gh command (page %d): %w", page, err)
		}

		var response struct {
			WorkflowRuns []WorkflowRun `json:"workflow_runs"`
		}

		if err := json.Unmarshal(output, &response); err != nil {
			return nil, fmt.Errorf("failed to parse workflow runs response (page %d): %w", page, err)
		}

		if len(response.WorkflowRuns) == 0 {
			break
		}

		allRuns = append(allRuns, response.WorkflowRuns...)
		fmt.Printf("Fetched page %d: %d runs (total: %d)\n", page, len(response.WorkflowRuns), len(allRuns))

		// If we got fewer than 100 results, this is the last page
		if len(response.WorkflowRuns) < 100 {
			break
		}

		page++
	}

	fmt.Printf("Total workflow runs fetched: %d\n", len(allRuns))
	return allRuns, nil
}

// fetchRunArtifacts retrieves all artifacts for a specific workflow run
func fetchRunArtifacts(ctx context.Context, repo string, runID int64) ([]WorkflowArtifact, error) {
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

	var response ArtifactsResponse
	if err := json.Unmarshal(output, &response); err != nil {
		return nil, fmt.Errorf("failed to parse artifacts response for run %d: %w", runID, err)
	}

	// Filter for JUnit/test artifacts
	var testArtifacts []WorkflowArtifact
	for _, artifact := range response.Artifacts {
		if artifact.Expired {
			continue
		}
		name := strings.ToLower(artifact.Name)
		if strings.Contains(name, "junit") {
			testArtifacts = append(testArtifacts, artifact)
		}
	}

	return testArtifacts, nil
}

// downloadArtifact downloads a single artifact zip file
func downloadArtifact(ctx context.Context, repo string, artifactID int64, outputDir string) (string, error) {
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

// extractArtifactZip extracts zip file and returns paths to JUnit XML files
func extractArtifactZip(zipPath, outputDir string) ([]string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open zip file %s: %w", zipPath, err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			fmt.Printf("Warning: Failed to close zip reader: %v\n", err)
		}
	}()

	var xmlFiles []string

	for _, f := range r.File {
		// Skip directories
		if f.FileInfo().IsDir() {
			continue
		}

		// Only extract XML files
		if !strings.HasSuffix(strings.ToLower(f.Name), ".xml") {
			continue
		}

		// Create extraction path
		extractPath := filepath.Join(outputDir, filepath.Base(f.Name))

		// Open file from zip
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s in zip: %w", f.Name, err)
		}

		// Create output file
		outFile, err := os.Create(extractPath)
		if err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("failed to create output file %s: %w", extractPath, err)
		}

		// Copy content
		_, err = io.Copy(outFile, rc)
		if closeErr := rc.Close(); closeErr != nil {
			_ = outFile.Close()
			return nil, fmt.Errorf("failed to close zip file reader: %w", closeErr)
		}
		if closeErr := outFile.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to close output file: %w", closeErr)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to extract file %s: %w", f.Name, err)
		}

		xmlFiles = append(xmlFiles, extractPath)
	}

	return xmlFiles, nil
}

// parseArtifactName extracts run_id and job_id from artifact name
// Format: {prefix}--{run_id}--{job_id}--{suffix}
// Returns: runID, jobID (or "unknown" if not parseable)
func parseArtifactName(artifactName string) (runID string, jobID string) {
	parts := strings.Split(artifactName, "--")
	if len(parts) >= 3 {
		runID = parts[1]
		jobID = parts[2]
		if jobID == "" {
			jobID = "unknown"
		}
		return runID, jobID
	}
	return "unknown", "unknown"
}

// buildGitHubURL constructs GitHub Actions URL from run/job IDs
// If jobID == "unknown": https://github.com/{repo}/actions/runs/{runID}
// Otherwise: https://github.com/{repo}/actions/runs/{runID}/job/{jobID}
func buildGitHubURL(repo, runID, jobID string) string {
	baseURL := fmt.Sprintf("https://github.com/%s/actions/runs/%s", repo, runID)
	if jobID != "unknown" && jobID != "" {
		return fmt.Sprintf("%s/job/%s", baseURL, jobID)
	}
	return baseURL
}
