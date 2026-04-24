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

// fetchWorkflowRuns retrieves all completed workflow runs within a date range.
// since is the oldest bound (inclusive); until is the newest bound (zero means open-ended).
// Implements proper pagination to fix the 100-run limit bug.
func fetchWorkflowRuns(ctx context.Context, repo string, workflowID int64, branch string, since, until time.Time) ([]WorkflowRun, error) {
	var allRuns []WorkflowRun

	createdFilter := ">=" + since.Format("2006-01-02")
	if !until.IsZero() {
		createdFilter = since.Format("2006-01-02") + ".." + until.Format("2006-01-02")
	}
	fmt.Printf("Fetching workflow runs created %s...\n", createdFilter)

	page := 1
	for {
		ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)

		cmd := exec.CommandContext(ctxTimeout, "gh", "api",
			fmt.Sprintf("/repos/%s/actions/workflows/%d/runs?branch=%s&created=%s&per_page=100&page=%d",
				repo, workflowID, branch, createdFilter, page),
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

// parseArtifactName extracts run_id, job_id, and matrix_name from artifact name.
// Functional tests: junit-xml--{run_id}--{job_id}--{run_attempt}--{matrix_name}--{display_name}--functional-test
// Unit/integration:  junit-xml--{run_id}--{job_id}--{run_attempt}--unit-test
// Returns: runID, jobID, matrixName ("unknown" for fields that are absent or unparseable)
func parseArtifactName(artifactName string) (runID string, jobID string, matrixName string) {
	parts := strings.Split(artifactName, "--")
	if len(parts) < 3 {
		return "unknown", "unknown", "unknown"
	}

	runID = parts[1]

	jobID = parts[2]
	if jobID == "" {
		jobID = "unknown"
	}

	// Functional test artifacts carry a matrix name (DB config) at parts[4].
	// Unit/integration artifacts have only 5 parts where parts[4] is the test type
	// (e.g. "unit-test"), not a matrix name. Functional artifacts have >=7 parts.
	if len(parts) >= 6 {
		matrixName = parts[4]
	} else {
		matrixName = "unknown"
	}

	return runID, jobID, matrixName
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

// fetchCommitMeta fetches commit title, author, and changed file list for a single commit SHA.
// Uses: GET /repos/{owner}/{repo}/commits/{sha}
func fetchCommitMeta(ctx context.Context, repo, sha string) (CommitMeta, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctxTimeout, "gh", "api",
		fmt.Sprintf("/repos/%s/commits/%s", repo, sha),
	)

	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return CommitMeta{SHA: sha}, fmt.Errorf("gh api failed for commit %s: %w\nstderr: %s", sha, err, string(exitErr.Stderr))
		}
		return CommitMeta{SHA: sha}, fmt.Errorf("failed to execute gh command for commit %s: %w", sha, err)
	}

	var response struct {
		SHA    string `json:"sha"`
		Commit struct {
			Message string `json:"message"`
			Author  struct {
				Name string    `json:"name"`
				Date time.Time `json:"date"`
			} `json:"author"`
		} `json:"commit"`
		Files []struct {
			Filename string `json:"filename"`
		} `json:"files"`
	}

	if err := json.Unmarshal(output, &response); err != nil {
		return CommitMeta{SHA: sha}, fmt.Errorf("failed to parse commit response for %s: %w", sha, err)
	}

	// Extract just the first line of the commit message as the title
	title := response.Commit.Message
	if idx := strings.IndexByte(title, '\n'); idx >= 0 {
		title = title[:idx]
	}

	files := make([]string, 0, len(response.Files))
	for _, f := range response.Files {
		files = append(files, f.Filename)
	}

	return CommitMeta{
		SHA:         sha,
		Title:       title,
		Author:      response.Commit.Author.Name,
		CommittedAt: response.Commit.Author.Date,
		Files:       files,
	}, nil
}
