package flakereport

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
)

// fetchWorkflowRuns retrieves all completed workflow runs within a date range.
// since is the oldest bound (inclusive); until is the newest bound (zero means open-ended).
// Implements proper pagination to fix the 100-run limit bug.
func fetchWorkflowRuns(ctx context.Context, repo string, workflowID int64, branch string, since, until time.Time) ([]github.Run, error) {
	createdFilter := ">=" + since.Format("2006-01-02")
	if !until.IsZero() {
		createdFilter = since.Format("2006-01-02") + ".." + until.Format("2006-01-02")
	}
	fmt.Printf("Fetching workflow runs created %s...\n", createdFilter)

	allRuns, err := github.ListRuns(ctx, github.RunListOptions{
		Repo:       repo,
		WorkflowID: workflowID,
		Branch:     branch,
		Created:    createdFilter,
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("Total workflow runs fetched: %d\n", len(allRuns))
	return allRuns, nil
}

// fetchRunArtifacts retrieves all artifacts for a specific workflow run
func fetchRunArtifacts(ctx context.Context, repo string, runID int64) ([]github.Artifact, error) {
	artifacts, err := github.ListRunArtifacts(ctx, repo, runID)
	if err != nil {
		return nil, err
	}

	// Filter for JUnit/test artifacts
	var testArtifacts []github.Artifact
	for _, artifact := range artifacts {
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
	if jobID != "unknown" && jobID != "" {
		return github.JobURL(repo, runID, jobID)
	}
	return github.RunURL(repo, runID)
}
