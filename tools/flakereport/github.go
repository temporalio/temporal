package flakereport

import (
	"context"
	"fmt"
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

// parseArtifactName extracts run_id, job_id, and matrix_name from artifact name.
// Functional tests: junit-xml--{run_id}--{job_id}--{run_attempt}--{matrix_name}--{display_name}--functional-test
// Unit/integration:  junit-xml--{run_id}--{job_id}--{run_attempt}--unit-test
// Returns: runID, jobID, matrixName ("unknown" for fields that are absent or unparseable)
func parseArtifactName(artifactName string) (string, string, string) {
	parsed, _ := github.ParseArtifactName(artifactName)
	if parsed.Type == "" {
		return "unknown", "unknown", "unknown"
	}

	jobID := parsed.JobID
	if jobID == "" {
		jobID = "unknown"
	}

	// Functional test artifacts carry a matrix name (DB config) at the start of the suffix.
	// Unit/integration artifacts have only the test type (e.g. "unit-test") in the suffix.
	matrixName := "unknown"
	suffix := strings.Split(parsed.NameSuffix, "--")
	if len(suffix) >= 2 {
		matrixName = suffix[0]
	}

	return parsed.RunID, jobID, matrixName
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
