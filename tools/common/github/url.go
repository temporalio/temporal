package github

import "fmt"

// CommitURL constructs a GitHub commit URL.
func CommitURL(repo, sha string) string {
	return fmt.Sprintf("https://github.com/%s/commit/%s", repo, sha)
}

// RunURL constructs a GitHub Actions workflow run URL.
func RunURL(repo, runID string) string {
	return fmt.Sprintf("https://github.com/%s/actions/runs/%s", repo, runID)
}

// JobURL constructs a GitHub Actions job URL.
func JobURL(repo, runID, jobID string) string {
	return fmt.Sprintf("%s/job/%s", RunURL(repo, runID), jobID)
}
