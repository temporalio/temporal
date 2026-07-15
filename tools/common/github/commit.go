package github

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Commit holds changed-file info fetched from the GitHub API.
type Commit struct {
	SHA    string        `json:"sha"`
	Commit CommitDetails `json:"commit"`
	Files  []CommitFile  `json:"files"`
}

// CommitDetails holds the commit metadata nested in the GitHub API response.
type CommitDetails struct {
	Message string       `json:"message"`
	Author  CommitAuthor `json:"author"`
}

// CommitAuthor holds the commit author nested in the GitHub API response.
type CommitAuthor struct {
	Name string    `json:"name"`
	Date time.Time `json:"date"`
}

// CommitFile holds a changed file in the GitHub API response.
type CommitFile struct {
	Filename string `json:"filename"`
}

// Title returns the first line of the commit message.
func (c Commit) Title() string {
	title, _, _ := strings.Cut(c.Commit.Message, "\n")
	return title
}

// Filenames returns the names of the files changed by the commit.
func (c Commit) Filenames() []string {
	filenames := make([]string, 0, len(c.Files))
	for _, file := range c.Files {
		filenames = append(filenames, file.Filename)
	}
	return filenames
}

// GetCommit fetches commit metadata from the GitHub API.
// Uses: GET /repos/{owner}/{repo}/commits/{sha}
func GetCommit(ctx context.Context, repo, sha string) (Commit, error) {
	commit := Commit{SHA: sha}

	if err := getJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s", repo, sha), &commit); err != nil {
		return commit, err
	}

	if commit.SHA == "" {
		commit.SHA = sha
	}
	return commit, nil
}
