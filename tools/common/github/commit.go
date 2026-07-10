package github

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Commit holds changed-file info fetched from the GitHub API.
type Commit struct {
	SHA         string
	Title       string
	Author      string
	CommittedAt time.Time
	Files       []string
}

// GetCommit fetches commit metadata from the GitHub API.
// Uses: GET /repos/{owner}/{repo}/commits/{sha}
func GetCommit(ctx context.Context, repo, sha string) (Commit, error) {
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

	if err := getJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s", repo, sha), &response); err != nil {
		return Commit{SHA: sha}, err
	}

	responseSHA := response.SHA
	if responseSHA == "" {
		responseSHA = sha
	}

	title := response.Commit.Message
	if idx := strings.IndexByte(title, '\n'); idx >= 0 {
		title = title[:idx]
	}

	files := make([]string, 0, len(response.Files))
	for _, f := range response.Files {
		files = append(files, f.Filename)
	}

	return Commit{
		SHA:         responseSHA,
		Title:       title,
		Author:      response.Commit.Author.Name,
		CommittedAt: response.Commit.Author.Date,
		Files:       files,
	}, nil
}
