package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"golang.org/x/time/rate"
)

const (
	githubAPIURL    = "https://api.github.com"
	DefaultAPIRPS   = 10
	githubAPIBurst  = 1
	maxAPIErrorBody = 4 << 10
)

var githubAPILimiter = rate.NewLimiter(DefaultAPIRPS, githubAPIBurst)

// SetAPIRPS sets the request rate limit for GitHub API requests.
func SetAPIRPS(rps int) error {
	if rps < 1 {
		return errors.New("GitHub API requests per second must be at least 1")
	}
	githubAPILimiter.SetLimit(rate.Limit(rps))
	return nil
}

func getJSON(ctx context.Context, path string, out any) (retErr error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	response, err := get(ctx, path)
	if err != nil {
		return err
	}
	defer func() {
		if err := response.Body.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close GitHub response for %s: %w", path, err)
		}
	}()
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to parse GitHub response for %s: %w", path, err)
	}
	return nil
}

func get(ctx context.Context, path string) (*http.Response, error) {
	token, err := apiToken(ctx)
	if err != nil {
		return nil, err
	}
	if err := githubAPILimiter.Wait(ctx); err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, githubAPIURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GitHub request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("GitHub request failed: %w", err)
	}
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return response, nil
	}

	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxAPIErrorBody))
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("failed to read GitHub error response: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("failed to close GitHub error response: %w", closeErr)
	}
	return nil, fmt.Errorf("GitHub returned %s: %s", response.Status, strings.TrimSpace(string(body)))
}
