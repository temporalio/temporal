package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	apiURL          = "https://api.github.com"
	DefaultAPIRPS   = 10
	apiBurst        = 1
	maxAPIErrorBody = 4 << 10
)

var apiClient = &http.Client{Transport: func() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100
	transport.IdleConnTimeout = 90 * time.Second
	return transport
}()}

var apiLimiter = rate.NewLimiter(DefaultAPIRPS, apiBurst)

// SetAPIRPS sets the request rate limit for GitHub API requests.
func SetAPIRPS(rps int) error {
	if rps < 1 {
		return errors.New("GitHub API requests per second must be at least 1")
	}
	apiLimiter.SetLimit(rate.Limit(rps))
	return nil
}

func getJSON(ctx context.Context, path string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	response, err := get(ctx, path)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to parse GitHub response for %s: %w", path, err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	return nil
}

func get(ctx context.Context, path string) (*http.Response, error) {
	token, err := apiToken(ctx)
	if err != nil {
		return nil, err
	}
	if err := apiLimiter.Wait(ctx); err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GitHub request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	response, err := apiClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("GitHub request failed: %w", err)
	}
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return response, nil
	}

	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxAPIErrorBody))
	_ = response.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("failed to read GitHub error response: %w", readErr)
	}
	return nil, fmt.Errorf("GitHub returned %s: %s", response.Status, strings.TrimSpace(string(body)))
}
