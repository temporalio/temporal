package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	DefaultAPIRPS   = 10
	apiBurst        = 1
	maxAPIErrorBody = 4 << 10
	maxAPIRetries   = 1
)

var apiURL = "https://api.github.com"

var apiClient = &http.Client{Transport: func() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100
	transport.IdleConnTimeout = 90 * time.Second
	return transport
}()}

var apiLimiter = rate.NewLimiter(DefaultAPIRPS, apiBurst)

// RateLimitError indicates GitHub continued rate limiting after the requested retry.
type RateLimitError struct {
	Status         string
	RetryAfter     time.Duration
	RetryAttempted bool
	Err            error
}

func (e *RateLimitError) Error() string {
	var message string
	if !e.RetryAttempted && e.RetryAfter == 0 {
		message = fmt.Sprintf("GitHub API rate limit response %s did not include a valid Retry-After header", e.Status)
	} else {
		message = fmt.Sprintf("GitHub API rate limit persisted after retrying in %s: %s", e.RetryAfter, e.Status)
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", message, e.Err)
	}
	return message
}

func (e *RateLimitError) Unwrap() error {
	return e.Err
}

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
	var rateLimitStatus string
	var retryDelay time.Duration
	for attempt := 0; attempt <= maxAPIRetries; attempt++ {
		response, err := getResponse(ctx, path, token)
		if err != nil {
			if attempt > 0 {
				return nil, retryFailure(rateLimitStatus, retryDelay, fmt.Errorf("GitHub retry request failed: %w", err))
			}
			return nil, err
		}
		if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
			return response, nil
		}

		if !isRateLimitResponse(response) {
			return nil, responseError(response)
		}
		retryAfter, ok := retryAfter(response)
		if !ok {
			return nil, &RateLimitError{Status: response.Status, Err: responseError(response)}
		}
		if attempt == maxAPIRetries {
			return nil, &RateLimitError{Status: response.Status, RetryAfter: retryAfter, RetryAttempted: true, Err: responseError(response)}
		}

		rateLimitStatus = response.Status
		retryDelay = retryAfter
		_ = response.Body.Close()
		fmt.Printf("GitHub API rate limited (%s); retrying request in %s\n", response.Status, retryAfter)
		if err := waitForRetry(ctx, retryAfter); err != nil {
			return nil, retryFailure(response.Status, retryAfter, fmt.Errorf("failed while waiting to retry GitHub request: %w", err))
		}
	}
	return nil, errors.New("unreachable GitHub request retry state")
}

func getResponse(ctx context.Context, path, token string) (*http.Response, error) {
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
	return response, nil
}

func retryFailure(status string, retryAfter time.Duration, err error) error {
	return &RateLimitError{Status: status, RetryAfter: retryAfter, RetryAttempted: true, Err: err}
}

func retryAfter(response *http.Response) (time.Duration, bool) {
	value := response.Header.Get("Retry-After")
	seconds, err := strconv.Atoi(value)
	if err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		return max(time.Until(retryAt), 0), true
	}
	return 0, false
}

func isRateLimitResponse(response *http.Response) bool {
	return response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusTooManyRequests
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func responseError(response *http.Response) error {
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxAPIErrorBody))
	_ = response.Body.Close()
	if readErr != nil {
		return fmt.Errorf("failed to read GitHub error response: %w", readErr)
	}
	return fmt.Errorf("GitHub returned %s: %s", response.Status, strings.TrimSpace(string(body)))
}
