package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	githubAPIURL         = "https://api.github.com"
	DefaultAPIRPS        = 5
	githubAPIBurst       = 10
	githubAPIConcurrency = 20
	githubAPIMaxAttempts = 3
	maxAPIErrorBody      = 4 << 10
)

type apiClient struct {
	baseURL       string
	httpClient    *http.Client
	limiter       *rate.Limiter
	token         func(context.Context) (string, error)
	wait          func(context.Context, time.Duration) error
	now           func() time.Time
	jitter        func(time.Duration) time.Duration
	maxAttempts   int
	requestSlots  chan struct{}
	cooldownMu    sync.Mutex
	cooldownUntil time.Time
}

var defaultAPIClient = newAPIClient()

func newAPIClient() *apiClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &apiClient{
		baseURL:    githubAPIURL,
		httpClient: &http.Client{Transport: transport},
		limiter:    rate.NewLimiter(DefaultAPIRPS, githubAPIBurst),
		token:      apiToken,
		wait:       waitWithContext,
		now:        time.Now,
		jitter: func(max time.Duration) time.Duration {
			if max <= 0 {
				return 0
			}
			return time.Duration(rand.Int64N(int64(max)))
		},
		maxAttempts:  githubAPIMaxAttempts,
		requestSlots: make(chan struct{}, githubAPIConcurrency),
	}
}

// SetAPIRPS sets the request rate limit for GitHub API requests.
func SetAPIRPS(rps int) error {
	if rps < 1 {
		return fmt.Errorf("GitHub API requests per second must be at least 1")
	}
	defaultAPIClient.limiter.SetLimit(rate.Limit(rps))
	return nil
}

func (c *apiClient) getJSON(ctx context.Context, path string, out any) (retErr error) {
	resp, err := c.get(ctx, path)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("failed to close GitHub response for %s: %w", path, err)
		}
	}()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to parse GitHub response for %s: %w", path, err)
	}
	return nil
}

func (c *apiClient) get(ctx context.Context, path string) (*http.Response, error) {
	token, err := c.token(ctx)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		if err := c.waitForAdmission(ctx); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create GitHub request: %w", err)
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

		if err := c.acquireRequestSlot(ctx); err != nil {
			return nil, err
		}
		resp, err := c.httpClient.Do(req)
		c.releaseRequestSlot()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = fmt.Errorf("GitHub request failed: %w", err)
			if attempt+1 == c.maxAttempts {
				break
			}
			if err := c.wait(ctx, c.transientDelay(attempt)); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			c.observePrimaryRateLimit(resp)
			return resp, nil
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxAPIErrorBody))
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("failed to read GitHub error response: %w", readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("failed to close GitHub error response: %w", closeErr)
		}
		lastErr = fmt.Errorf("GitHub returned %s: %s", resp.Status, strings.TrimSpace(string(body)))

		delay, retry := c.retryDelay(resp, body, attempt)
		rateLimited := resp.StatusCode == http.StatusTooManyRequests || isRateLimitResponse(resp, body)
		if retry && rateLimited {
			c.extendCooldown(delay)
		}
		if !retry || attempt+1 == c.maxAttempts {
			break
		}
		if !rateLimited {
			if err := c.wait(ctx, delay); err != nil {
				return nil, err
			}
		}
	}

	return nil, lastErr
}

func (c *apiClient) acquireRequestSlot(ctx context.Context) error {
	if c.requestSlots == nil {
		return nil
	}
	select {
	case c.requestSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *apiClient) releaseRequestSlot() {
	if c.requestSlots != nil {
		<-c.requestSlots
	}
}

func (c *apiClient) waitForAdmission(ctx context.Context) error {
	c.cooldownMu.Lock()
	delay := c.cooldownUntil.Sub(c.now())
	c.cooldownMu.Unlock()
	if delay > 0 {
		if err := c.wait(ctx, delay); err != nil {
			return err
		}
	}
	return c.limiter.Wait(ctx)
}

func (c *apiClient) extendCooldown(delay time.Duration) {
	until := c.now().Add(delay)
	c.cooldownMu.Lock()
	if until.After(c.cooldownUntil) {
		c.cooldownUntil = until
	}
	c.cooldownMu.Unlock()
}

func (c *apiClient) observePrimaryRateLimit(resp *http.Response) {
	if resp.Header.Get("X-RateLimit-Remaining") != "0" {
		return
	}
	reset, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return
	}
	delay := time.Unix(reset, 0).Sub(c.now())
	if delay > 0 {
		c.extendCooldown(delay + c.jitter(time.Second))
	}
}

func (c *apiClient) retryDelay(resp *http.Response, body []byte, attempt int) (time.Duration, bool) {
	if resp.StatusCode == http.StatusTooManyRequests || isRateLimitResponse(resp, body) {
		if delay, ok := retryAfterDelay(resp.Header.Get("Retry-After"), c.now()); ok {
			return delay + c.jitter(time.Second), true
		}
		if resp.Header.Get("X-RateLimit-Remaining") == "0" {
			if reset, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil {
				delay := time.Unix(reset, 0).Sub(c.now())
				if delay < 0 {
					delay = 0
				}
				return delay + c.jitter(time.Second), true
			}
		}
		return time.Minute + c.jitter(time.Second), true
	}

	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return c.transientDelay(attempt), true
	default:
		return 0, false
	}
}

func (c *apiClient) transientDelay(attempt int) time.Duration {
	base := time.Second << attempt
	return base + c.jitter(base/2)
}

func isRateLimitResponse(resp *http.Response, body []byte) bool {
	if resp.StatusCode != http.StatusForbidden {
		return false
	}
	if resp.Header.Get("Retry-After") != "" || resp.Header.Get("X-RateLimit-Remaining") == "0" {
		return true
	}
	message := strings.ToLower(string(body))
	return strings.Contains(message, "secondary rate limit") ||
		strings.Contains(message, "rate limit exceeded") ||
		strings.Contains(message, "abuse detection")
}

func retryAfterDelay(value string, now time.Time) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return max(time.Duration(seconds)*time.Second, 0), true
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		return max(retryAt.Sub(now), 0), true
	}
	return 0, false
}

func waitWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
