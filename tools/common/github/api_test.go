package github

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func writeAPIResponse(w http.ResponseWriter, body string, result chan<- error) {
	_, err := io.WriteString(w, body)
	result <- err
}

func newTestAPIClient(server *httptest.Server) *apiClient {
	return &apiClient{
		baseURL:     server.URL,
		httpClient:  server.Client(),
		limiter:     rate.NewLimiter(rate.Inf, 1),
		token:       func(context.Context) (string, error) { return "test-token", nil },
		wait:        waitWithContext,
		now:         func() time.Time { return time.Unix(100, 0) },
		jitter:      func(time.Duration) time.Duration { return 0 },
		maxAttempts: githubAPIMaxAttempts,
	}
}

func TestSetAPIRPS(t *testing.T) {
	originalRPS := defaultAPIClient.limiter.Limit()
	t.Cleanup(func() {
		defaultAPIClient.limiter.SetLimit(originalRPS)
	})

	require.NoError(t, SetAPIRPS(25))
	require.Equal(t, rate.Limit(25), defaultAPIClient.limiter.Limit())

	require.ErrorContains(t, SetAPIRPS(0), "must be at least 1")
	require.Equal(t, rate.Limit(25), defaultAPIClient.limiter.Limit())
}

func TestAPIClientHonorsRetryAfter(t *testing.T) {
	requests := 0
	writeResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Retry-After", "7")
			http.Error(w, "secondary rate limit", http.StatusTooManyRequests)
			return
		}
		writeAPIResponse(w, `{"value":"ok"}`, writeResult)
	}))
	defer server.Close()

	client := newTestAPIClient(server)
	var delays []time.Duration
	client.wait = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}
	var response struct {
		Value string `json:"value"`
	}
	require.NoError(t, client.getJSON(context.Background(), "/resource", &response))
	require.Equal(t, "ok", response.Value)
	require.NoError(t, <-writeResult)
	require.Equal(t, 2, requests)
	require.Equal(t, []time.Duration{7 * time.Second}, delays)
}

func TestAPIClientHonorsRateLimitReset(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{
		"X-Ratelimit-Remaining": []string{"0"},
		"X-Ratelimit-Reset":     []string{"112"},
	}}
	client := &apiClient{
		now:    func() time.Time { return time.Unix(100, 0) },
		jitter: func(time.Duration) time.Duration { return 0 },
	}
	delay, retry := client.retryDelay(resp, nil, 0)
	require.True(t, retry)
	require.Equal(t, 12*time.Second, delay)
}

func TestAPIClientSecondaryLimitFallback(t *testing.T) {
	resp := &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header)}
	client := &apiClient{
		now:    time.Now,
		jitter: func(time.Duration) time.Duration { return 0 },
	}
	delay, retry := client.retryDelay(resp, []byte("secondary rate limit"), 0)
	require.True(t, retry)
	require.Equal(t, time.Minute, delay)
}

func TestAPIClientRetriesTransientFailures(t *testing.T) {
	requests := 0
	writeResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests < 3 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		writeAPIResponse(w, `{}`, writeResult)
	}))
	defer server.Close()

	client := newTestAPIClient(server)
	var delays []time.Duration
	client.wait = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}
	require.NoError(t, client.getJSON(context.Background(), "/resource", &struct{}{}))
	require.NoError(t, <-writeResult)
	require.Equal(t, 3, requests)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, delays)
}

func TestAPIClientDoesNotRetryNonRateLimitClientError(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	client := newTestAPIClient(server)
	_, err := client.get(context.Background(), "/resource")
	require.ErrorContains(t, err, "404 Not Found")
	require.Equal(t, 1, requests)
}

func TestAPIClientLimiterBoundsRequests(t *testing.T) {
	requests := 0
	writeResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		writeAPIResponse(w, `{}`, writeResult)
	}))
	defer server.Close()

	client := newTestAPIClient(server)
	client.limiter = rate.NewLimiter(0, 1)
	resp, err := client.get(context.Background(), "/first")
	require.NoError(t, err)
	require.NoError(t, <-writeResult)
	require.NoError(t, resp.Body.Close())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.get(ctx, "/second")
	require.Error(t, err)
	require.Equal(t, 1, requests)
}

func TestAPIClientRequestSlotsAreCancelable(t *testing.T) {
	client := &apiClient{requestSlots: make(chan struct{}, 1)}
	client.requestSlots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, client.acquireRequestSlot(ctx), context.Canceled)
}

func TestAPIClientSharesPrimaryRateLimitCooldown(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client := newTestAPIClient(server)
	client.now = func() time.Time { return time.Unix(100, 0) }
	var delays []time.Duration
	client.wait = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}

	client.observePrimaryRateLimit(&http.Response{Header: http.Header{
		"X-Ratelimit-Remaining": []string{"0"},
		"X-Ratelimit-Reset":     []string{"112"},
	}})
	require.NoError(t, client.waitForAdmission(context.Background()))
	require.Equal(t, []time.Duration{12 * time.Second}, delays)
}

func TestAPIClientRetryWaitIsCancelable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "secondary rate limit", http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := newTestAPIClient(server)
	client.wait = func(context.Context, time.Duration) error { return context.Canceled }
	_, err := client.get(context.Background(), "/resource")
	require.ErrorIs(t, err, context.Canceled)
}
