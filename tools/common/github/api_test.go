package github

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestGetJSONRetriesAfterRateLimit(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempts++
		if attempts == 1 {
			writer.Header().Set("Retry-After", "0")
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = writer.Write([]byte(`{}`))
	}))
	defer server.Close()

	restoreAPIClient(t, server.URL, server.Client())
	t.Setenv("GH_TOKEN", "test-token")

	var response map[string]any
	require.NoError(t, getJSON(context.Background(), "/test", &response))
	require.Equal(t, 2, attempts)
}

func TestGetJSONFailsAfterRateLimitRetry(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempts++
		writer.Header().Set("Retry-After", "0")
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	restoreAPIClient(t, server.URL, server.Client())
	t.Setenv("GH_TOKEN", "test-token")

	err := getJSON(context.Background(), "/test", &map[string]any{})
	var rateLimitErr *RateLimitError
	require.ErrorAs(t, err, &rateLimitErr)
	require.Equal(t, "429 Too Many Requests", rateLimitErr.Status)
	require.Equal(t, 2, attempts)
}

func TestGetJSONFailsWhenRateLimitRetryRequestFails(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Status:     "429 Too Many Requests",
				Header:     http.Header{"Retry-After": []string{"0"}},
				Body:       io.NopCloser(strings.NewReader("rate limited")),
			}, nil
		}
		return nil, errors.New("connection reset")
	})}

	restoreAPIClient(t, "https://api.example.test", client)
	t.Setenv("GH_TOKEN", "test-token")

	err := getJSON(context.Background(), "/test", &map[string]any{})
	var rateLimitErr *RateLimitError
	require.ErrorAs(t, err, &rateLimitErr)
	require.ErrorContains(t, rateLimitErr, "GitHub retry request failed")
	require.Equal(t, 2, attempts)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func restoreAPIClient(t *testing.T, url string, client *http.Client) {
	t.Helper()
	originalURL := apiURL
	originalClient := apiClient
	originalLimiter := apiLimiter
	apiURL = url
	apiClient = client
	apiLimiter = rate.NewLimiter(rate.Inf, 1)
	t.Cleanup(func() {
		apiURL = originalURL
		apiClient = originalClient
		apiLimiter = originalLimiter
	})
}
