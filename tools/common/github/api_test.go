package github

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func newTestAPIClient(server *httptest.Server) *apiClient {
	return &apiClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		limiter:    rate.NewLimiter(rate.Inf, 1),
		token:      func(context.Context) (string, error) { return "test-token", nil },
	}
}

func TestAPIClientGetJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer test-token")
		}
		if got := request.Header.Get("Accept"); got != "application/vnd.github+json" {
			t.Errorf("Accept = %q, want %q", got, "application/vnd.github+json")
		}
		_, err := io.WriteString(w, `{"value":"ok"}`)
		if err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	var response struct {
		Value string `json:"value"`
	}
	require.NoError(t, newTestAPIClient(server).getJSON(context.Background(), "/resource", &response))
	require.Equal(t, "ok", response.Value)
}

func TestAPIClientGetReturnsResponseError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	_, err := newTestAPIClient(server).get(context.Background(), "/resource")
	require.ErrorContains(t, err, "404 Not Found")
}

func TestSetAPIRPS(t *testing.T) {
	originalRPS := defaultAPIClient.limiter.Limit()
	t.Cleanup(func() {
		defaultAPIClient.limiter.SetLimit(originalRPS)
	})

	require.NoError(t, SetAPIRPS(15))
	require.InDelta(t, 15, float64(defaultAPIClient.limiter.Limit()), 0)
	require.ErrorContains(t, SetAPIRPS(0), "must be at least 1")
}
