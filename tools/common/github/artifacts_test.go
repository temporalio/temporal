package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadArtifact(t *testing.T) {
	const contents = "zip contents"
	type requestResult struct {
		authorization string
		accept        string
		writeErr      error
	}
	requests := make(chan requestResult, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(contents))
		requests <- requestResult{
			authorization: r.Header.Get("Authorization"),
			accept:        r.Header.Get("Accept"),
			writeErr:      err,
		}
	}))
	defer server.Close()

	path, err := downloadArtifact(context.Background(), newTestAPIClient(server), "", 123, t.TempDir())
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, contents, string(data))
	request := <-requests
	require.NoError(t, request.writeErr)
	require.Equal(t, "Bearer test-token", request.authorization)
	require.Equal(t, "application/vnd.github+json", request.accept)
}

func TestDownloadArtifactHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := newTestAPIClient(server)
	client.maxAttempts = 1
	_, err := downloadArtifact(context.Background(), client, "", 123, t.TempDir())
	require.ErrorContains(t, err, "429 Too Many Requests")
	require.ErrorContains(t, err, "rate limited")
}

func TestDownloadArtifactCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	_, err := downloadArtifact(ctx, newTestAPIClient(server), "", 123, t.TempDir())
	require.ErrorIs(t, err, context.Canceled)
}
