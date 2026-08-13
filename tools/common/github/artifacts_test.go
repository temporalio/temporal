package github

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadArtifact(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if got := request.URL.Path; got != "/artifact" {
			t.Errorf("request path = %q, want %q", got, "/artifact")
		}
		_, err := io.WriteString(w, "zip contents")
		if err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	path, err := downloadArtifact(context.Background(), newTestAPIClient(server), "/artifact", 42, t.TempDir())
	require.NoError(t, err)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "zip contents", string(contents))
}
