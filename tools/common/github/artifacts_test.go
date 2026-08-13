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
		require.Equal(t, "/artifact", request.URL.Path)
		_, err := io.WriteString(w, "zip contents")
		require.NoError(t, err)
	}))
	defer server.Close()

	path, err := downloadArtifact(context.Background(), newTestAPIClient(server), "/artifact", 42, t.TempDir())
	require.NoError(t, err)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "zip contents", string(contents))
}
