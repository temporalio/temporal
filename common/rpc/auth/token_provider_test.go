package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileTokenProvider_ReadsTokenFile(t *testing.T) {
	t.Parallel()
	tokenFile := filepath.Join(t.TempDir(), "token.jwt")
	require.NoError(t, os.WriteFile(tokenFile, []byte("my-jwt-token"), 0600))

	provider := &FileTokenProvider{
		TokenFiles: map[string]string{"cluster-a": tokenFile},
	}
	token, err := provider.GetToken(context.Background(), "cluster-a")
	require.NoError(t, err)
	require.Equal(t, "my-jwt-token", token)
}

func TestFileTokenProvider_TrimsWhitespace(t *testing.T) {
	t.Parallel()
	tokenFile := filepath.Join(t.TempDir(), "token.jwt")
	require.NoError(t, os.WriteFile(tokenFile, []byte("  my-jwt-token\n\n"), 0600))

	provider := &FileTokenProvider{
		TokenFiles: map[string]string{"cluster-a": tokenFile},
	}
	token, err := provider.GetToken(context.Background(), "cluster-a")
	require.NoError(t, err)
	require.Equal(t, "my-jwt-token", token)
}

func TestFileTokenProvider_UnknownHostReturnsEmpty(t *testing.T) {
	t.Parallel()
	provider := &FileTokenProvider{
		TokenFiles: map[string]string{"cluster-a": "/some/path"},
	}
	token, err := provider.GetToken(context.Background(), "unknown-host")
	require.NoError(t, err)
	require.Empty(t, token)
}

func TestFileTokenProvider_MissingFileReturnsError(t *testing.T) {
	t.Parallel()
	provider := &FileTokenProvider{
		TokenFiles: map[string]string{"cluster-a": "/nonexistent/path/token"},
	}
	_, err := provider.GetToken(context.Background(), "cluster-a")
	require.ErrorContains(t, err, "failed to read token file")
}

func TestFileTokenProvider_ReReadsOnEachCall(t *testing.T) {
	t.Parallel()
	tokenFile := filepath.Join(t.TempDir(), "token.jwt")
	require.NoError(t, os.WriteFile(tokenFile, []byte("token-v1"), 0600))

	provider := &FileTokenProvider{
		TokenFiles: map[string]string{"cluster-a": tokenFile},
	}

	token, err := provider.GetToken(context.Background(), "cluster-a")
	require.NoError(t, err)
	require.Equal(t, "token-v1", token)

	require.NoError(t, os.WriteFile(tokenFile, []byte("token-v2"), 0600))

	token, err = provider.GetToken(context.Background(), "cluster-a")
	require.NoError(t, err)
	require.Equal(t, "token-v2", token)
}
