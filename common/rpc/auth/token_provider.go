package auth

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// TokenProvider supplies auth tokens for remote cluster gRPC connections.
// Implementations fetch tokens from an external source (Auth0, Vault, file, etc.).
// An empty clusterName (e.g. when probing an unregistered cluster) should return an empty token.
type TokenProvider interface {
	GetToken(ctx context.Context, clusterName string) (string, error)
}

// FileTokenProvider reads tokens from files on disk. Map keys are remote cluster names; values are file paths.
// For local testing only — production deployments should plug in a TokenProvider backed by their secret manager.
type FileTokenProvider struct {
	TokenFiles map[string]string
}

func (p *FileTokenProvider) GetToken(_ context.Context, clusterName string) (string, error) {
	if clusterName == "" {
		return "", nil
	}
	path, ok := p.TokenFiles[clusterName]
	if !ok {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read token file %q for cluster %q: %w", path, clusterName, err)
	}
	return strings.TrimSpace(string(data)), nil
}
