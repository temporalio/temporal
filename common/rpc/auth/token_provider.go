package auth

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// TokenProvider supplies auth tokens for remote cluster gRPC connections.
// Implementations fetch tokens from an external source (Auth0, Vault, file, etc.).
// The hostname identifies which remote cluster the token is for.
type TokenProvider interface {
	GetToken(ctx context.Context, hostname string) (string, error)
}

// FileTokenProvider reads tokens from files on disk.
// Map keys are hostnames; values are file paths.
type FileTokenProvider struct {
	TokenFiles map[string]string
}

func (p *FileTokenProvider) GetToken(_ context.Context, hostname string) (string, error) {
	path, ok := p.TokenFiles[hostname]
	if !ok {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read token file %q for host %q: %w", path, hostname, err)
	}
	return strings.TrimSpace(string(data)), nil
}
