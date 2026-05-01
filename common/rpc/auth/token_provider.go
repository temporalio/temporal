package auth

import (
	"context"
)

// TokenProvider supplies auth tokens for outbound remote-cluster gRPC connections.
// Implementations fetch tokens from an external source (Auth0, Vault, file, etc.).
// An empty clusterName (e.g. when probing an unregistered cluster) should return an empty token.
type TokenProvider interface {
	GetToken(ctx context.Context, clusterName string) (string, error)
}

// No-op token provider that supplies no auth header.
type noopTokenProvider struct{}

var _ TokenProvider = (*noopTokenProvider)(nil)

func NewNoopTokenProvider() TokenProvider {
	return &noopTokenProvider{}
}

func (*noopTokenProvider) GetToken(context.Context, string) (string, error) {
	return "", nil
}

func IsNoopTokenProvider(p TokenProvider) bool {
	_, ok := p.(*noopTokenProvider)
	return ok
}
