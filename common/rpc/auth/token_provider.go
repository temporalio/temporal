package auth

import (
	"context"
)

// TokenProvider supplies auth tokens for outbound remote-cluster gRPC connections.
// Implementations fetch tokens from an external source (Auth0, Vault, file, etc.).
// rpcAddress is the network-addressable resource indicator (host:port) of the receiver,
// matching the OAuth 2.0 RFC 8707 "resource" parameter shape.
type TokenProvider interface {
	GetToken(ctx context.Context, rpcAddress string) (string, error)
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
