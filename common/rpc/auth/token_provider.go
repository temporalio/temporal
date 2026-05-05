package auth

import (
	"context"
	"time"
)

// TokenProvider supplies auth tokens for outbound remote-cluster RPCs.
// rpcAddress is the receiver's host:port. expiresAt drives cache refresh;
// zero means never expires.
type TokenProvider interface {
	GetToken(ctx context.Context, rpcAddress string) (token string, expiresAt time.Time, err error)
}

// noopTokenProvider supplies no auth header.
type noopTokenProvider struct{}

var _ TokenProvider = (*noopTokenProvider)(nil)

func NewNoopTokenProvider() TokenProvider {
	return &noopTokenProvider{}
}

func (*noopTokenProvider) GetToken(context.Context, string) (string, time.Time, error) {
	return "", time.Time{}, nil
}

func IsNoopTokenProvider(p TokenProvider) bool {
	_, ok := p.(*noopTokenProvider)
	return ok
}
