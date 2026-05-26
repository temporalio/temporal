package auth

import (
	"context"

	"google.golang.org/grpc/credentials"
)

// TokenCredentials is a gRPC PerRPCCredentials that attaches a bearer token
// produced by fetchToken to each outbound RPC. It does no caching; the provider
// behind fetchToken owns the lifecycle (rotation, expiry-aware refresh,
// stampede protection). RequireTransportSecurity returns true per RFC 9700.
type TokenCredentials struct {
	headerName string
	fetchToken func(context.Context) (string, error)
}

var _ credentials.PerRPCCredentials = (*TokenCredentials)(nil)

func NewTokenCredentials(
	headerName string,
	fetchToken func(context.Context) (string, error),
) *TokenCredentials {
	return &TokenCredentials{
		headerName: headerName,
		fetchToken: fetchToken,
	}
}

func (c *TokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := c.fetchToken(ctx)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, nil
	}
	return map[string]string{
		c.headerName: "Bearer " + token,
	}, nil
}

// RequireTransportSecurity returns true so the bearer never attaches to plaintext dials, per RFC 9700.
func (c *TokenCredentials) RequireTransportSecurity() bool {
	return true
}
