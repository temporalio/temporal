package auth

import (
	"context"
)

// TokenProvider supplies auth tokens for outbound remote-cluster RPCs.
// rpcAddress is the receiver's host:port. Implementations are responsible for
// their own caching: GetToken is called on every outbound RPC, so a provider
// that incurs IdP round-trips must cache internally. A nil TokenProvider means
// no auth header is sent; downstream consumers (RPCFactory) handle this case
// directly.
type TokenProvider interface {
	GetToken(ctx context.Context, rpcAddress string) (token string, err error)
}
