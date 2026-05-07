package auth

import (
	"context"
	"time"
)

// TokenProvider supplies auth tokens for outbound remote-cluster RPCs.
// rpcAddress is the receiver's host:port. expiresAt drives cache refresh;
// zero means never expires. A nil TokenProvider means no auth header is sent;
// downstream consumers (RPCFactory) handle this case directly.
type TokenProvider interface {
	GetToken(ctx context.Context, rpcAddress string) (token string, expiresAt time.Time, err error)
}
