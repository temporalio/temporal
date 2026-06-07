package principaltoken

import (
	"context"

	"github.com/golang-jwt/jwt/v4"
)

// PeerTrustFunc reports whether the inbound connection is a trusted server peer
// (e.g. cell-to-cell mTLS).
type PeerTrustFunc func(ctx context.Context) bool

// TransportVerifier trusts a token's claims because the connection is trusted,
// not because it is signed — it reads claims unverified, gated on PeerTrustFunc.
// Not the OSS default (Cloud selects it via config); must never accept tokens
// from an unauthenticated peer.
type TransportVerifier struct {
	trusted PeerTrustFunc
	parser  *jwt.Parser
}

// NewTransportVerifier constructs a TransportVerifier. trusted is required.
func NewTransportVerifier(trusted PeerTrustFunc) *TransportVerifier {
	return &TransportVerifier{
		trusted: trusted,
		parser:  jwt.NewParser(jwt.WithoutClaimsValidation()),
	}
}

func (v *TransportVerifier) Verify(ctx context.Context, token string) (*Verified, error) {
	if token == "" {
		return nil, ErrNoToken
	}
	// Channel trust is the gate. If the peer is not trusted we refuse outright,
	// regardless of token contents.
	if v.trusted == nil || !v.trusted(ctx) {
		return nil, ErrVerification
	}
	var claims tokenClaims
	if _, _, err := v.parser.ParseUnverified(token, &claims); err != nil {
		return nil, ErrVerification
	}
	return claims.toVerified(), nil
}
