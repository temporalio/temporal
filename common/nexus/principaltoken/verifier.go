package principaltoken

import (
	"context"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// JWSVerifier is the OSS default Verifier: it validates the ES256 signature
// against the (issuer, kid) public key from the KeyProvider, then enforces
// expiry (with leeway) and issuer presence.
type JWSVerifier struct {
	keys   KeyProvider
	leeway time.Duration
	nowFn  func() time.Time
	parser *jwt.Parser
}

// JWSVerifierOptions configures a JWSVerifier.
type JWSVerifierOptions struct {
	// Keys resolves trusted public keys. Required.
	Keys KeyProvider
	// Leeway tolerates clock skew between caller and handler clusters when
	// checking exp/iat. Defaults to 30s.
	Leeway time.Duration
	// NowFn overrides the clock (tests). Defaults to time.Now.
	NowFn func() time.Time
}

// NewJWSVerifier constructs a JWSVerifier.
func NewJWSVerifier(opts JWSVerifierOptions) *JWSVerifier {
	leeway := opts.Leeway
	if leeway <= 0 {
		leeway = 30 * time.Second
	}
	nowFn := opts.NowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	return &JWSVerifier{
		keys:   opts.Keys,
		leeway: leeway,
		nowFn:  nowFn,
		// Restrict to the asymmetric algorithm and do claim validation
		// ourselves (golang-jwt/v4 has no leeway option).
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{signingAlg}),
			jwt.WithoutClaimsValidation(),
		),
	}
}

func (v *JWSVerifier) Verify(ctx context.Context, token string) (*Verified, error) {
	if token == "" {
		return nil, ErrNoToken
	}
	var claims tokenClaims
	keyFunc := func(t *jwt.Token) (any, error) {
		kid, ok := t.Header["kid"].(string)
		if !ok || kid == "" || claims.Issuer == "" {
			return nil, ErrVerification
		}
		key, err := v.keys.VerificationKey(ctx, claims.Issuer, kid)
		if err != nil {
			return nil, ErrVerification
		}
		return key, nil
	}

	if _, err := v.parser.ParseWithClaims(token, &claims, keyFunc); err != nil {
		// Collapse all parse/signature errors into the opaque sentinel.
		return nil, ErrVerification
	}

	now := v.nowFn()
	// Expiry is required and checked with leeway.
	if claims.ExpiresAt == nil || now.Add(-v.leeway).After(claims.ExpiresAt.Time) {
		return nil, ErrVerification
	}
	// iat, if present, must not be in the future beyond leeway.
	if claims.IssuedAt != nil && now.Add(v.leeway).Before(claims.IssuedAt.Time) {
		return nil, ErrVerification
	}

	return claims.toVerified(), nil
}
