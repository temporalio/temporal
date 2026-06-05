package principaltoken

import (
	"context"
	"crypto/ecdsa"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// ECDSASigner is the OSS default Signer: it mints ES256-signed tokens from a
// single in-process ECDSA private key. Cloud may replace it with a KMS-backed
// Signer; nothing else in the codebase depends on the concrete type.
type ECDSASigner struct {
	key    *ecdsa.PrivateKey
	kid    string
	issuer string
	ttl    time.Duration
	nowFn  func() time.Time
}

// ECDSASignerOptions configures an ECDSASigner.
type ECDSASignerOptions struct {
	// Key is the ECDSA private key used to sign. Required.
	Key *ecdsa.PrivateKey
	// KID identifies the key in the token header so verifiers can select the
	// matching public key (and so keys can be rotated). Required.
	KID string
	// Issuer identifies this minting cluster; verifiers resolve keys by it.
	// Required.
	Issuer string
	// TTL is the token lifetime. Short by design (covers the hop RPC, not the
	// async operation); re-mint on retry. Defaults to 60s if zero.
	TTL time.Duration
	// NowFn overrides the clock (tests). Defaults to time.Now.
	NowFn func() time.Time
}

// NewECDSASigner constructs an ECDSASigner. Returns ErrNoSigningKey if no key
// is supplied (lets call sites treat "feature off" uniformly).
func NewECDSASigner(opts ECDSASignerOptions) (*ECDSASigner, error) {
	if opts.Key == nil {
		return nil, ErrNoSigningKey
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	nowFn := opts.NowFn
	if nowFn == nil {
		nowFn = time.Now
	}
	return &ECDSASigner{
		key:    opts.Key,
		kid:    opts.KID,
		issuer: opts.Issuer,
		ttl:    ttl,
		nowFn:  nowFn,
	}, nil
}

func (s *ECDSASigner) Sign(_ context.Context, content Content) (string, error) {
	now := s.nowFn()
	claims := tokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   displayString(content.EndUser),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
		},
		EndUser:         toPrincipalClaim(content.EndUser, content.EndUserResolvedName),
		ServiceCaller:   toPrincipalClaim(content.ServiceCaller, content.ServiceCallerResolvedName),
		NamespaceCaller: toPrincipalClaim(content.NamespaceCaller, ""),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = s.kid
	return token.SignedString(s.key)
}
