package principaltoken

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"
)

// TrustMode selects how the handler establishes trust in a propagated token.
type TrustMode string

const (
	// TrustModeSignature (the default) verifies a JWS signature against a
	// trusted issuer's public key. Topology-independent; needs key distribution.
	TrustModeSignature TrustMode = "signature"
	// TrustModeTransport trusts the token's claims because the *connection* is
	// trusted (a sibling cell behind mutual TLS), without checking a signature.
	// The Cloud P0 path: no key infrastructure, reuses the cell-to-cell mTLS
	// trust the broker already runs.
	TrustModeTransport TrustMode = "transport"
)

// Config is the operator-facing configuration that materializes a signer and
// verifier from PEM key material and a trust mode. Keys are supplied as PEM
// bytes so they can come from a file, secret, or env var without this package
// knowing the source.
//
// Verification is gated by TrustMode:
//   - signature (default): a Verifier is built only if TrustedIssuers is set.
//   - transport: a Verifier is built that trusts the connection peer (the
//     PeerTrustFunc, supplied separately by the host).
//
// An empty Config is the safe default (feature off: no Signer, no Verifier).
type Config struct {
	// Issuer identifies this cluster as the "iss" in minted tokens.
	Issuer string
	// SigningKeyPEM is the PEM-encoded EC private key used to mint tokens
	// (SEC1 "EC PRIVATE KEY" or PKCS#8 "PRIVATE KEY"). Empty → no Signer.
	SigningKeyPEM []byte
	// SigningKeyID is the "kid" advertised in minted tokens and in the JWKS.
	SigningKeyID string
	// TTL is the minted-token lifetime (default 60s if zero).
	TTL time.Duration
	// Leeway tolerates clock skew on verification (default 30s if zero).
	Leeway time.Duration
	// TrustMode selects the verifier; defaults to signature.
	TrustMode TrustMode
	// TrustedIssuers maps issuer -> kid -> PEM-encoded EC public key, used by
	// the signature verifier. Ignored in transport mode.
	TrustedIssuers map[string]map[string][]byte
}

// Bundle is the materialized signing side of a Config: the Signer (nil when no
// signing key is configured) and the KeyProvider (always non-nil; serves this
// cluster's public JWKS, possibly empty). The Verifier is built separately by
// the host (it needs the trust mode and, for transport mode, a PeerTrustFunc).
type Bundle struct {
	Signer      Signer
	KeyProvider KeyProvider
}

// New builds a Bundle from cfg. Returns an error only on malformed key material.
func New(cfg Config) (*Bundle, error) {
	own := map[string]crypto.PublicKey{}

	var signer Signer
	if len(cfg.SigningKeyPEM) > 0 {
		priv, err := ParseECPrivateKeyPEM(cfg.SigningKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("principaltoken: signing key: %w", err)
		}
		s, err := NewECDSASigner(ECDSASignerOptions{
			Key: priv, KID: cfg.SigningKeyID, Issuer: cfg.Issuer, TTL: cfg.TTL,
		})
		if err != nil {
			return nil, err
		}
		signer = s
		own[cfg.SigningKeyID] = priv.Public()
	}

	trusted := map[string]map[string]crypto.PublicKey{}
	for issuer, byKID := range cfg.TrustedIssuers {
		m := map[string]crypto.PublicKey{}
		for kid, pemBytes := range byKID {
			pub, err := ParseECPublicKeyPEM(pemBytes)
			if err != nil {
				return nil, fmt.Errorf("principaltoken: trusted key %s/%s: %w", issuer, kid, err)
			}
			m[kid] = pub
		}
		trusted[issuer] = m
	}

	return &Bundle{Signer: signer, KeyProvider: NewStaticKeyProvider(trusted, own)}, nil
}

// NewVerifier builds the verifier selected by cfg.TrustMode, or nil when the
// feature is off for verification (signature mode with no trusted issuers).
// peerTrust is required for transport mode.
func NewVerifier(cfg Config, keys KeyProvider, peerTrust PeerTrustFunc) Verifier {
	if cfg.TrustMode == TrustModeTransport {
		return NewTransportVerifier(peerTrust)
	}
	if len(cfg.TrustedIssuers) > 0 {
		return NewJWSVerifier(JWSVerifierOptions{Keys: keys, Leeway: cfg.Leeway})
	}
	return nil
}

// ParseECPrivateKeyPEM decodes a PEM-encoded EC private key (SEC1 or PKCS#8).
func ParseECPrivateKeyPEM(pemBytes []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if ec, ok := key.(*ecdsa.PrivateKey); ok {
			return ec, nil
		}
		return nil, errors.New("PKCS#8 key is not an EC private key")
	}
	return nil, errors.New("not a valid EC private key (SEC1 or PKCS#8)")
}

// ParseECPublicKeyPEM decodes a PEM-encoded EC public key (PKIX/SPKI).
func ParseECPublicKeyPEM(pemBytes []byte) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ec, ok := pub.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("PEM key is not an EC public key")
	}
	return ec, nil
}
