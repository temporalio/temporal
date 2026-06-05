package principaltoken

import (
	"context"
	"crypto"
	"encoding/json"

	jose "github.com/go-jose/go-jose/v4"
)

// KeyProvider resolves public keys for verification and publishes this
// cluster's own public keys for distribution. It deliberately does NOT expose
// the signing private key: minting is the Signer's concern, so a Cloud
// KMS-backed Signer never has to hand a raw key through this interface.
//
// Verification keys are resolved by (issuer, kid): issuer identifies the
// minting cluster, kid the specific key (enables rotation and multi-key).
type KeyProvider interface {
	// VerificationKey returns the public key a token from issuer signed with
	// kid should be verified against, or an error if no such trusted key.
	VerificationKey(ctx context.Context, issuer, kid string) (crypto.PublicKey, error)
	// PublicJWKS returns this cluster's published public keys as a JWKS
	// document, for serving at a JWKS endpoint so peers can verify our tokens.
	PublicJWKS(ctx context.Context) ([]byte, error)
}

// StaticKeyProvider is the OSS default: trusted issuer keys and this cluster's
// own published keys are supplied at construction (loaded from config). It has
// no network dependency, so it works in air-gapped on-prem deployments where a
// peer JWKS endpoint may be unreachable.
type StaticKeyProvider struct {
	// trusted maps issuer -> kid -> public key, used for verification.
	trusted map[string]map[string]crypto.PublicKey
	// own maps kid -> public key for this cluster, published via PublicJWKS.
	own map[string]crypto.PublicKey
}

// NewStaticKeyProvider builds a provider from a trusted-issuer table and this
// cluster's own public keys (by kid). Either map may be nil/empty: an empty
// trusted table means "verify nothing" (safe default); an empty own map means
// "publish an empty JWKS".
func NewStaticKeyProvider(
	trusted map[string]map[string]crypto.PublicKey,
	own map[string]crypto.PublicKey,
) *StaticKeyProvider {
	return &StaticKeyProvider{trusted: trusted, own: own}
}

func (p *StaticKeyProvider) VerificationKey(_ context.Context, issuer, kid string) (crypto.PublicKey, error) {
	byKid, ok := p.trusted[issuer]
	if !ok {
		return nil, ErrVerification
	}
	key, ok := byKid[kid]
	if !ok {
		return nil, ErrVerification
	}
	return key, nil
}

func (p *StaticKeyProvider) PublicJWKS(_ context.Context) ([]byte, error) {
	set := jose.JSONWebKeySet{}
	for kid, pub := range p.own {
		set.Keys = append(set.Keys, jose.JSONWebKey{
			Key:       pub,
			KeyID:     kid,
			Algorithm: signingAlg,
			Use:       "sig",
		})
	}
	return json.Marshal(set)
}
