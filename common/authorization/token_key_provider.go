package authorization

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"

	"github.com/golang-jwt/jwt/v4"
)

// @@@SNIPSTART temporal-common-authorization-tokenkeyprovider-interface
// Provides keys for validating JWT tokens
type TokenKeyProvider interface {
	EcdsaKey(alg string, kid string) (*ecdsa.PublicKey, error)
	HmacKey(alg string, kid string) ([]byte, error)
	RsaKey(alg string, kid string) (*rsa.PublicKey, error)
	SupportedMethods() []string
	Close()
}

// RawTokenKeyProvider is a TokenKeyProvider that provides keys for validating JWT tokens
type RawTokenKeyProvider interface {
	GetKey(ctx context.Context, token *jwt.Token) (interface{}, error)
	SupportedMethods() []string
	Close()
}

// @@@SNIPEND
