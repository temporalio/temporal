package authorization

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
)

func TestDefaultTokenKeyProviderJWT(t *testing.T) {
	t.Parallel()
	edKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	otherEdKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, ed25519.SeedSize))
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		method     jwt.SigningMethod
		signingKey any
		keyID      any
		audience   string
		expired    bool
		noAlg      bool
		wantError  string
	}{
		{name: "EdDSA", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: "ed", audience: "temporal"},
		{name: "RSA", method: jwt.SigningMethodRS256, signingKey: rsaKey, keyID: "rsa", audience: "temporal"},
		{name: "ECDSA", method: jwt.SigningMethodES256, signingKey: ecKey, keyID: "ec", audience: "temporal"},
		{name: "missing kid", method: jwt.SigningMethodEdDSA, signingKey: edKey, wantError: "no \"kid\" header"},
		{name: "non-string kid", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: 123, wantError: "no \"kid\" header"},
		{name: "unknown kid", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: "unknown", wantError: "Ed25519 key not found"},
		{name: "wrong key type", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: "rsa", wantError: "Ed25519 key not found"},
		{name: "invalid signature", method: jwt.SigningMethodEdDSA, signingKey: otherEdKey, keyID: "ed", wantError: "verification error"},
		{name: "expired", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: "ed", expired: true, wantError: "expired"},
		{name: "wrong audience", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: "ed", audience: "other", wantError: "audience mismatch"},
		{name: "missing audience", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: "ed", wantError: "audience mismatch"},
		{name: "missing algorithm", method: jwt.SigningMethodEdDSA, signingKey: edKey, keyID: "ed", noAlg: true, wantError: "signing method"},
		{name: "HMAC confusion", method: jwt.SigningMethodHS256, signingKey: []byte(edKey.Public().(ed25519.PublicKey)), keyID: "ed", wantError: "signing method HS256 is invalid"},
		{name: "unsupported RSA algorithm", method: jwt.SigningMethodRS512, signingKey: rsaKey, keyID: "rsa", wantError: "signing method RS512 is invalid"},
		{name: "unsigned token", method: jwt.SigningMethodNone, signingKey: jwt.UnsafeAllowNoneSignatureType, keyID: "ed", wantError: "signing method none is invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "jwks.json")
			writeJWTTestKeys(t, path,
				jose.JSONWebKey{Key: edKey.Public(), KeyID: "ed", Algorithm: "EdDSA", Use: "sig"},
				jose.JSONWebKey{Key: &rsaKey.PublicKey, KeyID: "rsa", Algorithm: "RS256", Use: "sig"},
				jose.JSONWebKey{Key: &ecKey.PublicKey, KeyID: "ec", Algorithm: "ES256", Use: "sig"},
			)
			cfg := &config.Authorization{JWTKeyProvider: config.JWTKeyProvider{KeySourceURIs: []string{"file://" + path}}}
			provider := NewDefaultTokenKeyProvider(cfg, log.NewNoopLogger())
			t.Cleanup(provider.Close)
			mapper := NewDefaultJWTClaimMapper(provider, cfg, log.NewNoopLogger())
			claims := jwt.MapClaims{
				"sub":         "user",
				"permissions": []string{primitives.SystemLocalNamespace + ":admin", "default:read", "default:worker"},
			}
			if tc.audience != "" {
				claims["aud"] = tc.audience
			}
			if tc.expired {
				claims["exp"] = 1
			}
			token := jwt.NewWithClaims(tc.method, claims)
			if tc.keyID != nil {
				token.Header["kid"] = tc.keyID
			}
			if tc.noAlg {
				delete(token.Header, "alg")
			}
			signed, err := token.SignedString(tc.signingKey)
			require.NoError(t, err)
			got, err := mapper.GetClaims(&AuthInfo{AuthToken: "Bearer " + signed, Audience: "temporal"})
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &Claims{
				Subject:    "user",
				AuthType:   "jwt",
				System:     RoleAdmin,
				Namespaces: map[string]Role{"default": RoleReader | RoleWorker},
			}, got)
		})
	}
}

func TestDefaultTokenKeyProviderEdDSARefresh(t *testing.T) {
	t.Parallel()
	oldKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	newKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, ed25519.SeedSize))
	path := filepath.Join(t.TempDir(), "jwks.json")
	writeJWTTestKeys(t, path, jose.JSONWebKey{Key: oldKey.Public(), KeyID: "ed", Algorithm: "EdDSA"})
	provider := NewDefaultTokenKeyProvider(&config.Authorization{
		JWTKeyProvider: config.JWTKeyProvider{KeySourceURIs: []string{"file://" + path}},
	}, log.NewNoopLogger())
	t.Cleanup(provider.Close)

	sign := func(key ed25519.PrivateKey) string {
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"sub": "user"})
		token.Header["kid"] = "ed"
		signed, err := token.SignedString(key)
		require.NoError(t, err)
		return signed
	}
	oldToken, newToken := sign(oldKey), sign(newKey)
	_, err := parseJWT(oldToken, provider)
	require.NoError(t, err)

	writeJWTTestKeys(t, path, jose.JSONWebKey{Key: newKey.Public(), KeyID: "ed", Algorithm: "EdDSA"})
	require.NoError(t, provider.updateKeys())
	_, err = parseJWT(oldToken, provider)
	require.ErrorContains(t, err, "verification error")
	_, err = parseJWT(newToken, provider)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte(`{"keys":`), 0600))
	require.Error(t, provider.updateKeys())
	_, err = parseJWT(newToken, provider)
	require.NoError(t, err, "failed refresh must retain the last usable keys")

	writeJWTTestKeys(t, path)
	require.NoError(t, provider.updateKeys())
	_, err = parseJWT(newToken, provider)
	require.ErrorContains(t, err, "Ed25519 key not found")
}

func writeJWTTestKeys(t *testing.T, path string, keys ...jose.JSONWebKey) {
	t.Helper()
	data, err := json.Marshal(jose.JSONWebKeySet{Keys: keys})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
}
