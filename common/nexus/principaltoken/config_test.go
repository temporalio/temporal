package principaltoken

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func privPEM(t *testing.T, k *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(k)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

func pkcs8PEM(t *testing.T, k *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(k)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func pubPEM(t *testing.T, k *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func TestConfig_EndToEnd_SEC1(t *testing.T) {
	key := newKey(t)
	// Cluster A signs; cluster B (this config) trusts A and also signs itself.
	cfg := Config{
		Issuer:        testIssuer,
		SigningKeyPEM: privPEM(t, key),
		SigningKeyID:  testKID,
		TrustedIssuers: map[string]map[string][]byte{
			testIssuer: {testKID: pubPEM(t, key)},
		},
	}
	bundle, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, bundle.Signer)
	verifier := NewVerifier(cfg, bundle.KeyProvider, nil)
	require.NotNil(t, verifier)

	eu := &commonpb.Principal{Type: "users", Name: "alice@example.com"}
	svc := &commonpb.Principal{Type: "service-accounts", Name: "worker"}
	tok, err := bundle.Signer.Sign(context.Background(), Content{ServiceCaller: svc, EndUser: eu})
	require.NoError(t, err)

	got, err := verifier.Verify(context.Background(), tok)
	require.NoError(t, err)
	require.True(t, eq(eu, got.EndUser))
	require.True(t, eq(svc, got.ServiceCaller))
}

func TestConfig_AcceptsPKCS8SigningKey(t *testing.T) {
	key := newKey(t)
	cfg := Config{
		Issuer:        testIssuer,
		SigningKeyPEM: pkcs8PEM(t, key),
		SigningKeyID:  testKID,
		TrustedIssuers: map[string]map[string][]byte{
			testIssuer: {testKID: pubPEM(t, key)},
		},
	}
	bundle, err := New(cfg)
	require.NoError(t, err)
	tok, err := bundle.Signer.Sign(context.Background(), Content{
		EndUser: &commonpb.Principal{Type: "users", Name: "a"},
	})
	require.NoError(t, err)
	_, err = NewVerifier(cfg, bundle.KeyProvider, nil).Verify(context.Background(), tok)
	require.NoError(t, err)
}

func TestConfig_EmptyIsFeatureOff(t *testing.T) {
	bundle, err := New(Config{})
	require.NoError(t, err)
	require.Nil(t, bundle.Signer, "no signing key => no signer")
	require.NotNil(t, bundle.KeyProvider, "key provider always present (serves empty JWKS)")
	require.Nil(t, NewVerifier(Config{}, bundle.KeyProvider, nil), "no trusted issuers => no verifier")
}

func TestConfig_SignOnlyAndVerifyOnly(t *testing.T) {
	key := newKey(t)

	signCfg := Config{Issuer: testIssuer, SigningKeyPEM: privPEM(t, key), SigningKeyID: testKID}
	signOnly, err := New(signCfg)
	require.NoError(t, err)
	require.NotNil(t, signOnly.Signer)
	require.Nil(t, NewVerifier(signCfg, signOnly.KeyProvider, nil))

	verifyCfg := Config{TrustedIssuers: map[string]map[string][]byte{testIssuer: {testKID: pubPEM(t, key)}}}
	verifyOnly, err := New(verifyCfg)
	require.NoError(t, err)
	require.Nil(t, verifyOnly.Signer)
	require.NotNil(t, NewVerifier(verifyCfg, verifyOnly.KeyProvider, nil))
}

func TestConfig_MalformedKeysError(t *testing.T) {
	_, err := New(Config{SigningKeyPEM: []byte("not a pem"), SigningKeyID: "k"})
	require.Error(t, err)

	_, err = New(Config{TrustedIssuers: map[string]map[string][]byte{"iss": {"k": []byte("nope")}}})
	require.Error(t, err)
}

func TestJWKSHandler_ServesSigningKey(t *testing.T) {
	key := newKey(t)
	bundle, err := New(Config{Issuer: testIssuer, SigningKeyPEM: privPEM(t, key), SigningKeyID: testKID})
	require.NoError(t, err)

	srv := httptest.NewServer(JWKSHandler(bundle.KeyProvider))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var doc struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			Use string `json:"use"`
		} `json:"keys"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
	require.Len(t, doc.Keys, 1)
	require.Equal(t, testKID, doc.Keys[0].Kid)
	require.Equal(t, "EC", doc.Keys[0].Kty)
	require.Equal(t, "sig", doc.Keys[0].Use)
}
