package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type dynamicRootCATestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newDynamicRootCATestCA(t *testing.T, commonName string) *dynamicRootCATestCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &dynamicRootCATestCA{cert: cert, key: key, pool: pool}
}

func (ca *dynamicRootCATestCA) issueLeaf(t *testing.T, dnsName string) *x509.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return leaf
}

func connState(serverName string, peerCerts ...*x509.Certificate) tls.ConnectionState {
	return tls.ConnectionState{
		ServerName:       serverName,
		PeerCertificates: peerCerts,
	}
}

func TestNewTLSConfigForServerWithRootCAProvider(t *testing.T) {
	const serverName = "temporal-frontend.local"

	ca1 := newDynamicRootCATestCA(t, "ca1")
	ca2 := newDynamicRootCATestCA(t, "ca2")
	leaf1 := ca1.issueLeaf(t, serverName)
	leaf2 := ca2.issueLeaf(t, serverName)

	t.Run("resolves roots per handshake", func(t *testing.T) {
		activePool := ca1.pool
		c := NewTLSConfigForServerWithRootCAProvider(serverName, true,
			func() (*x509.CertPool, error) { return activePool, nil })

		require.True(t, c.InsecureSkipVerify)
		require.NotNil(t, c.VerifyConnection)

		require.NoError(t, c.VerifyConnection(connState(serverName, leaf1)))
		require.Error(t, c.VerifyConnection(connState(serverName, leaf2)))

		// A provider swap is observed by subsequent verifications of the same config.
		activePool = ca2.pool
		require.NoError(t, c.VerifyConnection(connState(serverName, leaf2)))
		require.Error(t, c.VerifyConnection(connState(serverName, leaf1)))
	})

	t.Run("verifies identity against negotiated SNI with static fallback", func(t *testing.T) {
		c := NewTLSConfigForServerWithRootCAProvider(serverName, true,
			func() (*x509.CertPool, error) { return ca1.pool, nil })

		require.Error(t, c.VerifyConnection(connState("wrong.host", leaf1)))
		// Empty SNI (e.g. literal-IP dial) falls back to the configured serverName.
		require.NoError(t, c.VerifyConnection(connState("", leaf1)))
	})

	t.Run("fails closed without any identity", func(t *testing.T) {
		c := NewTLSConfigForServerWithRootCAProvider("", true,
			func() (*x509.CertPool, error) { return ca1.pool, nil })

		require.ErrorContains(t, c.VerifyConnection(connState("", leaf1)),
			"cannot verify server identity")
	})

	t.Run("fails closed with nil provider", func(t *testing.T) {
		c := NewTLSConfigForServerWithRootCAProvider(serverName, true, nil)

		require.ErrorContains(t, c.VerifyConnection(connState(serverName, leaf1)),
			"root CA provider is not configured")
	})

	t.Run("propagates provider errors", func(t *testing.T) {
		providerErr := errors.New("provider boom")
		c := NewTLSConfigForServerWithRootCAProvider(serverName, true,
			func() (*x509.CertPool, error) { return nil, providerErr })

		require.ErrorIs(t, c.VerifyConnection(connState(serverName, leaf1)), providerErr)
	})

	t.Run("rejects empty peer certificates", func(t *testing.T) {
		c := NewTLSConfigForServerWithRootCAProvider(serverName, true,
			func() (*x509.CertPool, error) { return ca1.pool, nil })

		require.ErrorContains(t, c.VerifyConnection(connState(serverName)),
			"server presented no certificates")
	})

	t.Run("disabled host verification ignores the provider", func(t *testing.T) {
		c := NewTLSConfigForServerWithRootCAProvider(serverName, false,
			func() (*x509.CertPool, error) { return nil, errors.New("must not be called") })

		// Legacy no-verification config: stdlib skip-verify, no custom verifier.
		require.True(t, c.InsecureSkipVerify)
		require.Nil(t, c.VerifyConnection)
	})
}
