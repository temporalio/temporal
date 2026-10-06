package encryption

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T, commonName string) *testCA {
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
	return &testCA{cert: cert, key: key, pool: pool}
}

func (ca *testCA) issueLeaf(t *testing.T, name string) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip := net.ParseIP(name); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{name}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}
}

func clientHandshake(t *testing.T, clientConfig *tls.Config, serverLeaf tls.Certificate) error {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() {
			_ = conn.Close()
		}()

		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			serverErr <- err
			return
		}
		server := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{serverLeaf}})
		serverErr <- server.Handshake()
	}()

	conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	client := tls.Client(conn, clientConfig)
	clientErr := client.Handshake()
	<-serverErr
	return clientErr
}

func clientHandshakeWithServerName(
	t *testing.T,
	clientConfig *tls.Config,
	serverName string,
	serverLeaf tls.Certificate,
) error {
	t.Helper()

	tlsConfig := clientConfig.Clone()
	tlsConfig.ServerName = serverName
	return clientHandshake(t, tlsConfig, serverLeaf)
}

func poolTrusts(pool *x509.CertPool, leaf *x509.Certificate) bool {
	if pool == nil {
		return false
	}
	_, err := leaf.Verify(x509.VerifyOptions{Roots: pool})
	return err == nil
}

func writeCABundle(t *testing.T, path string, certificates ...*x509.Certificate) {
	t.Helper()

	var buffer bytes.Buffer
	for _, certificate := range certificates {
		require.NoError(t, pem.Encode(&buffer, &pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}))
	}
	require.NoError(t, os.WriteFile(path, buffer.Bytes(), 0o600))
}

func TestClientTLSConfigPicksUpCARotation(t *testing.T) {
	const serverName = "temporal-frontend.local"

	ca1 := newTestCA(t, "ca1")
	ca2 := newTestCA(t, "ca2")
	leaf1 := ca1.issueLeaf(t, serverName)
	leaf2 := ca2.issueLeaf(t, serverName)
	provider := NewTestDynamicCertProvider(nil, ca1.pool, ca2.pool, config.GroupTLS{})

	clientConfig, err := newClientTLSConfig(provider, serverName, false, false, true)
	require.NoError(t, err)

	require.NoError(t, clientHandshake(t, clientConfig, leaf1))
	require.Error(t, clientHandshake(t, clientConfig, leaf2))

	provider.SwitchToWrongServerRootCACerts()

	require.NoError(t, clientHandshake(t, clientConfig, leaf2))
	require.Error(t, clientHandshake(t, clientConfig, leaf1))
}

func TestClientTLSConfigServerNameVerification(t *testing.T) {
	ca := newTestCA(t, "ca")
	dnsLeaf := ca.issueLeaf(t, "real.host")
	ipLeaf := ca.issueLeaf(t, "127.0.0.1")
	noIPSANLeaf := ca.issueLeaf(t, "some.host")

	tests := []struct {
		name             string
		staticServerName string
		dialServerName   string
		leaf             tls.Certificate
		wantErr          bool
	}{
		{
			name:           "DNS dial verifies negotiated authority",
			dialServerName: "real.host",
			leaf:           dnsLeaf,
		},
		{
			name:           "DNS dial rejects mismatched authority",
			dialServerName: "wrong.host",
			leaf:           dnsLeaf,
			wantErr:        true,
		},
		{
			name:             "IP dial verifies configured IP SAN",
			staticServerName: "127.0.0.1",
			dialServerName:   "127.0.0.1",
			leaf:             ipLeaf,
		},
		{
			name:             "IP dial rejects missing IP SAN",
			staticServerName: "127.0.0.1",
			dialServerName:   "127.0.0.1",
			leaf:             noIPSANLeaf,
			wantErr:          true,
		},
		{
			name:           "IP dial without configured server name fails closed",
			dialServerName: "127.0.0.1",
			leaf:           ipLeaf,
			wantErr:        true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := NewTestDynamicCertProvider(nil, ca.pool, ca.pool, config.GroupTLS{})
			clientConfig, err := newClientTLSConfig(provider, test.staticServerName, false, false, true)
			require.NoError(t, err)

			err = clientHandshakeWithServerName(t, clientConfig, test.dialServerName, test.leaf)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestClientTLSConfigRejectsNilRootCAProvider(t *testing.T) {
	ca := newTestCA(t, "ca")
	leaf := ca.issueLeaf(t, "real.host")
	clientConfig := newDynamicTLSClientConfig(nil, nil, "real.host", true)

	require.ErrorContains(t, clientHandshake(t, clientConfig, leaf), "root CA provider is not configured")
}

func TestClientTLSConfigDisableHostVerificationIsLegacy(t *testing.T) {
	trusted := newTestCA(t, "trusted")
	untrusted := newTestCA(t, "untrusted")
	leaf := untrusted.issueLeaf(t, "whatever.host")
	provider := NewTestDynamicCertProvider(nil, trusted.pool, trusted.pool, config.GroupTLS{})

	clientConfig, err := newClientTLSConfig(provider, "expected.host", false, false, false)
	require.NoError(t, err)
	require.NoError(t, clientHandshake(t, clientConfig, leaf))
}

func TestLocalStoreCertProviderReloadsAppendedCABundle(t *testing.T) {
	ca1 := newTestCA(t, "ca1")
	ca2 := newTestCA(t, "ca2")
	leaf1 := ca1.issueLeaf(t, "svc").Leaf
	leaf2 := ca2.issueLeaf(t, "svc").Leaf

	path := filepath.Join(t.TempDir(), "ca.pem")
	writeCABundle(t, path, ca1.cert)

	tlsConfig := config.GroupTLS{Client: config.ClientTLS{RootCAFiles: []string{path}}}
	provider := NewLocalStoreCertProvider(&tlsConfig, nil, nil, 20*time.Millisecond, log.NewNoopLogger())
	defer provider.(*localStoreCertProvider).Close()

	pool, err := provider.FetchServerRootCAsForClient(false)
	require.NoError(t, err)
	require.True(t, poolTrusts(pool, leaf1))
	require.False(t, poolTrusts(pool, leaf2))

	writeCABundle(t, path, ca1.cert, ca2.cert)
	await.RequireTrue(t, func() bool {
		pool, err := provider.FetchServerRootCAsForClient(false)
		return err == nil && poolTrusts(pool, leaf1) && poolTrusts(pool, leaf2)
	}, 3*time.Second, 20*time.Millisecond)

	writeCABundle(t, path, ca2.cert)
	await.RequireTrue(t, func() bool {
		pool, err := provider.FetchServerRootCAsForClient(false)
		return err == nil && poolTrusts(pool, leaf2) && !poolTrusts(pool, leaf1)
	}, 3*time.Second, 20*time.Millisecond)
}
