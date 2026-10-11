package encryption

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type rotatingRootProvider struct {
	stubCertProvider
	roots          atomic.Pointer[x509.CertPool]
	unavailable    atomic.Bool
	workerCalls    atomic.Int64
	internodeCalls atomic.Int64
}

func (p *rotatingRootProvider) FetchServerRootCAsForClient(worker bool) (*x509.CertPool, error) {
	if worker {
		p.workerCalls.Add(1)
	} else {
		p.internodeCalls.Add(1)
	}
	if p.unavailable.Load() {
		return nil, errors.New("trust bundle unavailable")
	}
	return p.roots.Load(), nil
}

func rotationCertificate(t *testing.T, parent *x509.Certificate, parentKey ed25519.PrivateKey, name string, usage x509.ExtKeyUsage, expired bool) (*x509.Certificate, ed25519.PrivateKey, tls.Certificate) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          serial,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
	}
	if parent == nil {
		template.IsCA = true
		template.KeyUsage = x509.KeyUsageCertSign
		parent, parentKey = template, privateKey
	} else {
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = []net.IP{ip}
		} else {
			template.DNSNames = []string{name}
		}
		template.KeyUsage = x509.KeyUsageDigitalSignature
		template.ExtKeyUsage = []x509.ExtKeyUsage{usage}
	}
	if expired {
		template.NotAfter = time.Now().Add(-time.Minute)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, publicKey, parentKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return certificate, privateKey, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: privateKey, Leaf: certificate}
}

func rotationRoots(certificates ...*x509.Certificate) *x509.CertPool {
	roots := x509.NewCertPool()
	for _, certificate := range certificates {
		roots.AddCert(certificate)
	}
	return roots
}

func rotationHandshake(t *testing.T, clientConfig *tls.Config, serverCertificate tls.Certificate) error {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	require.NoError(t, clientConn.SetDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, serverConn.SetDeadline(time.Now().Add(5*time.Second)))
	server := tls.Server(serverConn, &tls.Config{Certificates: []tls.Certificate{serverCertificate}, MinVersion: tls.VersionTLS12})
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Handshake() }()
	client := tls.Client(clientConn, clientConfig)
	err := client.Handshake()
	if err != nil {
		clientConn.Close()
	}
	<-serverResult
	return err
}

func TestCachedClientConfigReloadsRootCAs(t *testing.T) {
	oldCA, oldKey, _ := rotationCertificate(t, nil, nil, "", 0, false)
	newCA, newKey, _ := rotationCertificate(t, nil, nil, "", 0, false)
	_, _, oldServer := rotationCertificate(t, oldCA, oldKey, "temporal.test", x509.ExtKeyUsageServerAuth, false)
	_, _, newServer := rotationCertificate(t, newCA, newKey, "temporal.test", x509.ExtKeyUsageServerAuth, false)
	for _, mode := range []struct{ worker, hostVerification bool }{
		{false, true}, {true, true}, {false, false}, {true, false},
	} {
		worker := mode.worker
		t.Run(fmt.Sprintf("worker=%t/hostname=%t", worker, mode.hostVerification), func(t *testing.T) {
			certProvider := &rotatingRootProvider{}
			certProvider.roots.Store(rotationRoots(oldCA))
			settings := &config.RootTLS{
				Internode:    config.GroupTLS{Client: config.ClientTLS{ServerName: "temporal.test", ForceTLS: true, DisableHostVerification: !mode.hostVerification}},
				SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ServerName: "temporal.test", ForceTLS: true, DisableHostVerification: !mode.hostVerification}},
			}
			provider, err := NewLocalStoreTlsProvider(settings, metrics.NoopMetricsHandler, log.NewTestLogger(), func(*config.GroupTLS, *config.WorkerTLS, *config.ClientTLS, time.Duration, log.Logger) CertProvider {
				return certProvider
			})
			require.NoError(t, err)
			getConfig := provider.GetInternodeClientConfig
			if worker {
				getConfig = provider.GetFrontendClientConfig
			}
			clientConfig, err := getConfig()
			require.NoError(t, err)
			// System worker requires a client SVID; the handshake here exercises server trust only.
			clientConfig.GetClientCertificate = nil
			require.NoError(t, rotationHandshake(t, clientConfig, oldServer))
			require.Error(t, rotationHandshake(t, clientConfig, newServer))
			certProvider.roots.Store(rotationRoots(oldCA, newCA))
			require.NoError(t, rotationHandshake(t, clientConfig, newServer))
			require.NoError(t, rotationHandshake(t, clientConfig, oldServer))
			certProvider.roots.Store(rotationRoots(newCA))
			require.NoError(t, rotationHandshake(t, clientConfig, newServer))
			require.Error(t, rotationHandshake(t, clientConfig, oldServer))
			cached, err := getConfig()
			require.NoError(t, err)
			require.Same(t, clientConfig, cached)
			if worker {
				require.Positive(t, certProvider.workerCalls.Load())
			} else {
				require.Positive(t, certProvider.internodeCalls.Load())
			}
			_, _, wrongNameServer := rotationCertificate(t, newCA, newKey, "wrong.test", x509.ExtKeyUsageServerAuth, false)
			if mode.hostVerification {
				require.Error(t, rotationHandshake(t, clientConfig, wrongNameServer))
			} else {
				require.NoError(t, rotationHandshake(t, clientConfig, wrongNameServer))
			}
			for _, invalid := range []struct {
				name    string
				usage   x509.ExtKeyUsage
				expired bool
			}{
				{"temporal.test", x509.ExtKeyUsageClientAuth, false},
				{"temporal.test", x509.ExtKeyUsageServerAuth, true},
			} {
				_, _, server := rotationCertificate(t, newCA, newKey, invalid.name, invalid.usage, invalid.expired)
				require.Error(t, rotationHandshake(t, clientConfig, server))
			}
			certProvider.unavailable.Store(true)
			require.ErrorContains(t, rotationHandshake(t, clientConfig, newServer), "trust bundle unavailable")
			require.Error(t, clientConfig.VerifyConnection(tls.ConnectionState{ServerName: "temporal.test"}))
			certProvider.unavailable.Store(false)
			require.ErrorContains(t, clientConfig.VerifyConnection(tls.ConnectionState{}), "server presented no certificate")
			// VerifyConnection is also invoked by crypto/tls on resumed sessions.
			require.Error(t, clientConfig.VerifyConnection(tls.ConnectionState{ServerName: "temporal.test", DidResume: true, PeerCertificates: []*x509.Certificate{oldServer.Leaf}}))
		})
	}
}

func TestDynamicRootsVerifyConfiguredIPWithoutSNI(t *testing.T) {
	ca, key, _ := rotationCertificate(t, nil, nil, "", 0, false)
	_, _, server := rotationCertificate(t, ca, key, "127.0.0.1", x509.ExtKeyUsageServerAuth, false)
	certProvider := &rotatingRootProvider{}
	certProvider.roots.Store(rotationRoots(ca))
	clientConfig, err := newClientTLSConfig(certProvider, "127.0.0.1", false, false, true)
	require.NoError(t, err)
	require.NoError(t, rotationHandshake(t, clientConfig, server))
	wrongName, err := newClientTLSConfig(certProvider, "127.0.0.2", false, false, true)
	require.NoError(t, err)
	require.Error(t, rotationHandshake(t, wrongName, server))
}
