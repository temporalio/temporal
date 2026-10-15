package encryption

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/tests/testutils"
)

// serverCertProvider is a stubCertProvider that serves a real certificate, which is required
// to build a server side tls.Config.
type serverCertProvider struct {
	stubCertProvider
	cert *tls.Certificate
}

func (s *serverCertProvider) FetchServerCertificate() (*tls.Certificate, error) {
	return s.cert, nil
}

func newTestServerTLSProvider(t *testing.T, cfg config.RootTLS, cert *tls.Certificate) TLSConfigProvider {
	t.Helper()
	factory := func(_ *config.GroupTLS, _ *config.WorkerTLS, _ *config.ClientTLS, _ time.Duration, _ log.Logger) CertProvider {
		return &serverCertProvider{cert: cert}
	}
	provider, err := NewLocalStoreTlsProvider(&cfg, metrics.NoopMetricsHandler, log.NewTestLogger(), factory)
	require.NoError(t, err)
	return provider
}

func TestClientTLSVersions(t *testing.T) {
	cfg := config.RootTLS{
		Internode: config.GroupTLS{
			Client: config.ClientTLS{ForceTLS: true, MinVersion: "1.3", MaxVersion: "1.3"},
		},
		Frontend: config.GroupTLS{
			Client: config.ClientTLS{ForceTLS: true},
		},
	}
	provider := newTestTLSProvider(t, cfg)

	internodeCfg, err := provider.GetInternodeClientConfig()
	require.NoError(t, err)
	require.NotNil(t, internodeCfg)
	require.Equal(t, uint16(tls.VersionTLS13), internodeCfg.MinVersion)
	require.Equal(t, uint16(tls.VersionTLS13), internodeCfg.MaxVersion)

	// Not configured, defaults are preserved
	frontendCfg, err := provider.GetFrontendClientConfig()
	require.NoError(t, err)
	require.NotNil(t, frontendCfg)
	require.Equal(t, uint16(tls.VersionTLS12), frontendCfg.MinVersion)
	require.Zero(t, frontendCfg.MaxVersion)
}

func TestRemoteClusterClientTLSVersions(t *testing.T) {
	cfg := config.RootTLS{
		RemoteClusters: map[string]config.GroupTLS{
			"cluster-a.example.com": {
				Client: config.ClientTLS{ForceTLS: true, MinVersion: "1.3"},
			},
		},
	}
	provider := newTestTLSProvider(t, cfg)

	tlsCfg, err := provider.GetRemoteClusterClientConfig("cluster-a.example.com")
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	require.Equal(t, uint16(tls.VersionTLS13), tlsCfg.MinVersion)
}

func TestServerTLSVersions(t *testing.T) {
	certs, _, _, err := testutils.GenerateTestCerts(t.TempDir(), "127.0.0.1", 1)
	require.NoError(t, err)

	cfg := config.RootTLS{
		Frontend: config.GroupTLS{
			Server: config.ServerTLS{KeyFile: "foo", MinVersion: "1.3"},
			PerHostOverrides: map[string]config.ServerTLS{
				"inherits.example.com": {KeyFile: "foo"},
				"blank.example.com":    {KeyFile: "foo", MinVersion: " "},
				"legacy.example.com":   {KeyFile: "foo", MinVersion: "1.2", MaxVersion: "1.2"},
			},
		},
		Internode: config.GroupTLS{
			Server: config.ServerTLS{KeyFile: "foo"},
		},
	}
	provider := newTestServerTLSProvider(t, cfg, certs[0])

	frontendCfg, err := provider.GetFrontendServerConfig()
	require.NoError(t, err)
	require.NotNil(t, frontendCfg)
	require.Equal(t, uint16(tls.VersionTLS13), frontendCfg.MinVersion)
	require.Zero(t, frontendCfg.MaxVersion)

	// Not configured, defaults are preserved
	internodeCfg, err := provider.GetInternodeServerConfig()
	require.NoError(t, err)
	require.NotNil(t, internodeCfg)
	require.Equal(t, uint16(tls.VersionTLS12), internodeCfg.MinVersion)
	require.Zero(t, internodeCfg.MaxVersion)

	// Go negotiates the protocol version after GetConfigForClient, so the config returned
	// for a host override is the one that takes effect.
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	configForHost := func(serverName string) *tls.Config {
		hostCfg, err := frontendCfg.GetConfigForClient(&tls.ClientHelloInfo{
			ServerName: serverName,
			Conn:       serverConn,
		})
		require.NoError(t, err)
		require.NotNil(t, hostCfg)
		return hostCfg
	}

	// An override without its own bounds inherits them from the group, so adding a host
	// override cannot silently widen the range.
	inherited := configForHost("inherits.example.com")
	require.Equal(t, uint16(tls.VersionTLS13), inherited.MinVersion)
	require.Zero(t, inherited.MaxVersion)

	// A blank value counts as unset, so it inherits rather than falling back to the default.
	blank := configForHost("blank.example.com")
	require.Equal(t, uint16(tls.VersionTLS13), blank.MinVersion)

	// Explicit per-host bounds win over the group.
	overridden := configForHost("legacy.example.com")
	require.Equal(t, uint16(tls.VersionTLS12), overridden.MinVersion)
	require.Equal(t, uint16(tls.VersionTLS12), overridden.MaxVersion)
}

func TestPerHostSettingsRejectDuplicateHostNames(t *testing.T) {
	// Without this check, map iteration order would decide whether TLS 1.2 or TLS 1.3 is the
	// minimum for this host, so the effective policy could differ between restarts.
	cfg := config.RootTLS{
		Frontend: config.GroupTLS{
			Server: config.ServerTLS{KeyFile: "foo", MinVersion: "1.3"},
			PerHostOverrides: map[string]config.ServerTLS{
				"EXAMPLE.COM": {KeyFile: "upper", MinVersion: "1.2", MaxVersion: "1.2"},
				"example.com": {KeyFile: "lower"},
			},
		},
	}

	for range 50 {
		_, err := NewLocalStoreTlsProvider(&cfg, metrics.NoopMetricsHandler, log.NewTestLogger(), stubCertProviderFactory)
		require.ErrorContains(t, err, `duplicate host override "example.com"`)
	}

	// Host names that only differ from each other are fine.
	cfg.Frontend.PerHostOverrides = map[string]config.ServerTLS{
		"EXAMPLE.COM": {KeyFile: "upper"},
		"other.com":   {KeyFile: "lower"},
	}
	_, err := NewLocalStoreTlsProvider(&cfg, metrics.NoopMetricsHandler, log.NewTestLogger(), stubCertProviderFactory)
	require.NoError(t, err)
}

func TestHostOverrideInheritedVersionsAreValidated(t *testing.T) {
	// Inherited minVersion 1.3 combined with the per-host maxVersion 1.2 is an empty range.
	cfg := config.RootTLS{
		Frontend: config.GroupTLS{
			Server: config.ServerTLS{KeyFile: "foo", MinVersion: "1.3"},
			PerHostOverrides: map[string]config.ServerTLS{
				"legacy.example.com": {KeyFile: "foo", MaxVersion: "1.2"},
			},
		},
	}
	_, err := NewLocalStoreTlsProvider(&cfg, metrics.NoopMetricsHandler, log.NewTestLogger(), stubCertProviderFactory)
	require.ErrorContains(t, err, "legacy.example.com")
}

func TestServerRejectsClientBelowMinVersion(t *testing.T) {
	certs, caPool, _, err := testutils.GenerateTestCerts(t.TempDir(), "127.0.0.1", 1)
	require.NoError(t, err)

	cfg := config.RootTLS{
		Frontend: config.GroupTLS{
			Server: config.ServerTLS{KeyFile: "foo", MinVersion: "1.3"},
		},
	}
	provider := newTestServerTLSProvider(t, cfg, certs[0])
	serverCfg, err := provider.GetFrontendServerConfig()
	require.NoError(t, err)

	handshake := func(clientMaxVersion uint16) (clientErr error, serverErr error) {
		serverConn, clientConn := net.Pipe()
		defer func() { _ = serverConn.Close() }()
		defer func() { _ = clientConn.Close() }()

		serverErrCh := make(chan error, 1)
		go func() {
			serverErrCh <- tls.Server(serverConn, serverCfg).Handshake()
		}()

		clientErr = tls.Client(clientConn, &tls.Config{
			RootCAs:    caPool,
			ServerName: "127.0.0.1",
			MinVersion: tls.VersionTLS12,
			MaxVersion: clientMaxVersion,
		}).Handshake()
		return clientErr, <-serverErrCh
	}

	// The failure must be about the protocol version, not about the certificate, otherwise
	// this test would keep passing if version enforcement were dropped.
	clientErr, serverErr := handshake(tls.VersionTLS12)
	require.ErrorContains(t, clientErr, "protocol version not supported")
	require.ErrorContains(t, serverErr, "client offered only unsupported versions")

	clientErr, serverErr = handshake(tls.VersionTLS13)
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)
}

func TestInvalidHostOverrideTLSVersionFailsProviderCreation(t *testing.T) {
	cfg := config.RootTLS{
		Frontend: config.GroupTLS{
			PerHostOverrides: map[string]config.ServerTLS{
				"legacy.example.com": {KeyFile: "foo", MinVersion: "1.4"},
			},
		},
	}
	_, err := NewLocalStoreTlsProvider(&cfg, metrics.NoopMetricsHandler, log.NewTestLogger(), stubCertProviderFactory)
	require.ErrorContains(t, err, "legacy.example.com")
}

func TestValidateTLSVersions(t *testing.T) {
	testCases := []struct {
		name string
		cfg  config.RootTLS
	}{
		{
			name: "invalid frontend server version",
			cfg:  config.RootTLS{Frontend: config.GroupTLS{Server: config.ServerTLS{MinVersion: "1.4"}}},
		},
		{
			name: "invalid internode client version",
			cfg:  config.RootTLS{Internode: config.GroupTLS{Client: config.ClientTLS{MaxVersion: "1.1"}}},
		},
		{
			name: "max lower than min",
			cfg:  config.RootTLS{Frontend: config.GroupTLS{Server: config.ServerTLS{MinVersion: "1.3", MaxVersion: "1.2"}}},
		},
		{
			name: "invalid host override version",
			cfg: config.RootTLS{Frontend: config.GroupTLS{
				PerHostOverrides: map[string]config.ServerTLS{"foo": {MinVersion: "1.4"}},
			}},
		},
		{
			name: "invalid remote cluster version",
			cfg: config.RootTLS{RemoteClusters: map[string]config.GroupTLS{
				"cluster-a": {Client: config.ClientTLS{MinVersion: "1.4"}},
			}},
		},
		{
			name: "invalid system worker client version",
			cfg:  config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{MinVersion: "1.4"}}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validateRootTLS(&tc.cfg))
		})
	}

	t.Run("remote cluster error names the cluster", func(t *testing.T) {
		err := validateRootTLS(&config.RootTLS{RemoteClusters: map[string]config.GroupTLS{
			"cluster-a": {Client: config.ClientTLS{MinVersion: "1.4"}},
		}})
		require.ErrorContains(t, err, "cluster-a")
	})

	t.Run("remote clusters keep passing the checks they were never subject to", func(t *testing.T) {
		require.NoError(t, validateRootTLS(&config.RootTLS{RemoteClusters: map[string]config.GroupTLS{
			"cluster-a": {Client: config.ClientTLS{
				RootCAFiles: []string{"ca.pem"},
				RootCAData:  []string{"Y2E="},
			}},
		}}))
	})

	require.NoError(t, validateRootTLS(&config.RootTLS{
		Frontend: config.GroupTLS{Server: config.ServerTLS{MinVersion: "1.2", MaxVersion: "1.3"}},
	}))
}
