package encryption

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	tlsConfigTest struct {
		suite.Suite
		*require.Assertions
	}
)

func TestTLSConfigSuite(t *testing.T) {
	s := new(tlsConfigTest)
	suite.Run(t, s)
}

func (s *tlsConfigTest) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *tlsConfigTest) TestIsEnabled() {

	emptyCfg := config.GroupTLS{}
	s.False(emptyCfg.IsServerEnabled())
	s.False(emptyCfg.IsClientEnabled())
	cfg := config.GroupTLS{Server: config.ServerTLS{KeyFile: "foo"}}
	s.True(cfg.IsServerEnabled())
	s.False(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Server: config.ServerTLS{KeyData: "foo"}}
	s.True(cfg.IsServerEnabled())
	s.False(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}
	s.False(cfg.IsServerEnabled())
	s.True(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}
	s.False(cfg.IsServerEnabled())
	s.True(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: true}}
	s.False(cfg.IsServerEnabled())
	s.True(cfg.IsClientEnabled())
	cfg = config.GroupTLS{Client: config.ClientTLS{ForceTLS: false}}
	s.False(cfg.IsServerEnabled())
	s.False(cfg.IsClientEnabled())

}

func (s *tlsConfigTest) TestIsSystemWorker() {

	cfg := &config.RootTLS{}
	s.False(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertFile: "foo"}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{CertData: "foo"}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAData: []string{"bar"}}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{RootCAFiles: []string{"bar"}}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: true}}}
	s.True(isSystemWorker(cfg))
	cfg = &config.RootTLS{SystemWorker: config.WorkerTLS{Client: config.ClientTLS{ForceTLS: false}}}
	s.False(isSystemWorker(cfg))
}

func (s *tlsConfigTest) TestCertFileAndData() {
	s.testGroupTLS(s.testCertFileAndData)
}

func (s *tlsConfigTest) TestKeyFileAndData() {
	s.testGroupTLS(s.testKeyFileAndData)
}

func (s *tlsConfigTest) TestClientCAData() {
	s.testGroupTLS(s.testClientCAData)
}

func (s *tlsConfigTest) TestClientCAFiles() {
	s.testGroupTLS(s.testClientCAFiles)
}

func (s *tlsConfigTest) TestRootCAData() {
	s.testGroupTLS(s.testRootCAData)
}

func (s *tlsConfigTest) TestRootCAFiles() {
	s.testGroupTLS(s.testRootCAFiles)
}

func (s *tlsConfigTest) testGroupTLS(f func(*config.RootTLS, *config.GroupTLS)) {

	cfg := &config.RootTLS{Internode: config.GroupTLS{}}
	f(cfg, &cfg.Internode)
	cfg = &config.RootTLS{Frontend: config.GroupTLS{}}
	f(cfg, &cfg.Frontend)
}

func (s *tlsConfigTest) testCertFileAndData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertData: "bar"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{CertFile: "foo", CertData: "bar"}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testKeyFileAndData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyData: "bar"}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{KeyFile: "foo", KeyData: "bar"}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testClientCAData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAData: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testClientCAFiles(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Server = config.ServerTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Server = config.ServerTLS{ClientCAFiles: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testRootCAData(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Client = config.ClientTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAData: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) testRootCAFiles(cfg *config.RootTLS, group *config.GroupTLS) {

	group.Client = config.ClientTLS{}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo", "bar"}}
	s.Nil(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{"foo", " "}}
	s.Error(validateRootTLS(cfg))
	group.Client = config.ClientTLS{RootCAFiles: []string{""}}
	s.Error(validateRootTLS(cfg))
}

func (s *tlsConfigTest) TestSystemWorkerTLSConfig() {
	cfg := &config.RootTLS{}
	cfg.SystemWorker = config.WorkerTLS{}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertData: "bar"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{CertFile: "foo", CertData: "bar"}
	s.Error(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyFile: "foo"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyData: "bar"}
	s.Nil(validateRootTLS(cfg))
	cfg.SystemWorker = config.WorkerTLS{KeyFile: "foo", KeyData: "bar"}
	s.Error(validateRootTLS(cfg))

	cfg.SystemWorker = config.WorkerTLS{Client: config.ClientTLS{}}
	client := &cfg.SystemWorker.Client
	client.RootCAData = []string{}
	s.Nil(validateRootTLS(cfg))
	client.RootCAData = []string{"foo"}
	s.Nil(validateRootTLS(cfg))
	client.RootCAData = []string{"foo", "bar"}
	s.Nil(validateRootTLS(cfg))
	client.RootCAData = []string{"foo", " "}
	s.Error(validateRootTLS(cfg))
	client.RootCAData = []string{""}
	s.Error(validateRootTLS(cfg))
}

// stubCertProvider is a no-op CertProvider for use in unit tests.
type stubCertProvider struct{}

func (s *stubCertProvider) FetchServerCertificate() (*tls.Certificate, error) { return nil, nil }
func (s *stubCertProvider) FetchClientCAs() (*x509.CertPool, error)           { return nil, nil }
func (s *stubCertProvider) FetchClientCertificate(_ bool) (*tls.Certificate, error) {
	return nil, nil
}
func (s *stubCertProvider) FetchServerRootCAsForClient(_ bool) (*x509.CertPool, error) {
	return nil, nil
}
func (s *stubCertProvider) GetExpiringCerts(_ time.Duration) (expiring CertExpirationMap, expired CertExpirationMap, err error) {
	return nil, nil, nil
}

func stubCertProviderFactory(_ *config.GroupTLS, _ *config.WorkerTLS, _ *config.ClientTLS, _ time.Duration, _ log.Logger) CertProvider {
	return &stubCertProvider{}
}

func newTestTLSProvider(t *testing.T, cfg config.RootTLS) TLSConfigProvider {
	t.Helper()
	provider, err := NewLocalStoreTlsProvider(&cfg, metrics.NoopMetricsHandler, log.NewTestLogger(), stubCertProviderFactory)
	require.NoError(t, err)
	return provider
}

func TestGetRemoteClusterClientConfig_NoConfig(t *testing.T) {
	provider := newTestTLSProvider(t, config.RootTLS{})
	tlsCfg, err := provider.GetRemoteClusterClientConfig("some-host")
	require.NoError(t, err)
	require.Nil(t, tlsCfg)
}

func TestGetRemoteClusterClientConfig_UnknownHostNoDefault(t *testing.T) {
	cfg := config.RootTLS{
		RemoteClusters: map[string]config.GroupTLS{
			"cluster-a.example.com": {Client: config.ClientTLS{ForceTLS: true}},
		},
	}
	provider := newTestTLSProvider(t, cfg)

	tlsCfg, err := provider.GetRemoteClusterClientConfig("unknown-host.example.com")
	require.NoError(t, err)
	require.Nil(t, tlsCfg)
}

func TestGetRemoteClusterClientConfig_ExactMatch(t *testing.T) {
	cfg := config.RootTLS{
		RemoteClusters: map[string]config.GroupTLS{
			"cluster-a.example.com": {Client: config.ClientTLS{ForceTLS: true}},
		},
	}
	provider := newTestTLSProvider(t, cfg)

	tlsCfg, err := provider.GetRemoteClusterClientConfig("cluster-a.example.com")
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)

	// Unknown host with no default → nil
	tlsCfg, err = provider.GetRemoteClusterClientConfig("cluster-b.example.com")
	require.NoError(t, err)
	require.Nil(t, tlsCfg)
}

func TestGetRemoteClusterClientConfig_DefaultFallback(t *testing.T) {
	cfg := config.RootTLS{
		RemoteClusters: map[string]config.GroupTLS{
			defaultRemoteCluster: {Client: config.ClientTLS{ForceTLS: true}},
		},
	}
	provider := newTestTLSProvider(t, cfg)

	tlsCfg, err := provider.GetRemoteClusterClientConfig("any-unknown-host")
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
}

func TestGetRemoteClusterClientConfig_ExactMatchTakesPriority(t *testing.T) {
	cfg := config.RootTLS{
		RemoteClusters: map[string]config.GroupTLS{
			"cluster-a.example.com": {Client: config.ClientTLS{ForceTLS: false}},
			// Default has ForceTLS: false so IsClientEnabled() returns false → nil config
			defaultRemoteCluster: {Client: config.ClientTLS{ForceTLS: true}},
		},
	}
	provider := newTestTLSProvider(t, cfg)

	// Exact match → nil (ForceTLS: false)
	tlsCfg, err := provider.GetRemoteClusterClientConfig("cluster-a.example.com")
	require.NoError(t, err)
	require.Nil(t, tlsCfg)

	// Unknown host falls back to default (ForceTLS: true) → non-nil
	tlsCfg, err = provider.GetRemoteClusterClientConfig("unknown-host")
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
}
