package encryption

import (
	"crypto/tls"
	"crypto/x509"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

type TestDynamicTLSConfigProvider struct {
	settings *config.RootTLS

	InternodeCertProvider       *TestDynamicCertProvider
	InternodeClientCertProvider *TestDynamicCertProvider
	FrontendCertProvider        *TestDynamicCertProvider
	FrontendClientCertProvider  *TestDynamicCertProvider
	WorkerCertProvider          *TestDynamicCertProvider

	FrontendPerHostCertProviderMap PerHostCertProviderMap

	logger log.Logger
}

func (t *TestDynamicTLSConfigProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return newServerTLSConfig(t.InternodeCertProvider, nil, &t.settings.Internode, t.logger)
}

func (t *TestDynamicTLSConfigProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return newClientTLSConfig(t.InternodeClientCertProvider, t.settings.Internode.Client.ServerName, true, false, true)
}

func (t *TestDynamicTLSConfigProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return newServerTLSConfig(t.FrontendCertProvider, t.FrontendPerHostCertProviderMap, &t.settings.Frontend, t.logger)
}

func (t *TestDynamicTLSConfigProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return newClientTLSConfig(t.WorkerCertProvider, t.settings.Frontend.Client.ServerName, true, false, true)
}

func (t *TestDynamicTLSConfigProvider) GetExpiringCerts(timeWindow time.Duration) (expiring CertExpirationMap, expired CertExpirationMap, err error) {
	panic("implement me")
}

func (t *TestDynamicTLSConfigProvider) GetRemoteClusterClientConfig(hostName string) (*tls.Config, error) {
	panic("implement me")
}

var _ TLSConfigProvider = (*TestDynamicTLSConfigProvider)(nil)

func NewTestDynamicTLSConfigProvider(
	tlsConfig *config.RootTLS,
	internodeCerts []*tls.Certificate,
	internodeCACerts *x509.CertPool,
	frontendCerts []*tls.Certificate,
	frontendCACerts *x509.CertPool,
	wrongCACerts *x509.CertPool,
) (*TestDynamicTLSConfigProvider, error) {

	internodeProvider := NewTestDynamicCertProvider(internodeCerts, internodeCACerts, wrongCACerts, tlsConfig.Internode)
	frontendProvider := NewTestDynamicCertProvider(frontendCerts, frontendCACerts, wrongCACerts, tlsConfig.Frontend)

	return &TestDynamicTLSConfigProvider{
		InternodeCertProvider:          internodeProvider,
		InternodeClientCertProvider:    internodeProvider,
		FrontendCertProvider:           frontendProvider,
		FrontendClientCertProvider:     frontendProvider,
		WorkerCertProvider:             frontendProvider,
		FrontendPerHostCertProviderMap: frontendProvider,
		settings:                       tlsConfig,
		logger:                         log.NewTestLogger(),
	}, nil
}
