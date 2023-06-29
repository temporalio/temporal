package encryption

import (
	"crypto/tls"
	"time"
)

// FixedTLSConfigProvider is a [TLSConfigProvider] that is for fixed sets of TLS
// configs. This is usually only used for testing.

type FixedTLSConfigProvider struct {
	InternodeServerConfig      *tls.Config
	InternodeClientConfig      *tls.Config
	FrontendServerConfig       *tls.Config
	FrontendClientConfig       *tls.Config
	RemoteClusterClientConfigs map[string]*tls.Config
	CertExpirationChecker      CertExpirationChecker
}

var _ TLSConfigProvider = (*FixedTLSConfigProvider)(nil)

// GetInternodeServerConfig implements [TLSConfigProvider.GetInternodeServerConfig].
func (f *FixedTLSConfigProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return f.InternodeServerConfig, nil
}

// GetInternodeClientConfig implements [TLSConfigProvider.GetInternodeClientConfig].
func (f *FixedTLSConfigProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return f.InternodeClientConfig, nil
}

// GetFrontendServerConfig implements [TLSConfigProvider.GetFrontendServerConfig].
func (f *FixedTLSConfigProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return f.FrontendServerConfig, nil
}

// GetFrontendClientConfig implements [TLSConfigProvider.GetFrontendClientConfig].
func (f *FixedTLSConfigProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return f.FrontendClientConfig, nil
}

// GetRemoteClusterClientConfig implements [TLSConfigProvider.GetRemoteClusterClientConfig].
func (f *FixedTLSConfigProvider) GetRemoteClusterClientConfig(hostname string) (*tls.Config, error) {
	return f.RemoteClusterClientConfigs[hostname], nil
}

// GetExpiringCerts implements [TLSConfigProvider.GetExpiringCerts].
func (f *FixedTLSConfigProvider) GetExpiringCerts(
	timeWindow time.Duration,
) (expiring CertExpirationMap, expired CertExpirationMap, err error) {
	if f.CertExpirationChecker != nil {
		return f.CertExpirationChecker.GetExpiringCerts(timeWindow)
	}
	return nil, nil, nil
}
