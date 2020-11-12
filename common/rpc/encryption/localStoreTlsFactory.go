// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package encryption

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/service/config"
)

type localStoreTlsProvider struct {
	sync.RWMutex

	settings *config.RootTLS

	internodeCertProvider CertProvider
	frontendCertProvider  CertProvider

	frontendPerHostCertProviderFactory PerHostCertProviderFactory

	internodeServerConfig *tls.Config
	internodeClientConfig *tls.Config
	frontendServerConfig  *tls.Config
	frontendClientConfig  *tls.Config
}

func NewLocalStoreTlsProvider(tlsConfig *config.RootTLS) (TLSConfigProvider, error) {
	return &localStoreTlsProvider{
		internodeCertProvider:              &localStoreCertProvider{tlsSettings: &tlsConfig.Internode},
		frontendCertProvider:               &localStoreCertProvider{tlsSettings: &tlsConfig.Frontend},
		frontendPerHostCertProviderFactory: newLocalStorePerHostCertProviderFactory(tlsConfig.Frontend.PerHostOverrides),
		RWMutex:                            sync.RWMutex{},
		settings:                           tlsConfig,
	}, nil
}

func (s *localStoreTlsProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.internodeClientConfig,
		func() (*tls.Config, error) {
			return newClientTLSConfig(s.internodeCertProvider, s.internodeCertProvider)
		},
		s.internodeCertProvider.GetSettings().IsEnabled(),
	)
}

func (s *localStoreTlsProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.frontendClientConfig,
		func() (*tls.Config, error) {
			return newClientTLSConfig(s.internodeCertProvider, s.frontendCertProvider)
		},
		s.internodeCertProvider.GetSettings().IsEnabled(),
	)
}

func (s *localStoreTlsProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.frontendServerConfig,
		func() (*tls.Config, error) {
			return newServerTLSConfig(s.frontendCertProvider, s.frontendPerHostCertProviderFactory)
		},
		s.frontendCertProvider.GetSettings().IsEnabled())
}

func (s *localStoreTlsProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.internodeServerConfig,
		func() (*tls.Config, error) {
			return newServerTLSConfig(s.internodeCertProvider, nil)
		},
		s.internodeCertProvider.GetSettings().IsEnabled())
}

func (s *localStoreTlsProvider) getOrCreateConfig(
	cachedConfig **tls.Config,
	configConstructor tlsConfigConstructor,
	isEnabled bool,
) (*tls.Config, error) {
	if !isEnabled {
		return nil, nil
	}

	// Check if exists under a read lock first
	s.RLock()
	if *cachedConfig != nil {
		defer s.RUnlock()
		return *cachedConfig, nil
	}
	// Not found, promote to write lock to initialize
	s.RUnlock()
	s.Lock()
	defer s.Unlock()
	// Check if someone got here first while waiting for write lock
	if *cachedConfig != nil {
		return *cachedConfig, nil
	}

	// Load configuration
	localConfig, err := configConstructor()

	if err != nil {
		return nil, err
	}

	*cachedConfig = localConfig
	return *cachedConfig, nil
}

func newServerTLSConfig(
	certProvider CertProvider,
	perHostCertProviderFactory PerHostCertProviderFactory,
) (*tls.Config, error) {
	tlsConfig, err := getServerTLSConfigFromCertProvider(certProvider)
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil && perHostCertProviderFactory != nil {
		tlsConfig.GetConfigForClient = func(c *tls.ClientHelloInfo) (*tls.Config, error) {
			perHostCertProvider, err := perHostCertProviderFactory.GetCertProvider(c.ServerName)
			if err != nil {
				return nil, err
			}

			// If there are no special TLS settings for the specific host name being requested,
			// returning nil here will fallback to the default, top-level TLS config
			if perHostCertProvider == nil {
				return nil, nil
			}

			return getServerTLSConfigFromCertProvider(perHostCertProvider)
		}
	}

	return tlsConfig, nil
}

func getServerTLSConfigFromCertProvider(certProvider CertProvider) (*tls.Config, error) {
	// Get serverCert from disk
	serverCert, err := certProvider.FetchServerCertificate()
	if err != nil {
		return nil, fmt.Errorf("loading server tls certificate failed: %v", err)
	}

	// tls disabled, responsibility of cert provider above to error otherwise
	if serverCert == nil {
		return nil, nil
	}

	// Default to NoClientAuth
	clientAuthType := tls.NoClientCert
	var clientCaPool *x509.CertPool

	// If mTLS enabled
	if certProvider.GetSettings().Server.RequireClientAuth {
		clientAuthType = tls.RequireAndVerifyClientCert

		ca, err := certProvider.FetchClientCAs()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch client CAs: %v", err)
		}

		clientCaPool = ca
	}

	return auth.NewTLSConfigWithClientAuthAndCAs(clientAuthType, []tls.Certificate{*serverCert}, clientCaPool), nil
}

func newClientTLSConfig(clientProvider CertProvider, remoteProvider CertProvider) (*tls.Config, error) {
	// Optional ServerCA for client if not already trusted by host
	serverCa, err := remoteProvider.FetchServerRootCAsForClient()
	if err != nil {
		return nil, fmt.Errorf("failed to load client ca: %v", err)
	}

	// mTLS enabled, present certificate
	var clientCerts []tls.Certificate
	if remoteProvider.GetSettings().Server.RequireClientAuth {
		cert, err := clientProvider.FetchServerCertificate()
		if err != nil {
			return nil, err
		}

		if cert == nil {
			return nil, fmt.Errorf("client auth required, but no certificate provided")
		}
		clientCerts = []tls.Certificate{*cert}
	}

	return auth.NewTLSConfigWithCertsAndCAs(
		clientCerts,
		serverCa,
		remoteProvider.GetSettings().Client.ServerName,
		!remoteProvider.GetSettings().Client.DisableHostVerification,
	), nil
}
