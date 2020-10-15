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

	internodeServerConfig *tls.Config
	internodeClientConfig *tls.Config
	frontendServerConfig  *tls.Config
	frontendClientConfig  *tls.Config
}

func NewLocalStoreTlsProvider(tlsConfig *config.RootTLS) (TLSConfigProvider, error) {
	return &localStoreTlsProvider{
		internodeCertProvider: &localStoreCertProvider{tlsSettings: &tlsConfig.Internode},
		frontendCertProvider:  &localStoreCertProvider{tlsSettings: &tlsConfig.Frontend},
		RWMutex:               sync.RWMutex{},
		settings:              tlsConfig,
	}, nil
}

func (s *localStoreTlsProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(&s.internodeClientConfig, newClientTLSConfig, s.internodeCertProvider, s.internodeCertProvider)
}

func (s *localStoreTlsProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(&s.frontendClientConfig, newClientTLSConfig, s.internodeCertProvider, s.frontendCertProvider)
}

func (s *localStoreTlsProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(&s.frontendServerConfig, newServerTLSConfig, s.frontendCertProvider, s.frontendCertProvider)
}

func (s *localStoreTlsProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(&s.internodeServerConfig, newServerTLSConfig, s.internodeCertProvider, s.internodeCertProvider)
}

func (s *localStoreTlsProvider) getOrCreateConfig(
	cachedConfig **tls.Config,
	configConstructor tlsConfigConstructor,
	localCertProvider CertProvider,
	settingsProvider CertProvider,
) (*tls.Config, error) {
	if !localCertProvider.GetSettings().IsEnabled() {
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
	localConfig, err := configConstructor(localCertProvider, settingsProvider)

	if err != nil {
		return nil, err
	}

	*cachedConfig = localConfig
	return *cachedConfig, nil
}

func newServerTLSConfig(certProvider CertProvider, settingsProvider CertProvider) (*tls.Config, error) {
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
	if settingsProvider.GetSettings().Server.RequireClientAuth {
		// TODO: We could expose tls.ClientAuth enum instead of a bool `RequireClientAuth` for more fine grained control in config
		clientAuthType = tls.RequireAndVerifyClientCert

		ca, err := certProvider.FetchClientCAs()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch client CAs: %v", err)
		}

		clientCaPool = ca
	}

	return auth.NewTLSConfigWithClientAuthAndCAs(clientAuthType, []tls.Certificate{*serverCert}, clientCaPool), nil
}

func newClientTLSConfig(localProvider CertProvider, remoteProvider CertProvider) (*tls.Config, error) {
	// Optional ServerCA for client if not already trusted by host
	serverCa, err := remoteProvider.FetchServerRootCAsForClient()
	if err != nil {
		return nil, fmt.Errorf("failed to load client ca: %v", err)
	}

	// mTLS enabled, present certificate
	var clientCerts []tls.Certificate
	if remoteProvider.GetSettings().Server.RequireClientAuth {
		cert, err := localProvider.FetchServerCertificate()
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
		true,
	), nil
}
