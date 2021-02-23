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
	"time"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/service/config"
)

type localStoreTlsProvider struct {
	sync.RWMutex

	settings *config.RootTLS

	internodeCertProvider       CertProvider
	internodeClientCertProvider ClientCertProvider
	frontendCertProvider        CertProvider
	workerCertProvider          ClientCertProvider

	frontendPerHostCertProviderFactory PerHostCertProviderFactory

	internodeServerConfig *tls.Config
	internodeClientConfig *tls.Config
	frontendServerConfig  *tls.Config
	frontendClientConfig  *tls.Config
}

var _ TLSConfigProvider = (*localStoreTlsProvider)(nil)
var _ CertExpirationChecker = (*localStoreTlsProvider)(nil)

func NewLocalStoreTlsProvider(tlsConfig *config.RootTLS) (TLSConfigProvider, error) {
	internodeProvider := &localStoreCertProvider{tlsSettings: &tlsConfig.Internode}
	var workerProvider ClientCertProvider
	if tlsConfig.SystemWorker.CertFile != "" || tlsConfig.SystemWorker.CertData != "" { // explicit system worker config
		workerProvider = &localStoreCertProvider{workerTLSSettings: &tlsConfig.SystemWorker}
	} else { // legacy implicit system worker config case
		internodeWorkerProvider := &localStoreCertProvider{tlsSettings: &tlsConfig.Internode}
		internodeWorkerProvider.isLegacyWorkerConfig = true
		internodeWorkerProvider.legacyWorkerSettings = &tlsConfig.Frontend.Client
		workerProvider = internodeWorkerProvider
	}

	return &localStoreTlsProvider{
		internodeCertProvider:              internodeProvider,
		internodeClientCertProvider:        internodeProvider,
		frontendCertProvider:               &localStoreCertProvider{tlsSettings: &tlsConfig.Frontend},
		workerCertProvider:                 workerProvider,
		frontendPerHostCertProviderFactory: newLocalStorePerHostCertProviderFactory(tlsConfig.Frontend.PerHostOverrides),
		RWMutex:                            sync.RWMutex{},
		settings:                           tlsConfig,
	}, nil
}

func (s *localStoreTlsProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.internodeClientConfig,
		func() (*tls.Config, error) {
			return newClientTLSConfig(s.internodeClientCertProvider,
				s.internodeCertProvider.GetSettings().Server.RequireClientAuth, false)
		},
		s.internodeCertProvider.GetSettings().IsEnabled(),
	)
}

func (s *localStoreTlsProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.frontendClientConfig,
		func() (*tls.Config, error) {
			return newClientTLSConfig(s.workerCertProvider,
				s.frontendCertProvider.GetSettings().Server.RequireClientAuth, true)
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

func (s *localStoreTlsProvider) Expiring(fromNow time.Duration) ([]CertExpirationData, []error) {

	list := make([]CertExpirationData, 0)
	errs := make([]error, 0)

	list, errs = expiring(s.internodeCertProvider, fromNow, list, errs)
	list, errs = expiring(s.frontendCertProvider, fromNow, list, errs)
	list, errs = expiring(s.workerCertProvider, fromNow, list, errs)
	list, errs = expiring(s.frontendPerHostCertProviderFactory, fromNow, list, errs)

	return list, errs
}

func expiring(provider interface{}, fromNow time.Duration, list []CertExpirationData, errs []error) ([]CertExpirationData, []error) {

	p, ok := provider.(CertExpirationChecker)
	if ok {
		l, err := p.Expiring(fromNow)
		if len(l) != 0 {
			list = append(list, l...)
		}
		if len(err) != 0 {
			errs = append(errs, err...)
		}
	}
	return list, errs
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

	tlsConfig.GetConfigForClient = func(c *tls.ClientHelloInfo) (*tls.Config, error) {
		if perHostCertProviderFactory != nil {
			perHostCertProvider, err := perHostCertProviderFactory.GetCertProvider(c.ServerName)
			if err != nil {
				return nil, err
			}

			if perHostCertProvider == nil {
				return getServerTLSConfigFromCertProvider(certProvider)
			}
			return getServerTLSConfigFromCertProvider(perHostCertProvider)
		}
		return getServerTLSConfigFromCertProvider(certProvider)
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
	return auth.NewTLSConfigWithCertsAndCAs(
		clientAuthType,
		[]tls.Certificate{*serverCert},
		clientCaPool), nil
}

func newClientTLSConfig(clientProvider ClientCertProvider, isAuthRequired bool, isWorker bool) (*tls.Config, error) {
	// Optional ServerCA for client if not already trusted by host
	serverCa, err := clientProvider.FetchServerRootCAsForClient(isWorker)
	if err != nil {
		return nil, fmt.Errorf("failed to load client ca: %v", err)
	}

	var getCert func() (*tls.Certificate, error)

	// mTLS enabled, present certificate
	if isAuthRequired {
		getCert = func() (*tls.Certificate, error) {
			cert, err := clientProvider.FetchClientCertificate(isWorker)
			if err != nil {
				return nil, err
			}

			if cert == nil {
				return nil, fmt.Errorf("client auth required, but no certificate provided")
			}
			return cert, nil
		}
	}

	return auth.NewDynamicTLSClientConfig(
		getCert,
		serverCa,
		clientProvider.ServerName(isWorker),
		!clientProvider.DisableHostVerification(isWorker),
	), nil
}
