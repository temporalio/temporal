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

	"github.com/uber-go/tally"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

const (
	metricCertsExpired  = "certificates_expired"
	metricCertsExpiring = "certificates_expiring"
)

type CertProviderFactory func(
	tlsSettings *config.GroupTLS,
	workerTlsSettings *config.WorkerTLS,
	legacyWorkerSettings *config.ClientTLS,
	refreshInterval time.Duration) CertProvider

type localStoreTlsProvider struct {
	sync.RWMutex

	settings *config.RootTLS

	internodeCertProvider       CertProvider
	internodeClientCertProvider CertProvider
	frontendCertProvider        CertProvider
	workerCertProvider          CertProvider

	frontendPerHostCertProviderMap *localStorePerHostCertProviderMap

	cachedInternodeServerConfig *tls.Config
	cachedInternodeClientConfig *tls.Config
	cachedFrontendServerConfig  *tls.Config
	cachedFrontendClientConfig  *tls.Config

	ticker *time.Ticker
	logger log.Logger
	stop   chan bool
	scope  tally.Scope
}

var _ TLSConfigProvider = (*localStoreTlsProvider)(nil)
var _ CertExpirationChecker = (*localStoreTlsProvider)(nil)

func NewLocalStoreTlsProvider(tlsConfig *config.RootTLS, scope tally.Scope, certProviderFactory CertProviderFactory,
) (TLSConfigProvider, error) {

	internodeProvider := certProviderFactory(&tlsConfig.Internode, nil, nil, tlsConfig.RefreshInterval)
	var workerProvider CertProvider
	if tlsConfig.SystemWorker.CertFile != "" || tlsConfig.SystemWorker.CertData != "" { // explicit system worker config
		workerProvider = certProviderFactory(nil, &tlsConfig.SystemWorker, nil, tlsConfig.RefreshInterval)
	} else { // legacy implicit system worker config case
		internodeWorkerProvider := certProviderFactory(&tlsConfig.Internode, nil, &tlsConfig.Frontend.Client, tlsConfig.RefreshInterval)
		workerProvider = internodeWorkerProvider
	}

	provider := &localStoreTlsProvider{
		internodeCertProvider:       internodeProvider,
		internodeClientCertProvider: internodeProvider,
		frontendCertProvider:        certProviderFactory(&tlsConfig.Frontend, nil, nil, tlsConfig.RefreshInterval),
		workerCertProvider:          workerProvider,
		frontendPerHostCertProviderMap: newLocalStorePerHostCertProviderMap(
			tlsConfig.Frontend.PerHostOverrides, certProviderFactory, tlsConfig.RefreshInterval),
		RWMutex:  sync.RWMutex{},
		settings: tlsConfig,
		scope:    scope,
		logger:   log.NewDefaultLogger(),
	}
	provider.initialize()
	return provider, nil
}

func (s *localStoreTlsProvider) initialize() {

	period := s.settings.ExpirationChecks.CheckInterval
	if period != 0 {
		s.stop = make(chan bool)
		s.ticker = time.NewTicker(period)
		go s.timerCallback()
	}
}

func (s *localStoreTlsProvider) Close() {

	if s.ticker != nil {
		s.ticker.Stop()
	}
	if s.stop != nil {
		s.stop <- true
		close(s.stop)
	}
}

func (s *localStoreTlsProvider) GetInternodeClientConfig() (*tls.Config, error) {

	client := &s.settings.Internode.Client
	return s.getOrCreateConfig(
		&s.cachedInternodeClientConfig,
		func() (*tls.Config, error) {
			return newClientTLSConfig(s.internodeClientCertProvider, client.ServerName,
				s.settings.Internode.Server.RequireClientAuth, false, !client.DisableHostVerification)
		},
		s.settings.Internode.IsEnabled(),
	)
}

func (s *localStoreTlsProvider) GetFrontendClientConfig() (*tls.Config, error) {

	client := &s.settings.Frontend.Client
	return s.getOrCreateConfig(
		&s.cachedFrontendClientConfig,
		func() (*tls.Config, error) {
			return newClientTLSConfig(s.workerCertProvider, client.ServerName,
				s.settings.Frontend.Server.RequireClientAuth, true, !client.DisableHostVerification)
		},
		s.settings.Internode.IsEnabled(),
	)
}

func (s *localStoreTlsProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.cachedFrontendServerConfig,
		func() (*tls.Config, error) {
			return newServerTLSConfig(s.frontendCertProvider, s.frontendPerHostCertProviderMap, &s.settings.Frontend)
		},
		s.settings.Frontend.IsEnabled())
}

func (s *localStoreTlsProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.cachedInternodeServerConfig,
		func() (*tls.Config, error) {
			return newServerTLSConfig(s.internodeCertProvider, nil, &s.settings.Internode)
		},
		s.settings.Internode.IsEnabled())
}

func (s *localStoreTlsProvider) GetExpiringCerts(timeWindow time.Duration,
) (expiring CertExpirationMap, expired CertExpirationMap, err error) {

	expiring = make(CertExpirationMap, 0)
	expired = make(CertExpirationMap, 0)

	checkError := checkExpiration(s.internodeCertProvider, timeWindow, expiring, expired)
	err = appendError(err, checkError)
	checkError = checkExpiration(s.frontendCertProvider, timeWindow, expiring, expired)
	err = appendError(err, checkError)
	checkError = checkExpiration(s.workerCertProvider, timeWindow, expiring, expired)
	err = appendError(err, checkError)
	checkError = checkExpiration(s.frontendPerHostCertProviderMap, timeWindow, expiring, expired)
	err = appendError(err, checkError)

	return expiring, expired, err
}

func checkExpiration(
	provider CertExpirationChecker,
	timeWindow time.Duration,
	expiring CertExpirationMap,
	expired CertExpirationMap,
) error {

	providerExpiring, providerExpired, err := provider.GetExpiringCerts(timeWindow)
	mergeMaps(expiring, providerExpiring)
	mergeMaps(expired, providerExpired)
	return err
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
	perHostCertProviderMap PerHostCertProviderMap,
	config *config.GroupTLS,
) (*tls.Config, error) {

	clientAuthRequired := config.Server.RequireClientAuth
	tlsConfig, err := getServerTLSConfigFromCertProvider(certProvider, clientAuthRequired)
	if err != nil {
		return nil, err
	}

	tlsConfig.GetConfigForClient = func(c *tls.ClientHelloInfo) (*tls.Config, error) {
		if perHostCertProviderMap != nil {
			perHostCertProvider, hostClientAuthRequired, err := perHostCertProviderMap.GetCertProvider(c.ServerName)
			if err != nil {
				return nil, err
			}

			if perHostCertProvider != nil {
				return getServerTLSConfigFromCertProvider(perHostCertProvider, hostClientAuthRequired)
			}
			return getServerTLSConfigFromCertProvider(certProvider, clientAuthRequired)
		}
		return getServerTLSConfigFromCertProvider(certProvider, clientAuthRequired)
	}
	return tlsConfig, nil
}

func getServerTLSConfigFromCertProvider(certProvider CertProvider, requireClientAuth bool) (*tls.Config, error) {
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
	if requireClientAuth {
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

func newClientTLSConfig(clientProvider CertProvider, serverName string, isAuthRequired bool,
	isWorker bool, enableHostVerification bool) (*tls.Config, error) {
	// Optional ServerCA for client if not already trusted by host
	serverCa, err := clientProvider.FetchServerRootCAsForClient(isWorker)
	if err != nil {
		return nil, fmt.Errorf("failed to load client ca: %v", err)
	}

	var getCert tlsCertFetcher

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
		serverName,
		enableHostVerification,
	), nil
}

func (s *localStoreTlsProvider) timerCallback() {
	for {
		select {
		case <-s.stop:
			break
		case <-s.ticker.C:
		}

		var errorTime time.Time
		if s.settings.ExpirationChecks.ErrorWindow != 0 {
			errorTime = time.Now().UTC().Add(s.settings.ExpirationChecks.ErrorWindow)
		} else {
			errorTime = time.Now().UTC().AddDate(10, 0, 0)
		}

		window := s.settings.ExpirationChecks.WarningWindow
		// if only ErrorWindow is set, we set WarningWindow to the same value, so that the checks do happen
		if window == 0 && s.settings.ExpirationChecks.ErrorWindow != 0 {
			window = s.settings.ExpirationChecks.ErrorWindow
		}
		if window != 0 {
			expiring, expired, err := s.GetExpiringCerts(window)
			if err != nil {
				s.logger.Error(fmt.Sprintf("error while checking for certificate expiration: %v", err))
				continue
			}
			if s.scope != nil {
				s.scope.Gauge(metricCertsExpired).Update(float64(len(expired)))
				s.scope.Gauge(metricCertsExpiring).Update(float64(len(expiring)))
			}
			s.logCerts(expired, true, errorTime)
			s.logCerts(expiring, false, errorTime)
		}
	}
}

func (s *localStoreTlsProvider) logCerts(certs CertExpirationMap, expired bool, errorTime time.Time) {

	for _, cert := range certs {
		str := createExpirationLogMessage(cert, expired)
		if expired || cert.Expiration.Before(errorTime) {
			s.logger.Error(str)
		} else {
			s.logger.Warn(str)
		}
	}
}

func createExpirationLogMessage(cert CertExpirationData, expired bool) string {

	var verb string
	if expired {
		verb = "has expired"
	} else {
		verb = "will expire"
	}
	return fmt.Sprintf("certificate with thumbprint=%x %s on %v, IsCA=%t, DNS=%v",
		cert.Thumbprint, verb, cert.Expiration, cert.IsCA, cert.DNSNames)
}

func mergeMaps(to CertExpirationMap, from CertExpirationMap) {
	for k, v := range from {
		to[k] = v
	}
}
