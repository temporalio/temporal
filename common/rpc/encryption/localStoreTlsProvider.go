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

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

type CertProviderFactory func(
	tlsSettings *config.GroupTLS,
	workerTlsSettings *config.WorkerTLS,
	legacyWorkerSettings *config.ClientTLS,
	refreshInterval time.Duration,
	logger log.Logger) CertProvider

type localStoreTlsProvider struct {
	sync.RWMutex

	settings *config.RootTLS

	internodeCertProvider           CertProvider
	internodeClientCertProvider     CertProvider
	frontendCertProvider            CertProvider
	workerCertProvider              CertProvider
	remoteClusterClientCertProvider map[string]CertProvider
	frontendPerHostCertProviderMap  *localStorePerHostCertProviderMap

	cachedInternodeServerConfig     *tls.Config
	cachedInternodeClientConfig     *tls.Config
	cachedFrontendServerConfig      *tls.Config
	cachedFrontendClientConfig      *tls.Config
	cachedRemoteClusterClientConfig map[string]*tls.Config

	ticker *time.Ticker
	logger log.Logger
	stop   chan bool
	scope  metrics.Scope
}

var _ TLSConfigProvider = (*localStoreTlsProvider)(nil)
var _ CertExpirationChecker = (*localStoreTlsProvider)(nil)

func NewLocalStoreTlsProvider(tlsConfig *config.RootTLS, scope metrics.Scope, logger log.Logger, certProviderFactory CertProviderFactory,
) (TLSConfigProvider, error) {

	internodeProvider := certProviderFactory(&tlsConfig.Internode, nil, nil, tlsConfig.RefreshInterval, logger)
	var workerProvider CertProvider
	if isSystemWorker(tlsConfig) { // explicit system worker config
		workerProvider = certProviderFactory(nil, &tlsConfig.SystemWorker, nil, tlsConfig.RefreshInterval, logger)
	} else { // legacy implicit system worker config case
		internodeWorkerProvider := certProviderFactory(&tlsConfig.Internode, nil, &tlsConfig.Frontend.Client, tlsConfig.RefreshInterval, logger)
		workerProvider = internodeWorkerProvider
	}

	remoteClusterClientCertProvider := make(map[string]CertProvider)
	for hostname, groupTLS := range tlsConfig.RemoteClusters {
		remoteClusterClientCertProvider[hostname] = certProviderFactory(&groupTLS, nil, nil, tlsConfig.RefreshInterval, logger)
	}

	provider := &localStoreTlsProvider{
		internodeCertProvider:       internodeProvider,
		internodeClientCertProvider: internodeProvider,
		frontendCertProvider:        certProviderFactory(&tlsConfig.Frontend, nil, nil, tlsConfig.RefreshInterval, logger),
		workerCertProvider:          workerProvider,
		frontendPerHostCertProviderMap: newLocalStorePerHostCertProviderMap(
			tlsConfig.Frontend.PerHostOverrides, certProviderFactory, tlsConfig.RefreshInterval, logger),
		remoteClusterClientCertProvider: remoteClusterClientCertProvider,
		RWMutex:                         sync.RWMutex{},
		settings:                        tlsConfig,
		scope:                           scope,
		logger:                          logger,
		cachedRemoteClusterClientConfig: make(map[string]*tls.Config),
	}
	provider.initialize()
	return provider, nil
}

func (s *localStoreTlsProvider) initialize() {
	period := s.settings.ExpirationChecks.CheckInterval
	if period != 0 {
		s.stop = make(chan bool)
		s.ticker = time.NewTicker(period)
		s.checkCertExpiration() // perform initial check to emit metrics and logs right away
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
		s.settings.Internode.IsClientEnabled(),
	)
}

func (s *localStoreTlsProvider) GetFrontendClientConfig() (*tls.Config, error) {

	var client *config.ClientTLS
	var useTLS bool
	if isSystemWorker(s.settings) {
		client = &s.settings.SystemWorker.Client
		useTLS = true
	} else {
		client = &s.settings.Frontend.Client
		useTLS = s.settings.Frontend.IsClientEnabled()
	}
	return s.getOrCreateConfig(
		&s.cachedFrontendClientConfig,
		func() (*tls.Config, error) {
			return newClientTLSConfig(s.workerCertProvider, client.ServerName,
				useTLS, true, !client.DisableHostVerification)
		},
		useTLS,
	)
}

func (s *localStoreTlsProvider) GetRemoteClusterClientConfig(hostname string) (*tls.Config, error) {
	groupTLS, ok := s.settings.RemoteClusters[hostname]
	if !ok {
		return nil, nil
	}

	return s.getOrCreateRemoteClusterClientConfig(
		hostname,
		func() (*tls.Config, error) {
			return newClientTLSConfig(
				s.remoteClusterClientCertProvider[hostname],
				groupTLS.Client.ServerName,
				groupTLS.Server.RequireClientAuth,
				false,
				!groupTLS.Client.DisableHostVerification)
		},
		groupTLS.IsClientEnabled(),
	)
}

func (s *localStoreTlsProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.cachedFrontendServerConfig,
		func() (*tls.Config, error) {
			return newServerTLSConfig(s.frontendCertProvider, s.frontendPerHostCertProviderMap, &s.settings.Frontend, s.logger)
		},
		s.settings.Frontend.IsServerEnabled())
}

func (s *localStoreTlsProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return s.getOrCreateConfig(
		&s.cachedInternodeServerConfig,
		func() (*tls.Config, error) {
			return newServerTLSConfig(s.internodeCertProvider, nil, &s.settings.Internode, s.logger)
		},
		s.settings.Internode.IsServerEnabled())
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

func (s *localStoreTlsProvider) getOrCreateRemoteClusterClientConfig(
	hostname string,
	configConstructor tlsConfigConstructor,
	isEnabled bool,
) (*tls.Config, error) {
	if !isEnabled {
		return nil, nil
	}

	// Check if exists under a read lock first
	s.RLock()
	if clientConfig, ok := s.cachedRemoteClusterClientConfig[hostname]; ok {
		defer s.RUnlock()
		return clientConfig, nil
	}
	// Not found, promote to write lock to initialize
	s.RUnlock()
	s.Lock()
	defer s.Unlock()
	// Check if someone got here first while waiting for write lock
	if clientConfig, ok := s.cachedRemoteClusterClientConfig[hostname]; ok {
		return clientConfig, nil
	}

	// Load configuration
	localConfig, err := configConstructor()

	if err != nil {
		return nil, err
	}

	s.cachedRemoteClusterClientConfig[hostname] = localConfig
	return localConfig, nil
}

func newServerTLSConfig(
	certProvider CertProvider,
	perHostCertProviderMap PerHostCertProviderMap,
	config *config.GroupTLS,
	logger log.Logger,
) (*tls.Config, error) {

	clientAuthRequired := config.Server.RequireClientAuth
	tlsConfig, err := getServerTLSConfigFromCertProvider(certProvider, clientAuthRequired, "", "", logger)
	if err != nil {
		return nil, err
	}

	tlsConfig.GetConfigForClient = func(c *tls.ClientHelloInfo) (*tls.Config, error) {

		remoteAddress := c.Conn.RemoteAddr().String()
		logger.Debug("attempted incoming TLS connection", tag.Address(remoteAddress), tag.ServerName(c.ServerName))

		if perHostCertProviderMap != nil && perHostCertProviderMap.NumberOfHosts() > 0 {
			perHostCertProvider, hostClientAuthRequired, err := perHostCertProviderMap.GetCertProvider(c.ServerName)
			if err != nil {
				logger.Error("error while looking up per-host provider for attempted incoming TLS connection",
					tag.ServerName(c.ServerName), tag.Address(remoteAddress), tag.Error(err))
				return nil, err
			}

			if perHostCertProvider != nil {
				return getServerTLSConfigFromCertProvider(perHostCertProvider, hostClientAuthRequired, remoteAddress, c.ServerName, logger)
			}
			logger.Warn("cannot find a per-host provider for attempted incoming TLS connection. returning default TLS configuration",
				tag.ServerName(c.ServerName), tag.Address(remoteAddress))
			return getServerTLSConfigFromCertProvider(certProvider, clientAuthRequired, remoteAddress, c.ServerName, logger)
		}
		return getServerTLSConfigFromCertProvider(certProvider, clientAuthRequired, remoteAddress, c.ServerName, logger)
	}

	return tlsConfig, nil
}

func getServerTLSConfigFromCertProvider(
	certProvider CertProvider,
	requireClientAuth bool,
	remoteAddress string,
	serverName string,
	logger log.Logger) (*tls.Config, error) {

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
	if remoteAddress != "" { // remoteAddress=="" when we return initial tls.Config object when configuring server
		logger.Debug("returning TLS config for connection", tag.Address(remoteAddress), tag.ServerName(serverName))
	}
	return auth.NewTLSConfigWithCertsAndCAs(
		clientAuthType,
		[]tls.Certificate{*serverCert},
		clientCaPool,
		logger), nil
}

func newClientTLSConfig(
	clientProvider CertProvider,
	serverName string,
	isAuthRequired bool,
	isWorker bool,
	enableHostVerification bool,
) (*tls.Config, error) {
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
			return
		case <-s.ticker.C:
		}

		s.checkCertExpiration()
	}
}

func (s *localStoreTlsProvider) checkCertExpiration() {
	defer func() {
		var retError error
		log.CapturePanic(s.logger, &retError)
	}()

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
			return
		}
		if s.scope != nil {
			s.scope.UpdateGauge(metrics.TlsCertsExpired, float64(len(expired)))
			s.scope.UpdateGauge(metrics.TlsCertsExpiring, float64(len(expiring)))
		}
		s.logCerts(expired, true, errorTime)
		s.logCerts(expiring, false, errorTime)
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

func isSystemWorker(tls *config.RootTLS) bool {
	return tls.SystemWorker.CertData != "" || tls.SystemWorker.CertFile != "" ||
		len(tls.SystemWorker.Client.RootCAData) > 0 || len(tls.SystemWorker.Client.RootCAFiles) > 0 ||
		tls.SystemWorker.Client.ForceTLS
}
