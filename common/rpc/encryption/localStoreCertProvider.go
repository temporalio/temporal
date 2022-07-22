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
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ CertProvider = (*localStoreCertProvider)(nil)
var _ CertExpirationChecker = (*localStoreCertProvider)(nil)

type certCache struct {
	serverCert          *tls.Certificate
	workerCert          *tls.Certificate
	clientCAPool        *x509.CertPool
	serverCAPool        *x509.CertPool
	serverCAsWorkerPool *x509.CertPool
	clientCACerts       []*x509.Certificate // copies of certs in the clientCAPool CertPool for expiration checks
	serverCACerts       []*x509.Certificate // copies of certs in the serverCAPool CertPool for expiration checks
	serverCACertsWorker []*x509.Certificate // copies of certs in the serverCAsWorkerPool CertPool for expiration checks
}

type localStoreCertProvider struct {
	sync.RWMutex

	tlsSettings          *config.GroupTLS
	workerTLSSettings    *config.WorkerTLS
	isLegacyWorkerConfig bool
	legacyWorkerSettings *config.ClientTLS

	certs           *certCache
	refreshInterval time.Duration

	ticker *time.Ticker
	stop   chan bool
	logger log.Logger
}

type loadOrDecodeDataFunc func(item string) ([]byte, error)

type tlsCertFetcher func() (*tls.Certificate, error)

func (s *localStoreCertProvider) initialize() {

	if s.refreshInterval != 0 {
		s.stop = make(chan bool)
		s.ticker = time.NewTicker(s.refreshInterval)
		go s.refreshCerts()
	}
}

func NewLocalStoreCertProvider(
	tlsSettings *config.GroupTLS,
	workerTlsSettings *config.WorkerTLS,
	legacyWorkerSettings *config.ClientTLS,
	refreshInterval time.Duration,
	logger log.Logger) CertProvider {

	provider := &localStoreCertProvider{
		tlsSettings:          tlsSettings,
		workerTLSSettings:    workerTlsSettings,
		legacyWorkerSettings: legacyWorkerSettings,
		isLegacyWorkerConfig: legacyWorkerSettings != nil,
		logger:               logger,
		refreshInterval:      refreshInterval,
	}
	provider.initialize()
	return provider
}

func (s *localStoreCertProvider) Close() {

	if s.ticker != nil {
		s.ticker.Stop()
	}
	if s.stop != nil {
		s.stop <- true
		close(s.stop)
	}
}

func (s *localStoreCertProvider) FetchServerCertificate() (*tls.Certificate, error) {

	if s.tlsSettings == nil {
		return nil, nil
	}
	certs, err := s.getCerts()
	if err != nil {
		return nil, err
	}
	return certs.serverCert, nil
}

func (s *localStoreCertProvider) FetchClientCAs() (*x509.CertPool, error) {

	if s.tlsSettings == nil {
		return nil, nil
	}
	certs, err := s.getCerts()
	if err != nil {
		return nil, err
	}
	return certs.clientCAPool, nil
}

func (s *localStoreCertProvider) FetchServerRootCAsForClient(isWorker bool) (*x509.CertPool, error) {

	clientSettings := s.getClientTLSSettings(isWorker)
	if clientSettings == nil {
		return nil, nil
	}
	certs, err := s.getCerts()
	if err != nil {
		return nil, err
	}

	if isWorker {
		return certs.serverCAsWorkerPool, nil
	}

	return certs.serverCAPool, nil
}

func (s *localStoreCertProvider) FetchClientCertificate(isWorker bool) (*tls.Certificate, error) {

	if !s.isTLSEnabled() {
		return nil, nil
	}
	certs, err := s.getCerts()
	if err != nil {
		return nil, err
	}
	if isWorker {
		return certs.workerCert, nil
	}
	return certs.serverCert, nil
}

func (s *localStoreCertProvider) GetExpiringCerts(timeWindow time.Duration,
) (CertExpirationMap, CertExpirationMap, error) {

	expiring := make(CertExpirationMap)
	expired := make(CertExpirationMap)
	when := time.Now().UTC().Add(timeWindow)

	certs, err := s.getCerts()
	if err != nil {
		return nil, nil, err
	}

	checkError := checkTLSCertForExpiration(certs.serverCert, when, expiring, expired)
	err = appendError(err, checkError)
	checkError = checkTLSCertForExpiration(certs.workerCert, when, expiring, expired)
	err = appendError(err, checkError)

	checkCertsForExpiration(certs.clientCACerts, when, expiring, expired)
	checkCertsForExpiration(certs.serverCACerts, when, expiring, expired)
	checkCertsForExpiration(certs.serverCACertsWorker, when, expiring, expired)

	return expiring, expired, err
}

func (s *localStoreCertProvider) getCerts() (*certCache, error) {

	s.RLock()
	if s.certs != nil {
		defer s.RUnlock()
		return s.certs, nil
	}
	s.RUnlock()
	s.Lock()
	defer s.Unlock()

	if s.certs != nil {
		return s.certs, nil
	}

	newCerts, err := s.loadCerts()
	if err != nil {
		return nil, err
	}

	if newCerts == nil {
		s.certs = &certCache{}
	} else {
		s.certs = newCerts
	}
	return s.certs, nil
}

func (s *localStoreCertProvider) loadCerts() (*certCache, error) {

	if !s.isTLSEnabled() {
		return nil, nil
	}

	newCerts := certCache{}
	var err error

	if s.tlsSettings != nil {
		newCerts.serverCert, err = s.fetchCertificate(s.tlsSettings.Server.CertFile, s.tlsSettings.Server.CertData,
			s.tlsSettings.Server.KeyFile, s.tlsSettings.Server.KeyData)
		if err != nil {
			return nil, err
		}

		certPool, certs, err := s.fetchCAs(s.tlsSettings.Server.ClientCAFiles, s.tlsSettings.Server.ClientCAData,
			"cannot specify both clientCAFiles and clientCAData properties")
		if err != nil {
			return nil, err
		}
		newCerts.clientCAPool = certPool
		newCerts.clientCACerts = certs
	}

	if s.isLegacyWorkerConfig {
		newCerts.workerCert = newCerts.serverCert
	} else {
		if s.workerTLSSettings != nil {
			newCerts.workerCert, err = s.fetchCertificate(s.workerTLSSettings.CertFile, s.workerTLSSettings.CertData,
				s.workerTLSSettings.KeyFile, s.workerTLSSettings.KeyData)
			if err != nil {
				return nil, err
			}
		}
	}

	nonWorkerPool, nonWorkerCerts, err := s.loadServerCACerts(false)
	if err != nil {
		return nil, err
	}
	newCerts.serverCAPool = nonWorkerPool
	newCerts.serverCACerts = nonWorkerCerts

	workerPool, workerCerts, err := s.loadServerCACerts(true)
	if err != nil {
		return nil, err
	}
	newCerts.serverCAsWorkerPool = workerPool
	newCerts.serverCACertsWorker = workerCerts

	return &newCerts, nil
}

func (s *localStoreCertProvider) fetchCertificate(
	certFile string, certData string,
	keyFile string, keyData string) (*tls.Certificate, error) {
	if certFile == "" && certData == "" {
		return nil, nil
	}

	if certFile != "" && certData != "" {
		return nil, errors.New("only one of certFile or certData properties should be spcified")
	}

	var certBytes []byte
	var keyBytes []byte
	var err error

	if certFile != "" {
		s.logger.Info("loading certificate from file", tag.TLSCertFile(certFile))
		certBytes, err = os.ReadFile(certFile)
		if err != nil {
			return nil, err
		}
	} else if certData != "" {
		certBytes, err = base64.StdEncoding.DecodeString(certData)
		if err != nil {
			return nil, fmt.Errorf("TLS public certificate could not be decoded: %w", err)
		}
	}

	if keyFile != "" {
		s.logger.Info("loading private key from file", tag.TLSKeyFile(keyFile))
		keyBytes, err = os.ReadFile(keyFile)
		if err != nil {
			return nil, err
		}
	} else if keyData != "" {
		keyBytes, err = base64.StdEncoding.DecodeString(keyData)
		if err != nil {
			return nil, fmt.Errorf("TLS private key could not be decoded: %w", err)
		}
	}

	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("loading tls certificate failed: %v", err)
	}

	return &cert, nil
}

func (s *localStoreCertProvider) getClientTLSSettings(isWorker bool) *config.ClientTLS {
	if isWorker && s.workerTLSSettings != nil {
		return &s.workerTLSSettings.Client // explicit system worker case
	} else if isWorker {
		return s.legacyWorkerSettings // legacy config case when we use Frontend.Client settings
	} else {
		if s.tlsSettings == nil {
			return nil
		}
		return &s.tlsSettings.Client // internode client case
	}
}

func (s *localStoreCertProvider) loadServerCACerts(isWorker bool) (*x509.CertPool, []*x509.Certificate, error) {

	clientSettings := s.getClientTLSSettings(isWorker)
	if clientSettings == nil {
		return nil, nil, nil
	}

	return s.fetchCAs(clientSettings.RootCAFiles, clientSettings.RootCAData,
		"cannot specify both rootCAFiles and rootCAData properties")
}

func (s *localStoreCertProvider) fetchCAs(
	files []string,
	data []string,
	duplicateErrorMessage string) (*x509.CertPool, []*x509.Certificate, error) {
	if len(files) == 0 && len(data) == 0 {
		return nil, nil, nil
	}

	caPoolFromFiles, caCertsFromFiles, err := s.buildCAPoolFromFiles(files)
	if err != nil {
		return nil, nil, err
	}

	caPoolFromData, caCertsFromData, err := buildCAPoolFromData(data)
	if err != nil {
		return nil, nil, err
	}

	if caPoolFromFiles != nil && caPoolFromData != nil {
		return nil, nil, errors.New(duplicateErrorMessage)
	}

	var certPool *x509.CertPool
	var certs []*x509.Certificate

	if caPoolFromData != nil {
		certPool = caPoolFromData
		certs = caCertsFromData
	} else {
		certPool = caPoolFromFiles
		certs = caCertsFromFiles
	}

	return certPool, certs, nil
}

func checkTLSCertForExpiration(
	cert *tls.Certificate,
	when time.Time,
	expiring CertExpirationMap,
	expired CertExpirationMap,
) error {

	if cert == nil {
		return nil
	}

	x509cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	checkCertForExpiration(x509cert, when, expiring, expired)
	return nil
}

func checkCertsForExpiration(
	certs []*x509.Certificate,
	time time.Time,
	expiring CertExpirationMap,
	expired CertExpirationMap,
) {

	for _, cert := range certs {
		checkCertForExpiration(cert, time, expiring, expired)
	}
}

func checkCertForExpiration(
	cert *x509.Certificate,
	pointInTime time.Time,
	expiring CertExpirationMap,
	expired CertExpirationMap,
) {

	if cert != nil && expiresBefore(cert, pointInTime) {
		record := CertExpirationData{
			Thumbprint: md5.Sum(cert.Raw),
			IsCA:       cert.IsCA,
			DNSNames:   cert.DNSNames,
			Expiration: cert.NotAfter,
		}
		if record.Expiration.Before(time.Now().UTC()) {
			expired[record.Thumbprint] = record
		} else {
			expiring[record.Thumbprint] = record
		}
	}
}

func expiresBefore(cert *x509.Certificate, pointInTime time.Time) bool {
	return cert.NotAfter.Before(pointInTime)
}

func buildCAPoolFromData(caData []string) (*x509.CertPool, []*x509.Certificate, error) {

	return buildCAPool(caData, base64.StdEncoding.DecodeString)
}

func (s *localStoreCertProvider) buildCAPoolFromFiles(caFiles []string) (*x509.CertPool, []*x509.Certificate, error) {
	if len(caFiles) == 0 {
		return nil, nil, nil
	}

	s.logger.Info("loading CA certs from", tag.TLSCertFiles(caFiles))
	return buildCAPool(caFiles, os.ReadFile)
}

func buildCAPool(cas []string, getBytes loadOrDecodeDataFunc) (*x509.CertPool, []*x509.Certificate, error) {

	var caPool *x509.CertPool
	var certs []*x509.Certificate

	for _, ca := range cas {
		if ca == "" {
			continue
		}

		caBytes, err := getBytes(ca)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode ca cert: %w", err)
		}

		if caPool == nil {
			caPool = x509.NewCertPool()
		}
		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, nil, errors.New("unknown failure constructing cert pool for ca")
		}

		cert, err := parseCert(caBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse x509 certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	return caPool, certs, nil
}

// logic borrowed from tls.X509KeyPair()
func parseCert(bytes []byte) (*x509.Certificate, error) {

	var certBytes [][]byte
	for {
		var certDERBlock *pem.Block
		certDERBlock, bytes = pem.Decode(bytes)
		if certDERBlock == nil {
			break
		}
		if certDERBlock.Type == "CERTIFICATE" {
			certBytes = append(certBytes, certDERBlock.Bytes)
		}
	}

	if len(certBytes) == 0 || len(certBytes[0]) == 0 {
		return nil, fmt.Errorf("failed to decode PEM certificate data")
	}
	return x509.ParseCertificate(certBytes[0])
}

func appendError(aggregatedErr error, err error) error {
	if aggregatedErr == nil {
		return err
	}
	if err == nil {
		return aggregatedErr
	}
	return fmt.Errorf("%v, %w", aggregatedErr, err)
}

func (s *localStoreCertProvider) refreshCerts() {

	for {
		select {
		case <-s.stop:
			return
		case <-s.ticker.C:
		}

		newCerts, err := s.loadCerts()
		if err != nil {
			s.logger.Error("failed to load certificates", tag.Error(err))
			continue
		}

		s.RLock()
		currentCerts := s.certs
		s.RUnlock()
		if currentCerts.isEqual(newCerts) {
			continue
		}

		s.logger.Info("loaded new TLS certificates")
		s.Lock()
		s.certs = newCerts
		s.Unlock()
	}
}

func (s *localStoreCertProvider) isTLSEnabled() bool {
	return s.tlsSettings != nil || s.workerTLSSettings != nil
}

func (c *certCache) isEqual(other *certCache) bool {

	if c == other {
		return true
	}
	if c == nil || other == nil {
		return false
	}

	if !equalTLSCerts(c.serverCert, other.serverCert) ||
		!equalTLSCerts(c.workerCert, other.workerCert) ||
		!equalX509(c.clientCACerts, other.clientCACerts) ||
		!equalX509(c.serverCACerts, other.serverCACerts) ||
		!equalX509(c.serverCACertsWorker, other.serverCACertsWorker) {
		return false
	}
	return true
}

func equal(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func equalX509(a, b []*x509.Certificate) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func equalTLSCerts(a, b *tls.Certificate) bool {
	if a != nil {
		if b == nil || !equal(a.Certificate, b.Certificate) {
			return false
		}
	} else {
		if b != nil {
			return false
		}
	}
	return true
}
