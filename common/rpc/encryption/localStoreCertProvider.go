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
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"go.temporal.io/server/common/service/config"
)

var _ CertProvider = (*localStoreCertProvider)(nil)
var _ ClientCertProvider = (*localStoreCertProvider)(nil)
var _ CertExpirationChecker = (*localStoreCertProvider)(nil)

type localStoreCertProvider struct {
	sync.RWMutex

	tlsSettings       *config.GroupTLS
	workerTLSSettings *config.WorkerTLS

	serverCert          *tls.Certificate
	clientCert          *tls.Certificate
	clientCAs           *x509.CertPool
	serverCAs           *x509.CertPool
	serverCAsWorker     *x509.CertPool
	clientCACerts       []*x509.Certificate // copies of certs in the clientCAs CertPool for expiration checks
	serverCACerts       []*x509.Certificate // copies of certs in the serverCAs CertPool for expiration checks
	serverCACertsWorker []*x509.Certificate // copies of certs in the serverCAsWorker CertPool for expiration checks

	isLegacyWorkerConfig bool
	legacyWorkerSettings *config.ClientTLS
}

type loadOrDecodeDataFunc func(item string) ([]byte, error)

type x509CertFetcher func() (*x509.Certificate, error)
type x509CertPoolFetcher func() (*x509.CertPool, error)
type tlsCertFetcher func() (*tls.Certificate, error)

func (s *localStoreCertProvider) GetSettings() *config.GroupTLS {
	return s.tlsSettings
}

func (s *localStoreCertProvider) FetchServerCertificate() (*tls.Certificate, error) {

	if s.tlsSettings == nil {
		return nil, nil
	}
	return s.FetchCertificate(&s.serverCert, s.tlsSettings.Server.CertFile, s.tlsSettings.Server.CertData,
		s.tlsSettings.Server.KeyFile, s.tlsSettings.Server.KeyData)
}

func (s *localStoreCertProvider) FetchClientCAs() (*x509.CertPool, error) {

	if s.tlsSettings == nil {
		return nil, nil
	}
	return s.fetchCAs(
		s.tlsSettings.Server.ClientCAFiles,
		s.tlsSettings.Server.ClientCAData,
		&s.clientCAs,
		&s.clientCACerts,
		"cannot specify both clientCAFiles and clientCAData properties")
}

func (s *localStoreCertProvider) FetchServerRootCAsForClient(isWorker bool) (*x509.CertPool, error) {

	clientSettings := s.getClientTLSSettings(isWorker)
	if clientSettings == nil {
		return nil, nil
	}
	rootCAFiles := clientSettings.RootCAFiles
	rootCAData := clientSettings.RootCAData

	var cached **x509.CertPool
	var certs *[]*x509.Certificate
	if isWorker {
		cached = &s.serverCAsWorker
		certs = &s.serverCACertsWorker
	} else {
		cached = &s.serverCAs
		certs = &s.serverCACerts
	}

	return s.fetchCAs(
		rootCAFiles,
		rootCAData,
		cached,
		certs,
		"cannot specify both rootCAFiles and rootCAData properties")
}

func (s *localStoreCertProvider) fetchCAs(
	files []string,
	data []string,
	cached **x509.CertPool,
	certs *[]*x509.Certificate,
	duplicateErrorMessage string) (*x509.CertPool, error) {
	if len(files) == 0 && len(data) == 0 {
		return nil, nil
	}

	s.RLock()
	if *cached != nil {
		defer s.RUnlock()
		return *cached, nil
	}

	s.RUnlock()
	s.Lock()
	defer s.Unlock()

	if *cached != nil {
		return *cached, nil
	}

	caPoolFromFiles, caCertsFromFiles, err := buildCAPoolFromFiles(files)
	if err != nil {
		return nil, err
	}

	caPoolFromData, caCertsFromData, err := buildCAPoolFromData(data)
	if err != nil {
		return nil, err
	}

	if caPoolFromFiles != nil && caPoolFromData != nil {
		return nil, errors.New(duplicateErrorMessage)
	}

	if caPoolFromData != nil {
		*cached = caPoolFromData
		*certs = caCertsFromData
	} else {
		*cached = caPoolFromFiles
		*certs = caCertsFromFiles
	}

	return *cached, nil
}

func (s *localStoreCertProvider) FetchClientCertificate(isWorker bool) (*tls.Certificate, error) {
	if isWorker {
		return s.fetchWorkerCertificate()
	}
	if s.tlsSettings == nil {
		return nil, nil
	}
	return s.FetchCertificate(&s.clientCert, s.tlsSettings.Server.CertFile, s.tlsSettings.Server.CertData,
		s.tlsSettings.Server.KeyFile, s.tlsSettings.Server.KeyData)
}

func (s *localStoreCertProvider) fetchWorkerCertificate() (*tls.Certificate, error) {
	if s.isLegacyWorkerConfig {
		return s.FetchCertificate(&s.clientCert, s.tlsSettings.Server.CertFile, s.tlsSettings.Server.CertData,
			s.tlsSettings.Server.KeyFile, s.tlsSettings.Server.KeyData)
	} else {
		if s.workerTLSSettings != nil {
			return s.FetchCertificate(&s.clientCert, s.workerTLSSettings.CertFile, s.workerTLSSettings.CertData,
				s.workerTLSSettings.KeyFile, s.workerTLSSettings.KeyData)
		}
		return nil, nil
	}
}

func (s *localStoreCertProvider) FetchCertificate(cachedCert **tls.Certificate,
	certFile string, certData string,
	keyFile string, keyData string) (*tls.Certificate, error) {
	if certFile == "" && certData == "" {
		return nil, nil
	}

	if certFile != "" && certData != "" {
		return nil, errors.New("Cannot specify both certFile and certData properties")
	}

	s.RLock()
	if *cachedCert != nil {
		defer s.RUnlock()
		return *cachedCert, nil
	}

	s.RUnlock()
	s.Lock()
	defer s.Unlock()

	if *cachedCert != nil {
		return *cachedCert, nil
	}

	var certBytes []byte
	var keyBytes []byte
	var err error

	if certFile != "" {
		certBytes, err = ioutil.ReadFile(certFile)
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
		keyBytes, err = ioutil.ReadFile(keyFile)
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

	*cachedCert = &cert
	return *cachedCert, nil
}

func (s *localStoreCertProvider) ServerName(isWorker bool) string {
	return s.getClientTLSSettings(isWorker).ServerName
}

func (s *localStoreCertProvider) DisableHostVerification(isWorker bool) bool {
	return s.getClientTLSSettings(isWorker).DisableHostVerification
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

func (s *localStoreCertProvider) Expiring(fromNow time.Duration,
) (expiring CertExpirationMap, expired CertExpirationMap, err error) {

	expiring = make(CertExpirationMap)
	expired = make(CertExpirationMap)
	when := time.Now().UTC().Add(fromNow)

	checkError := checkTLSCertForExpiration(s.FetchServerCertificate, when, expiring, expired)
	err = appendError(err, checkError)
	checkError = checkTLSCertForExpiration(s.fetchClientCert, when, expiring, expired)
	err = appendError(err, checkError)
	checkError = checkTLSCertForExpiration(s.fetchWorkerCert, when, expiring, expired)
	err = appendError(err, checkError)

	// load CA certs, so that they are cached in memory
	checkError = loadCAsAndCaptureErrors(s.FetchClientCAs)
	err = appendError(err, checkError)
	checkCertsForExpiration(s.clientCACerts, when, expiring, expired)

	checkError = loadCAsAndCaptureErrors(func() (*x509.CertPool, error) {
		return s.FetchServerRootCAsForClient(false)
	})
	err = appendError(err, checkError)
	checkCertsForExpiration(s.serverCACerts, when, expiring, expired)

	checkError = loadCAsAndCaptureErrors(func() (*x509.CertPool, error) {
		return s.FetchServerRootCAsForClient(true)
	})
	err = appendError(err, checkError)
	checkCertsForExpiration(s.serverCACertsWorker, when, expiring, expired)

	return expiring, expired, err
}

func (s *localStoreCertProvider) fetchClientCert() (*tls.Certificate, error) {
	return s.FetchClientCertificate(false)
}

func (s *localStoreCertProvider) fetchWorkerCert() (*tls.Certificate, error) {
	return s.FetchClientCertificate(true)
}

func checkTLSCertForExpiration(
	fetchCert tlsCertFetcher,
	time time.Time,
	expiring CertExpirationMap,
	expired CertExpirationMap,
) error {

	return fetchAndCheckCertForExpiration(
		func() (*x509.Certificate, error) { return fetchAndParseTLSCert(fetchCert) },
		time, expiring, expired)
}

func fetchAndParseTLSCert(fetchCert tlsCertFetcher) (*x509.Certificate, error) {
	cert, err := fetchCert()
	if err != nil {
		return nil, err
	}
	if cert == nil {
		return nil, nil
	}
	return x509.ParseCertificate(cert.Certificate[0])
}

func fetchAndCheckCertForExpiration(
	fetchCert x509CertFetcher,
	when time.Time,
	expiring CertExpirationMap,
	expired CertExpirationMap,
) error {

	cert, err := fetchCert()
	checkCertForExpiration(cert, when, expiring, expired)
	return err
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
	when time.Time,
	expiring CertExpirationMap,
	expired CertExpirationMap,
) {

	if cert != nil && cert.NotAfter.Before(when) {
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

func buildCAPoolFromData(caData []string) (*x509.CertPool, []*x509.Certificate, error) {

	return buildCAPool(caData, base64.StdEncoding.DecodeString)
}

func buildCAPoolFromFiles(caFiles []string) (*x509.CertPool, []*x509.Certificate, error) {

	return buildCAPool(caFiles, ioutil.ReadFile)
}

func buildCAPool(cas []string, getBytes loadOrDecodeDataFunc) (*x509.CertPool, []*x509.Certificate, error) {
	atLeastOneCert := false
	caPool := x509.NewCertPool()
	var certs []*x509.Certificate

	for _, ca := range cas {
		if ca == "" {
			continue
		}

		caBytes, err := getBytes(ca)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode ca cert: %w", err)
		}

		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, nil, errors.New("unknown failure constructing cert pool for ca")
		}

		cert, err := parseCert(caBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse x509 certificate: %w", err)
		}
		certs = append(certs, cert)
		atLeastOneCert = true
	}

	if !atLeastOneCert {
		return nil, nil, nil
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

	if len(certBytes[0]) == 0 {
		return nil, fmt.Errorf("failed to decode PEM certificate data")
	}
	return x509.ParseCertificate(certBytes[0])
}

func loadCAsAndCaptureErrors(getCAs x509CertPoolFetcher) error {

	_, err := getCAs()
	return err
}

func appendError(to error, from error) error {

	if from != nil {
		return multierror.Append(to, from)
	}
	return to
}
