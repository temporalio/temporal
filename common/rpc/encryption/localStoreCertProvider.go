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
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"

	"go.temporal.io/server/common/service/config"
)

var _ CertProvider = (*localStoreCertProvider)(nil)
var _ ClientCertProvider = (*localStoreCertProvider)(nil)

type localStoreCertProvider struct {
	sync.RWMutex

	tlsSettings       *config.GroupTLS
	workerTLSSettings *config.WorkerTLS

	serverCert *tls.Certificate
	clientCert *tls.Certificate
	clientCAs  *x509.CertPool
	serverCAs  *x509.CertPool

	isLegacyWorkerConfig bool
	legacyWorkerSettings *config.ClientTLS
}

func (s *localStoreCertProvider) GetSettings() *config.GroupTLS {
	return s.tlsSettings
}

func (s *localStoreCertProvider) FetchServerCertificate() (*tls.Certificate, error) {
	return s.FetchCertificate(&s.serverCert, s.tlsSettings.Server.CertFile, s.tlsSettings.Server.CertData,
		s.tlsSettings.Server.KeyFile, s.tlsSettings.Server.KeyData)
}

func (s *localStoreCertProvider) FetchClientCAs() (*x509.CertPool, error) {
	if len(s.tlsSettings.Server.ClientCAFiles) == 0 && len(s.tlsSettings.Server.ClientCAData) == 0 {
		return nil, nil
	}

	s.RLock()
	if s.clientCAs != nil {
		defer s.RUnlock()
		return s.clientCAs, nil
	}

	s.RUnlock()
	s.Lock()
	defer s.Unlock()

	if s.clientCAs != nil {
		return s.clientCAs, nil
	}

	clientCaPoolFromFiles, err := buildCAPoolFromFiles(s.tlsSettings.Server.ClientCAFiles)
	if err != nil {
		return nil, err
	}

	clientCaPoolFromData, err := buildCAPoolFromData(s.tlsSettings.Server.ClientCAData)
	if err != nil {
		return nil, err
	}

	if clientCaPoolFromFiles != nil && clientCaPoolFromData != nil {
		return nil, errors.New("cannot specify both clientCAFiles and clientCAData properties")
	}

	if clientCaPoolFromData != nil {
		s.clientCAs = clientCaPoolFromData
	} else {
		s.clientCAs = clientCaPoolFromFiles
	}

	return s.clientCAs, nil
}

func (s *localStoreCertProvider) FetchServerRootCAsForClient(isWorker bool) (*x509.CertPool, error) {
	clientSettings := s.getClientTLSSettings(isWorker)
	rootCAFiles := clientSettings.RootCAFiles
	rootCAData := clientSettings.RootCAData

	if len(rootCAFiles) == 0 && len(rootCAData) == 0 {
		return nil, nil
	}

	s.RLock()
	if s.serverCAs != nil {
		defer s.RUnlock()
		return s.serverCAs, nil
	}

	s.RUnlock()
	s.Lock()
	defer s.Unlock()

	if s.serverCAs != nil {
		return s.serverCAs, nil
	}

	serverCAPoolFromFiles, err := buildCAPoolFromFiles(rootCAFiles)
	if err != nil {
		return nil, err
	}

	serverCAPoolFromData, err := buildCAPoolFromData(rootCAData)
	if err != nil {
		return nil, err
	}

	if serverCAPoolFromData != nil && serverCAPoolFromFiles != nil {
		return nil, errors.New("cannot specify both rootCAFiles and rootCAData properties")
	}

	if serverCAPoolFromData != nil {
		s.serverCAs = serverCAPoolFromData
	} else {
		s.serverCAs = serverCAPoolFromFiles
	}

	return s.serverCAs, nil
}

func (s *localStoreCertProvider) FetchClientCertificate() (*tls.Certificate, error) {
	return s.FetchCertificate(&s.clientCert, s.tlsSettings.Server.CertFile, s.tlsSettings.Server.CertData,
		s.tlsSettings.Server.KeyFile, s.tlsSettings.Server.KeyData)
}

func (s *localStoreCertProvider) FetchWorkerCertificate() (*tls.Certificate, error) {
	if s.isLegacyWorkerConfig {
		return s.FetchCertificate(&s.clientCert, s.tlsSettings.Server.CertFile, s.tlsSettings.Server.CertData,
			s.tlsSettings.Server.KeyFile, s.tlsSettings.Server.KeyData)
	} else {
		return s.FetchCertificate(&s.clientCert, s.workerTLSSettings.CertFile, s.workerTLSSettings.CertData,
			s.workerTLSSettings.KeyFile, s.workerTLSSettings.KeyData)
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
		return &s.tlsSettings.Client // internode client case
	}
}

func buildCAPoolFromData(caData []string) (*x509.CertPool, error) {
	atLeastOneCert := false
	caPool := x509.NewCertPool()

	for _, ca := range caData {
		if ca == "" {
			continue
		}

		caBytes, err := base64.StdEncoding.DecodeString(ca)
		if err != nil {
			return nil, fmt.Errorf("failed to decode ca cert: %v", err)
		}

		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("unknown failure constructing cert pool for ca")
		}

		atLeastOneCert = true
	}

	if !atLeastOneCert {
		return nil, nil
	}

	return caPool, nil
}

func buildCAPoolFromFiles(caFiles []string) (*x509.CertPool, error) {
	atLeastOneCert := false
	caPool := x509.NewCertPool()

	for _, ca := range caFiles {
		if ca == "" {
			continue
		}

		caBytes, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca cert from file '%v': %v", ca, err)
		}

		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("unknown failure constructing cert pool for ca")
		}

		atLeastOneCert = true
	}

	if !atLeastOneCert {
		return nil, nil
	}

	return caPool, nil
}
