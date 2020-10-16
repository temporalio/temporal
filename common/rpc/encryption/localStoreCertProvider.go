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

type localStoreCertProvider struct {
	sync.RWMutex

	tlsSettings *config.GroupTLS

	serverCert *tls.Certificate
	clientCAs  *x509.CertPool
	serverCAs  *x509.CertPool
}

func (s *localStoreCertProvider) GetSettings() *config.GroupTLS {
	return s.tlsSettings
}

func (s *localStoreCertProvider) FetchServerCertificate() (*tls.Certificate, error) {
	if !s.tlsSettings.Server.InlineData && s.tlsSettings.Server.CertFile == "" {
		return nil, nil
	}

	if s.tlsSettings.Server.InlineData && s.tlsSettings.Server.CertData == "" {
		return nil, nil
	}

	s.RLock()
	if s.serverCert != nil {
		defer s.RUnlock()
		return s.serverCert, nil
	}

	s.RUnlock()
	s.Lock()
	defer s.Unlock()

	if s.serverCert != nil {
		return s.serverCert, nil
	}

	if s.tlsSettings.Server.InlineData {
		return s.fetchServerCertificateFromInline()
	}

	return s.fetchServerCertificateFromFile()
}

func (s *localStoreCertProvider) fetchServerCertificateFromFile() (*tls.Certificate, error) {
	serverCert, err := tls.LoadX509KeyPair(s.tlsSettings.Server.CertFile, s.tlsSettings.Server.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading server tls certificate failed: %v", err)
	}

	s.serverCert = &serverCert
	return s.serverCert, nil
}

func (s *localStoreCertProvider) fetchServerCertificateFromInline() (*tls.Certificate, error) {
	if s.tlsSettings.Server.CertData == "" {
		return nil, nil
	}

	certBytes, err := base64.StdEncoding.DecodeString(s.tlsSettings.Server.CertData)
	if err != nil {
		return nil, fmt.Errorf("TLS public certificate could not be decoded: %w", err)
	}

	keyBytes, err := base64.StdEncoding.DecodeString(s.tlsSettings.Server.KeyData)
	if err != nil {
		return nil, fmt.Errorf("TLS certificate private key could not be decoded: %w", err)
	}

	serverCert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("loading server tls certificate failed: %v", err)
	}

	s.serverCert = &serverCert
	return s.serverCert, nil
}

func (s *localStoreCertProvider) FetchClientCAs() (*x509.CertPool, error) {
	if !s.tlsSettings.Server.InlineData && len(s.tlsSettings.Server.ClientCAFiles) == 0 {
		return nil, nil
	}
	if s.tlsSettings.Server.InlineData && len(s.tlsSettings.Server.ClientCaData) == 0 {
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

	if s.tlsSettings.Server.InlineData {
		return s.fetchClientCAsFromInline()
	}

	return s.fetchClientCAsFromFiles()
}

func (s *localStoreCertProvider) fetchClientCAsFromFiles() (*x509.CertPool, error) {
	if len(s.tlsSettings.Server.ClientCAFiles) == 0 {
		return nil, nil
	}

	var clientCaPool *x509.CertPool
	var err error
	clientCaPool, err = buildCAPool(s.tlsSettings.Server.ClientCAFiles)
	if err != nil {
		return nil, err
	}

	s.clientCAs = clientCaPool
	return s.clientCAs, nil
}

func (s *localStoreCertProvider) fetchClientCAsFromInline() (*x509.CertPool, error) {
	if len(s.tlsSettings.Server.ClientCaData) == 0 {
		return nil, nil
	}

	var clientCaPool *x509.CertPool
	var err error
	clientCaPool, err = buildCAPoolFromInline(s.tlsSettings.Server.ClientCaData)
	if err != nil {
		return nil, err
	}

	s.clientCAs = clientCaPool
	return s.clientCAs, nil
}

func (s *localStoreCertProvider) FetchServerRootCAsForClient() (*x509.CertPool, error) {
	if !s.tlsSettings.Client.InlineData && len(s.tlsSettings.Client.RootCAFiles) == 0 {
		return nil, nil
	}
	if s.tlsSettings.Client.InlineData && len(s.tlsSettings.Client.RootCAData) == 0 {
		return nil, nil
	}

	s.RLock()
	if s.serverCAs != nil {
		defer s.RUnlock()
		return s.clientCAs, nil
	}

	s.RUnlock()
	s.Lock()
	defer s.Unlock()

	if s.tlsSettings.Client.InlineData {
		return s.fetchServerRootCAsForClientFromInline()
	}

	return s.fetchServerRootCAsForClientFromFiles()
}

func (s *localStoreCertProvider) fetchServerRootCAsForClientFromFiles() (*x509.CertPool, error) {
	if len(s.tlsSettings.Client.RootCAFiles) == 0 {
		return nil, nil
	}

	var serverCAPool *x509.CertPool
	var err error
	serverCAPool, err = buildCAPool(s.tlsSettings.Client.RootCAFiles)
	if err != nil {
		return nil, err
	}

	s.serverCAs = serverCAPool
	return s.serverCAs, nil
}

func (s *localStoreCertProvider) fetchServerRootCAsForClientFromInline() (*x509.CertPool, error) {
	if len(s.tlsSettings.Client.RootCAData) == 0 {
		return nil, nil
	}

	var serverCAPool *x509.CertPool
	var err error
	serverCAPool, err = buildCAPoolFromInline(s.tlsSettings.Client.RootCAData)
	if err != nil {
		return nil, err
	}

	s.serverCAs = serverCAPool
	return s.serverCAs, nil
}

func buildCAPool(caFiles []string) (*x509.CertPool, error) {
	caPool := x509.NewCertPool()
	for _, ca := range caFiles {
		caBytes, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, fmt.Errorf("failed reading client ca cert: %v", err)
		}

		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("unknown failure constructing cert pool for ca")
		}
	}
	return caPool, nil
}

func buildCAPoolFromInline(caData []string) (*x509.CertPool, error) {
	caPool := x509.NewCertPool()
	for _, ca := range caData {
		caBytes, err := base64.StdEncoding.DecodeString(ca)
		if err != nil {
			return nil, fmt.Errorf("failed reading client ca cert: %v", err)
		}

		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("unknown failure constructing cert pool for ca")
		}
	}
	return caPool, nil
}
