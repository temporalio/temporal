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
	if s.tlsSettings.Server.CertFile == "" && s.tlsSettings.Server.CertData == "" {
		return nil, nil
	}

	if s.tlsSettings.Server.CertFile != "" && s.tlsSettings.Server.CertData != "" {
		return nil, errors.New("Cannot specify both certFile and certData properties")
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

	var certBytes []byte
	var keyBytes []byte
	var err error

	if s.tlsSettings.Server.CertFile != "" {
		certBytes, err = ioutil.ReadFile(s.tlsSettings.Server.CertFile)
		if err != nil {
			return nil, err
		}
	} else if s.tlsSettings.Server.CertData != "" {
		certBytes, err = base64.StdEncoding.DecodeString(s.tlsSettings.Server.CertData)
		if err != nil {
			return nil, fmt.Errorf("TLS public certificate could not be decoded: %w", err)
		}
	}

	if s.tlsSettings.Server.KeyFile != "" {
		keyBytes, err = ioutil.ReadFile(s.tlsSettings.Server.KeyFile)
		if err != nil {
			return nil, err
		}
	} else if s.tlsSettings.Server.KeyData != "" {
		keyBytes, err = base64.StdEncoding.DecodeString(s.tlsSettings.Server.KeyData)
		if err != nil {
			return nil, fmt.Errorf("TLS private key could not be decoded: %w", err)
		}
	}

	serverCert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("loading server tls certificate failed: %v", err)
	}

	s.serverCert = &serverCert
	return s.serverCert, nil
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

func (s *localStoreCertProvider) FetchServerRootCAsForClient() (*x509.CertPool, error) {
	if len(s.tlsSettings.Client.RootCAFiles) == 0 && len(s.tlsSettings.Client.RootCAData) == 0 {
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

	serverCAPoolFromFiles, err := buildCAPoolFromFiles(s.tlsSettings.Client.RootCAFiles)
	if err != nil {
		return nil, err
	}

	serverCAPoolFromData, err := buildCAPoolFromData(s.tlsSettings.Client.RootCAData)
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
