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

	"go.temporal.io/server/common/service/config"
)

type TestDynamicCertProvider struct {
	serverCerts     []*tls.Certificate
	caCerts         *x509.CertPool
	wrongCACerts    *x509.CertPool
	serverCertIndex int
	caCertIndex     int
	config          *config.GroupTLS
}

var _ CertProvider = (*TestDynamicCertProvider)(nil)
var _ ClientCertProvider = (*TestDynamicCertProvider)(nil)
var _ PerHostCertProviderFactory = (*TestDynamicCertProvider)(nil)

func NewTestDynamicCertProvider(
	serverCerts []*tls.Certificate,
	caCerts *x509.CertPool,
	wrongCACerts *x509.CertPool,
	config config.GroupTLS) *TestDynamicCertProvider {

	return &TestDynamicCertProvider{
		serverCerts:  serverCerts,
		caCerts:      caCerts,
		wrongCACerts: wrongCACerts,
		config:       &config,
	}
}

func (t *TestDynamicCertProvider) FetchServerCertificate() (*tls.Certificate, error) {
	i := t.serverCertIndex % len(t.serverCerts)
	t.serverCertIndex++
	return t.serverCerts[i], nil
}

func (t *TestDynamicCertProvider) FetchClientCAs() (*x509.CertPool, error) {
	panic("not implemented")
}

func (t *TestDynamicCertProvider) GetSettings() *config.GroupTLS {
	return t.config
}

func (t *TestDynamicCertProvider) FetchClientCertificate(_ bool) (*tls.Certificate, error) {
	panic("not implemented")
}

func (t *TestDynamicCertProvider) FetchServerRootCAsForClient(_ bool) (*x509.CertPool, error) {
	return t.caCerts, nil
}

func (t *TestDynamicCertProvider) ServerName(_ bool) string {
	return "localhost"
}

func (t *TestDynamicCertProvider) DisableHostVerification(_ bool) bool {
	return false
}

func (t *TestDynamicCertProvider) GetCertProvider(hostName string) (CertProvider, error) {
	return t, nil
}

func (t *TestDynamicCertProvider) SwitchToWrongServerRootCACerts() {
	t.caCerts = t.wrongCACerts
}
