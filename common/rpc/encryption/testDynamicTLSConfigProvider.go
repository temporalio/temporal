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

type testDynamicTLSConfigProvider struct {
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

func (t *testDynamicTLSConfigProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return newServerTLSConfig(t.internodeCertProvider, nil)
}

func (t *testDynamicTLSConfigProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return newClientTLSConfig(t.internodeClientCertProvider, true, false)
}

func (t *testDynamicTLSConfigProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return newServerTLSConfig(t.frontendCertProvider, t.frontendPerHostCertProviderFactory)
}

func (t *testDynamicTLSConfigProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return newClientTLSConfig(t.workerCertProvider, true, false)
}

var _ TLSConfigProvider = (*testDynamicTLSConfigProvider)(nil)

func NewTestDynamicTLSConfigProvider(
	tlsConfig *config.RootTLS,
	internodeCerts []*tls.Certificate,
	internodeCACerts *x509.CertPool,
	frontendCerts []*tls.Certificate,
	frontendCACerts *x509.CertPool,
	wrongCACerts *x509.CertPool,
) (TLSConfigProvider, error) {

	internodeProvider := NewTestDynamicCertProvider(internodeCerts, internodeCACerts, wrongCACerts, tlsConfig.Internode)
	frontendProvider := NewTestDynamicCertProvider(frontendCerts, frontendCACerts, wrongCACerts, tlsConfig.Frontend)

	return &testDynamicTLSConfigProvider{
		internodeCertProvider:              internodeProvider,
		internodeClientCertProvider:        internodeProvider,
		frontendCertProvider:               frontendProvider,
		workerCertProvider:                 frontendProvider,
		frontendPerHostCertProviderFactory: frontendProvider,
		settings:                           tlsConfig,
	}, nil
}
