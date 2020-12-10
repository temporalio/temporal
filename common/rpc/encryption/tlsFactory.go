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

type (
	// TLSConfigProvider serves as a common interface to read server and client configuration for TLS.
	TLSConfigProvider interface {
		GetInternodeServerConfig() (*tls.Config, error)
		GetInternodeClientConfig() (*tls.Config, error)
		GetFrontendServerConfig() (*tls.Config, error)
		GetFrontendClientConfig() (*tls.Config, error)
	}

	// CertProvider is a common interface to load raw TLS/X509 primitives.
	CertProvider interface {
		FetchServerCertificate() (*tls.Certificate, error)
		FetchClientCAs() (*x509.CertPool, error)
		GetSettings() *config.GroupTLS
	}

	// ClientCertProvider is an interface to load raw TLS/X509 primitives for configuring clients.
	ClientCertProvider interface {
		FetchClientCertificate(isWorker bool) (*tls.Certificate, error)
		FetchServerRootCAsForClient(isWorker bool) (*x509.CertPool, error)
		ServerName(isWorker bool) string
		DisableHostVerification(isWorker bool) bool
	}

	// PerHostCertProviderFactory creates a CertProvider in the context of a specific Domain.
	PerHostCertProviderFactory interface {
		GetCertProvider(hostName string) (CertProvider, error)
	}

	tlsConfigConstructor func() (*tls.Config, error)
)

// NewTLSConfigProviderFromConfig creates a new TLS Config provider from RootTLS config
func NewTLSConfigProviderFromConfig(encryptionSettings config.RootTLS) (TLSConfigProvider, error) {
	return NewLocalStoreTlsProvider(&encryptionSettings)
}
