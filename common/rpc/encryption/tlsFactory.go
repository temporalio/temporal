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
	"time"

	"github.com/uber-go/tally"

	"go.temporal.io/server/common/config"
)

type (
	// TLSConfigProvider serves as a common interface to read server and client configuration for TLS.
	TLSConfigProvider interface {
		GetInternodeServerConfig() (*tls.Config, error)
		GetInternodeClientConfig() (*tls.Config, error)
		GetFrontendServerConfig() (*tls.Config, error)
		GetFrontendClientConfig() (*tls.Config, error)
		GetExpiringCerts(timeWindow time.Duration) (expiring CertExpirationMap, expired CertExpirationMap, err error)
	}

	// CertProvider is a common interface to load raw TLS/X509 primitives.
	CertProvider interface {
		FetchServerCertificate() (*tls.Certificate, error)
		FetchClientCAs() (*x509.CertPool, error)
		FetchClientCertificate(isWorker bool) (*tls.Certificate, error)
		FetchServerRootCAsForClient(isWorker bool) (*x509.CertPool, error)
		GetExpiringCerts(timeWindow time.Duration) (expiring CertExpirationMap, expired CertExpirationMap, err error)
	}

	// PerHostCertProviderMap returns a CertProvider for a given host name.
	PerHostCertProviderMap interface {
		GetCertProvider(hostName string) (provider CertProvider, clientAuthRequired bool, err error)
		GetExpiringCerts(timeWindow time.Duration) (expiring CertExpirationMap, expired CertExpirationMap, err error)
	}

	CertThumbprint [16]byte

	CertExpirationData struct {
		Thumbprint CertThumbprint
		IsCA       bool
		DNSNames   []string
		Expiration time.Time
	}

	CertExpirationMap map[CertThumbprint]CertExpirationData

	CertExpirationChecker interface {
		GetExpiringCerts(timeWindow time.Duration) (expiring CertExpirationMap, expired CertExpirationMap, err error)
	}

	tlsConfigConstructor func() (*tls.Config, error)
)

// NewTLSConfigProviderFromConfig creates a new TLS Config provider from RootTLS config.
// A custom cert provider factory can be optionally injected via certProviderFactory argument.
// Otherwise, it defaults to using localStoreCertProvider
func NewTLSConfigProviderFromConfig(
	encryptionSettings config.RootTLS,
	scope tally.Scope,
	certProviderFactory CertProviderFactory,
) (TLSConfigProvider, error) {

	if certProviderFactory == nil {
		certProviderFactory = NewLocalStoreCertProvider
	}
	return NewLocalStoreTlsProvider(&encryptionSettings, scope, certProviderFactory)
}
