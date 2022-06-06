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
	"strings"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"

	"go.temporal.io/server/common/config"
)

type (
	// TLSConfigProvider serves as a common interface to read server and client configuration for TLS.
	TLSConfigProvider interface {
		GetInternodeServerConfig() (*tls.Config, error)
		GetInternodeClientConfig() (*tls.Config, error)
		GetFrontendServerConfig() (*tls.Config, error)
		GetFrontendClientConfig() (*tls.Config, error)
		GetRemoteClusterClientConfig(hostname string) (*tls.Config, error)
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
		NumberOfHosts() int
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
	client metrics.Client,
	logger log.Logger,
	certProviderFactory CertProviderFactory,
) (TLSConfigProvider, error) {
	if err := validateRootTLS(&encryptionSettings); err != nil {
		return nil, err
	}
	if certProviderFactory == nil {
		certProviderFactory = NewLocalStoreCertProvider
	}
	return NewLocalStoreTlsProvider(&encryptionSettings, client.Scope(metrics.ServerTlsScope), logger, certProviderFactory)
}

func validateRootTLS(cfg *config.RootTLS) error {
	if err := validateGroupTLS(&cfg.Internode); err != nil {
		return err
	}
	if err := validateGroupTLS(&cfg.Frontend); err != nil {
		return err
	}
	if err := validateWorkerTLS(&cfg.SystemWorker); err != nil {
		return err
	}
	return nil
}

func validateGroupTLS(cfg *config.GroupTLS) error {
	if err := validateServerTLS(&cfg.Server); err != nil {
		return err
	}
	if err := validateClientTLS(&cfg.Client); err != nil {
		return err
	}
	for host, hostConfig := range cfg.PerHostOverrides {

		if strings.TrimSpace(host) == "" {
			return fmt.Errorf("host name cannot be empty string")
		}
		if err := validateServerTLS(&hostConfig); err != nil {
			return err
		}
	}
	return nil
}

func validateWorkerTLS(cfg *config.WorkerTLS) error {
	if cfg.CertFile != "" && cfg.CertData != "" {
		return fmt.Errorf("cannot specify CertFile and CertData at the same time")
	}
	if cfg.KeyFile != "" && cfg.KeyData != "" {
		return fmt.Errorf("cannot specify KeyFile and KeyData at the same time")
	}
	if err := validateClientTLS(&cfg.Client); err != nil {
		return err
	}
	return nil
}

func validateServerTLS(cfg *config.ServerTLS) error {
	if cfg.CertFile != "" && cfg.CertData != "" {
		return fmt.Errorf("cannot specify CertFile and CertData at the same time")
	}
	if cfg.KeyFile != "" && cfg.KeyData != "" {
		return fmt.Errorf("cannot specify KeyFile and KeyData at the same time")
	}
	if err := validateCAs(cfg.ClientCAData); err != nil {
		return fmt.Errorf("invalid ServerTLS.ClientCAData: %w", err)
	}
	if err := validateCAs(cfg.ClientCAFiles); err != nil {
		return fmt.Errorf("invalid ServerTLS.ClientCAFiles: %w", err)
	}
	if len(cfg.ClientCAFiles) > 0 && len(cfg.ClientCAData) > 0 {
		return fmt.Errorf("cannot specify ClientCAFiles and ClientCAData at the same time")
	}
	return nil
}

func validateClientTLS(cfg *config.ClientTLS) error {
	if err := validateCAs(cfg.RootCAData); err != nil {
		return fmt.Errorf("invalid ClientTLS.RootCAData: %w", err)
	}
	if err := validateCAs(cfg.RootCAFiles); err != nil {
		return fmt.Errorf("invalid ClientTLS.RootCAFiles: %w", err)
	}
	if len(cfg.RootCAData) > 0 && len(cfg.RootCAFiles) > 0 {
		return fmt.Errorf("cannot specify RootCAFiles and RootCAData at the same time")
	}
	return nil
}

func validateCAs(cas []string) error {
	for _, ca := range cas {
		if strings.TrimSpace(ca) == "" {
			return fmt.Errorf("CA cannot be empty string")
		}
	}
	return nil
}
