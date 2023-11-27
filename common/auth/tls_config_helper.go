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

package auth

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// Helper methods for creating tls.Config structs to ensure MinVersion is 1.3

func NewEmptyTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{
			"h2",
		},
	}
}

func NewTLSConfigForServer(
	serverName string,
	enableHostVerification bool,
) *tls.Config {
	c := NewEmptyTLSConfig()
	c.ServerName = serverName
	c.InsecureSkipVerify = !enableHostVerification
	return c
}

func NewDynamicTLSClientConfig(
	getCert func() (*tls.Certificate, error),
	rootCAs *x509.CertPool,
	serverName string,
	enableHostVerification bool,
) *tls.Config {
	c := NewTLSConfigForServer(serverName, enableHostVerification)

	if getCert != nil {
		c.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return getCert()
		}
	}
	c.RootCAs = rootCAs

	return c
}

func NewTLSConfigWithCertsAndCAs(
	clientAuth tls.ClientAuthType,
	certificates []tls.Certificate,
	clientCAs *x509.CertPool,
	logger log.Logger,
) *tls.Config {
	c := NewEmptyTLSConfig()
	c.ClientAuth = clientAuth
	c.Certificates = certificates
	c.ClientCAs = clientCAs
	c.VerifyConnection = func(state tls.ConnectionState) error {
		logger.Debug("successfully established incoming TLS connection", tag.ServerName(state.ServerName), tag.Name(tlsCN(state)))
		return nil
	}
	return c
}

func tlsCN(state tls.ConnectionState) string {

	if len(state.PeerCertificates) == 0 {
		return ""
	}
	return state.PeerCertificates[0].Subject.CommonName
}

func NewTLSConfig(temporalTls *TLS) (*tls.Config, error) {
	if temporalTls == nil || !temporalTls.Enabled {
		return nil, nil
	}
	if temporalTls.CertData != "" && temporalTls.CertFile != "" {
		return nil, errors.New("only one of certData or certFile properties should be specified")
	}

	if temporalTls.KeyData != "" && temporalTls.KeyFile != "" {
		return nil, errors.New("only one of keyData or keyFile properties should be specified")
	}

	if temporalTls.CaData != "" && temporalTls.CaFile != "" {
		return nil, errors.New("only one of caData or caFile properties should be specified")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: !temporalTls.EnableHostVerification,
	}
	if temporalTls.ServerName != "" {
		tlsConfig.ServerName = temporalTls.ServerName
	}

	// Load CA cert
	var caBytes []byte
	var err error
	if temporalTls.CaFile != "" {
		caBytes, err = os.ReadFile(temporalTls.CaFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read client ca file: %w", err)
		}
	} else if temporalTls.CaData != "" {
		caBytes, err = base64.StdEncoding.DecodeString(temporalTls.CaData)
		if err != nil {
			return nil, fmt.Errorf("unable to decode client ca data: %w", err)
		}
	}
	if len(caBytes) > 0 {
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caBytes) {
			return nil, errors.New("failed to load decoded CA Cert as PEM")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client cert
	var certBytes []byte
	var keyBytes []byte
	if temporalTls.CertFile != "" {
		certBytes, err = os.ReadFile(temporalTls.CertFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read client certificate file: %w", err)
		}
	} else if temporalTls.CertData != "" {
		certBytes, err = base64.StdEncoding.DecodeString(temporalTls.CertData)
		if err != nil {
			return nil, fmt.Errorf("unable to decode client certificate: %w", err)
		}
	}

	if temporalTls.KeyFile != "" {
		keyBytes, err = os.ReadFile(temporalTls.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read client certificate private key file: %w", err)
		}
	} else if temporalTls.KeyData != "" {
		keyBytes, err = base64.StdEncoding.DecodeString(temporalTls.KeyData)
		if err != nil {
			return nil, fmt.Errorf("unable to decode client certificate private key: %w", err)
		}
	}

	if len(certBytes) > 0 {
		clientCert, err := tls.X509KeyPair(certBytes, keyBytes)
		if err != nil {
			return nil, fmt.Errorf("unable to generate x509 key pair: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	return tlsConfig, nil
}
