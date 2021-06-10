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
