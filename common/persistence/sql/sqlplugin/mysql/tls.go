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

package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/go-sql-driver/mysql"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

func registerTLSConfig(cfg *config.SQL) error {
	if cfg.TLS == nil || !cfg.TLS.Enabled {
		return nil
	}

	// TODO: create a way to set MinVersion and CipherSuites via cfg.
	tlsConfig := auth.NewTLSConfigForServer(cfg.TLS.ServerName, cfg.TLS.EnableHostVerification)

	if cfg.TLS.CaFile != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.TLS.CaFile)
		if err != nil {
			return fmt.Errorf("failed to load CA files: %v", err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return fmt.Errorf("failed to append CA file")
		}
		tlsConfig.RootCAs = rootCertPool
	}

	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		clientCert := make([]tls.Certificate, 0, 1)
		certs, err := tls.LoadX509KeyPair(
			cfg.TLS.CertFile,
			cfg.TLS.KeyFile,
		)
		if err != nil {
			return fmt.Errorf("failed to load tls x509 key pair: %v", err)
		}
		clientCert = append(clientCert, certs)
		tlsConfig.Certificates = clientCert
	}

	// In order to use the TLS configuration you need to register it. Once registered you use it by specifying
	// `tls` in the connect attributes.
	err := mysql.RegisterTLSConfig(customTLSName, tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to register tls config: %v", err)
	}

	if cfg.ConnectAttributes == nil {
		cfg.ConnectAttributes = map[string]string{}
	}

	// If no `tls` connect attribute is provided then we override it to our newly registered tls config automatically.
	// This allows users to simply provide a tls config without needing to remember to also set the connect attribute
	if cfg.ConnectAttributes["tls"] == "" {
		cfg.ConnectAttributes["tls"] = customTLSName
	}

	return nil
}
