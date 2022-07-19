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

package gocql

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/resolver"
)

func NewCassandraCluster(
	cfg config.Cassandra,
	resolver resolver.ServiceResolver,
) (*gocql.ClusterConfig, error) {
	var resolvedHosts []string
	for _, host := range parseHosts(cfg.Hosts) {
		resolvedHosts = append(resolvedHosts, resolver.Resolve(host)...)
	}
	cluster := gocql.NewCluster(resolvedHosts...)
	cluster.ProtoVersion = 4
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	if cfg.User != "" && cfg.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.User,
			Password: cfg.Password,
		}
	}
	if cfg.Keyspace != "" {
		cluster.Keyspace = cfg.Keyspace
	}
	if cfg.Datacenter != "" {
		cluster.HostFilter = gocql.DataCentreHostFilter(cfg.Datacenter)
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		if cfg.TLS.CertData != "" && cfg.TLS.CertFile != "" {
			return nil, errors.New("only one of certData or certFile properties should be specified")
		}

		if cfg.TLS.KeyData != "" && cfg.TLS.KeyFile != "" {
			return nil, errors.New("only one of keyData or keyFile properties should be specified")
		}

		if cfg.TLS.CaData != "" && cfg.TLS.CaFile != "" {
			return nil, errors.New("only one of caData or caFile properties should be specified")
		}

		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.TLS.CaFile,
			EnableHostVerification: cfg.TLS.EnableHostVerification,
			Config:                 auth.NewTLSConfigForServer(cfg.TLS.ServerName, cfg.TLS.EnableHostVerification),
		}

		var certBytes []byte
		var keyBytes []byte
		var err error

		if cfg.TLS.CertFile != "" {
			certBytes, err = os.ReadFile(cfg.TLS.CertFile)
			if err != nil {
				return nil, fmt.Errorf("error reading client certificate file: %w", err)
			}
		} else if cfg.TLS.CertData != "" {
			certBytes, err = base64.StdEncoding.DecodeString(cfg.TLS.CertData)
			if err != nil {
				return nil, fmt.Errorf("client certificate could not be decoded: %w", err)
			}
		}

		if cfg.TLS.KeyFile != "" {
			keyBytes, err = os.ReadFile(cfg.TLS.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("error reading client certificate private key file: %w", err)
			}
		} else if cfg.TLS.KeyData != "" {
			keyBytes, err = base64.StdEncoding.DecodeString(cfg.TLS.KeyData)
			if err != nil {
				return nil, fmt.Errorf("client certificate private key could not be decoded: %w", err)
			}
		}

		if len(certBytes) > 0 {
			clientCert, err := tls.X509KeyPair(certBytes, keyBytes)
			if err != nil {
				return nil, fmt.Errorf("unable to generate x509 key pair: %w", err)
			}

			cluster.SslOpts.Certificates = []tls.Certificate{clientCert}
		}

		if cfg.TLS.CaData != "" {
			cluster.SslOpts.RootCAs = x509.NewCertPool()
			pem, err := base64.StdEncoding.DecodeString(cfg.TLS.CaData)
			if err != nil {
				return nil, fmt.Errorf("caData could not be decoded: %w", err)
			}
			if !cluster.SslOpts.RootCAs.AppendCertsFromPEM(pem) {
				return nil, errors.New("failed to load decoded CA Cert as PEM")
			}
		}
	}

	if cfg.MaxConns > 0 {
		cluster.NumConns = cfg.MaxConns
	}

	if cfg.ConnectTimeout > 0 {
		cluster.Timeout = cfg.ConnectTimeout
		cluster.ConnectTimeout = cfg.ConnectTimeout
	} else {
		cluster.Timeout = 10 * time.Second
		cluster.ConnectTimeout = 10 * time.Second
	}

	cluster.ProtoVersion = 4
	cluster.Consistency = cfg.Consistency.GetConsistency()
	cluster.SerialConsistency = cfg.Consistency.GetSerialConsistency()
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup

	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      30,
		InitialInterval: time.Second,
		MaxInterval:     10 * time.Second,
	}

	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	return cluster, nil
}

// parseHosts returns parses a list of hosts separated by comma
func parseHosts(input string) []string {
	var hosts []string
	for _, h := range strings.Split(input, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}
	return hosts
}
