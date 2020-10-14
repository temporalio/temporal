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

package cassandra

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"

	"errors"
	"fmt"
	"strings"

	"github.com/gocql/gocql"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/service/config"
)

// NewCassandraCluster creates a cassandra cluster from a given configuration
func NewCassandraCluster(cfg config.Cassandra) (*gocql.ClusterConfig, error) {
	hosts := parseHosts(cfg.Hosts)
	cluster := gocql.NewCluster(hosts...)
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
		cluster.SslOpts = &gocql.SslOptions{
			CertPath:               cfg.TLS.CertFile,
			KeyPath:                cfg.TLS.KeyFile,
			CaPath:                 cfg.TLS.CaFile,
			EnableHostVerification: cfg.TLS.EnableHostVerification,

			Config: auth.NewTLSConfigForServer(cfg.TLS.ServerName, cfg.TLS.EnableHostVerification),
		}

		if cfg.TLS.CertData != "" {
			certBytes, err := base64.StdEncoding.DecodeString(cfg.TLS.CertData)
			if err != nil {
				return nil, fmt.Errorf("client certificate could not be decoded: %w", err)
			}

			keyBytes, err := base64.StdEncoding.DecodeString(cfg.TLS.KeyData)
			if err != nil {
				return nil, fmt.Errorf("client certificate private key could not be decoded: %w", err)
			}

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
		cluster.ConnectTimeout = cfg.ConnectTimeout
	}

	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	return cluster, nil
}

func parseHosts(input string) []string {
	var hosts = make([]string, 0)
	for _, h := range strings.Split(input, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}
	return hosts
}
