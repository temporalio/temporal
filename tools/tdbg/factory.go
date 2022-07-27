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

package tdbg

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var netClient HttpGetter = &http.Client{
	Timeout: time.Second * 10,
}

// HttpGetter defines http.Client.Get(...) as an interface so we can mock it
type HttpGetter interface {
	Get(url string) (resp *http.Response, err error)
}

// ClientFactory is used to construct rpc clients
type ClientFactory interface {
	AdminClient(c *cli.Context) adminservice.AdminServiceClient
}

type clientFactory struct {
	logger log.Logger
}

// NewClientFactory creates a new ClientFactory
func NewClientFactory() ClientFactory {
	logger := log.NewCLILogger()

	return &clientFactory{
		logger: logger,
	}
}

// AdminClient builds an admin client.
func (b *clientFactory) AdminClient(c *cli.Context) adminservice.AdminServiceClient {
	connection, _ := b.createGRPCConnection(c)

	return adminservice.NewAdminServiceClient(connection)
}

func (b *clientFactory) createGRPCConnection(c *cli.Context) (*grpc.ClientConn, error) {
	hostPort := c.String(FlagAddress)
	if hostPort == "" {
		hostPort = localHostPort
	}

	tlsConfig, err := b.createTLSConfig(c)
	if err != nil {
		return nil, err
	}

	grpcSecurityOptions := grpc.WithTransportCredentials(insecure.NewCredentials())

	if tlsConfig != nil {
		grpcSecurityOptions = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	dialOpts := []grpc.DialOption{
		grpcSecurityOptions,
	}

	connection, err := grpc.Dial(hostPort, dialOpts...)
	if err != nil {
		b.logger.Fatal("Failed to create connection", tag.Error(err))
		return nil, err
	}
	return connection, nil
}

func (b *clientFactory) createTLSConfig(c *cli.Context) (*tls.Config, error) {
	certPath := c.String(FlagTLSCertPath)
	keyPath := c.String(FlagTLSKeyPath)
	caPath := c.String(FlagTLSCaPath)
	disableHostNameVerificationS := c.String(FlagTLSDisableHostVerification)
	disableHostNameVerification, err := strconv.ParseBool(disableHostNameVerificationS)
	if err != nil {
		return nil, fmt.Errorf("unable to read TLS disable host verification flag: %s", err)
	}

	serverName := c.String(FlagTLSServerName)

	var host string
	var cert *tls.Certificate
	var caPool *x509.CertPool

	if caPath != "" {
		caCertPool, err := fetchCACert(caPath)
		if err != nil {
			b.logger.Fatal("Failed to load server CA certificate", tag.Error(err))
			return nil, err
		}
		caPool = caCertPool
	}
	if certPath != "" {
		myCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			b.logger.Fatal("Failed to load client certificate", tag.Error(err))
			return nil, err
		}
		cert = &myCert
	}
	// If we are given arguments to verify either server or client, configure TLS
	if caPool != nil || cert != nil {
		if serverName != "" {
			host = serverName
		} else {
			hostPort := c.String(FlagAddress)
			if hostPort == "" {
				hostPort = localHostPort
			}
			// Ignoring error as we'll fail to dial anyway, and that will produce a meaningful error
			host, _, _ = net.SplitHostPort(hostPort)
		}
		tlsConfig := auth.NewTLSConfigForServer(host, !disableHostNameVerification)
		if caPool != nil {
			tlsConfig.RootCAs = caPool
		}
		if cert != nil {
			tlsConfig.Certificates = []tls.Certificate{*cert}
		}

		return tlsConfig, nil
	}
	// If we are given a server name, set the TLS server name for DNS resolution
	if serverName != "" {
		host = serverName
		tlsConfig := auth.NewTLSConfigForServer(host, !disableHostNameVerification)
		return tlsConfig, nil
	}

	return nil, nil
}

func fetchCACert(pathOrUrl string) (caPool *x509.CertPool, err error) {
	caPool = x509.NewCertPool()
	var caBytes []byte

	if strings.HasPrefix(pathOrUrl, "http://") {
		return nil, errors.New("HTTP is not supported for CA cert URLs. Provide HTTPS URL")
	}

	if strings.HasPrefix(pathOrUrl, "https://") {
		resp, err := netClient.Get(pathOrUrl)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		caBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
	} else {
		caBytes, err = os.ReadFile(pathOrUrl)
		if err != nil {
			return nil, err
		}
	}

	if !caPool.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("unknown failure constructing cert pool for ca")
	}
	return caPool, nil
}
