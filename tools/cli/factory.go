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

package cli

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"net"

	"github.com/urfave/cli"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// ClientFactory is used to construct rpc clients
type ClientFactory interface {
	FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient
	AdminClient(c *cli.Context) adminservice.AdminServiceClient
	SDKClient(c *cli.Context, namespace string) sdkclient.Client
	HealthClient(c *cli.Context) healthpb.HealthClient
}

type clientFactory struct {
	logger log.Logger
}

// NewClientFactory creates a new ClientFactory
func NewClientFactory() ClientFactory {
	logger := log.NewDefaultLogger()

	return &clientFactory{
		logger: logger,
	}
}

// FrontendClient builds a frontend client
func (b *clientFactory) FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient {
	connection, _ := b.createGRPCConnection(c)

	return workflowservice.NewWorkflowServiceClient(connection)
}

// AdminClient builds an admin client.
func (b *clientFactory) AdminClient(c *cli.Context) adminservice.AdminServiceClient {
	connection, _ := b.createGRPCConnection(c)

	return adminservice.NewAdminServiceClient(connection)
}

// SDKClient builds an SDK client.
func (b *clientFactory) SDKClient(c *cli.Context, namespace string) sdkclient.Client {
	hostPort := c.GlobalString(FlagAddress)
	if hostPort == "" {
		hostPort = localHostPort
	}

	tlsConfig, err := b.createTLSConfig(c)
	if err != nil {
		b.logger.Fatal("Failed to configure TLS for SDK client", tag.Error(err))
	}

	sdkClient, err := sdkclient.NewClient(sdkclient.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		Logger:    log.NewSdkLogger(b.logger),
		ConnectionOptions: sdkclient.ConnectionOptions{
			DisableHealthCheck: true,
			TLS:                tlsConfig,
		},
	})
	if err != nil {
		b.logger.Fatal("Failed to create SDK client", tag.Error(err))
	}

	return sdkClient
}

// HealthClient builds a health client.
func (b *clientFactory) HealthClient(c *cli.Context) healthpb.HealthClient {
	connection, _ := b.createGRPCConnection(c)

	return healthpb.NewHealthClient(connection)
}

func (b *clientFactory) createGRPCConnection(c *cli.Context) (*grpc.ClientConn, error) {
	hostPort := c.GlobalString(FlagAddress)
	if hostPort == "" {
		hostPort = localHostPort
	}

	tlsConfig, err := b.createTLSConfig(c)
	if err != nil {
		return nil, err
	}

	grpcSecurityOptions := grpc.WithInsecure()

	if tlsConfig != nil {
		grpcSecurityOptions = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	connection, err := grpc.Dial(hostPort, grpcSecurityOptions)
	if err != nil {
		b.logger.Fatal("Failed to create connection", tag.Error(err))
		return nil, err
	}
	return connection, nil
}

func (b *clientFactory) createTLSConfig(c *cli.Context) (*tls.Config, error) {

	certPath := c.GlobalString(FlagTLSCertPath)
	keyPath := c.GlobalString(FlagTLSKeyPath)
	caPath := c.GlobalString(FlagTLSCaPath)
	hostNameVerification := c.GlobalBool(FlagTLSEnableHostVerification)
	serverName := c.GlobalString(FlagTLSServerName)

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
			// If server name is provided, we enable host verification
			// because that's the only reason for providing server name
			hostNameVerification = true
		} else {
			hostPort := c.GlobalString(FlagAddress)
			if hostPort == "" {
				hostPort = localHostPort
			}
			// Ignoring error as we'll fail to dial anyway, and that will produce a meaningful error
			host, _, _ = net.SplitHostPort(hostPort)
		}
		tlsConfig := auth.NewTLSConfigForServer(host, hostNameVerification)
		if caPool != nil {
			tlsConfig.RootCAs = caPool
		}
		if cert != nil {
			tlsConfig.Certificates = []tls.Certificate{*cert}
		}

		return tlsConfig, nil
	}

	return nil, nil
}

func fetchCACert(path string) (*x509.CertPool, error) {
	caPool := x509.NewCertPool()
	caBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if !caPool.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("unknown failure constructing cert pool for ca")
	}
	return caPool, nil
}
