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
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
)

// ClientFactory is used to construct rpc clients
type ClientFactory interface {
	FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient
	AdminClient(c *cli.Context) adminservice.AdminServiceClient
	SDKClient(c *cli.Context, namespace string) sdkclient.Client
}

type clientFactory struct {
	logger *zap.Logger
}

// NewClientFactory creates a new ClientFactory
func NewClientFactory() ClientFactory {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	return &clientFactory{
		logger: logger,
	}
}

// FrontendClient builds a frontend client
func (b *clientFactory) FrontendClient(c *cli.Context) workflowservice.WorkflowServiceClient {
	connection, _ := b.createGRPCConnection(c)

	return workflowservice.NewWorkflowServiceClient(connection)
}

// AdminClient builds an admin client (based on server side thrift interface)
func (b *clientFactory) AdminClient(c *cli.Context) adminservice.AdminServiceClient {
	connection, _ := b.createGRPCConnection(c)

	return adminservice.NewAdminServiceClient(connection)
}

// AdminClient builds an admin client (based on server side thrift interface)
func (b *clientFactory) SDKClient(c *cli.Context, namespace string) sdkclient.Client {
	hostPort := c.GlobalString(FlagAddress)
	if hostPort == "" {
		hostPort = localHostPort
	}

	sdkClient, err := sdkclient.NewClient(sdkclient.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		Logger:    log.NewZapAdapter(b.logger),
		ConnectionOptions: sdkclient.ConnectionOptions{
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		b.logger.Fatal("Failed to create SDK client", zap.Error(err))
	}

	return sdkClient
}

func (b *clientFactory) createGRPCConnection(c *cli.Context) (*grpc.ClientConn, error) {
	hostPort := c.GlobalString(FlagAddress)
	if hostPort == "" {
		hostPort = localHostPort
	}
	// Ignoring error as we'll fail to dial anyway, and that will produce a meaningful error
	host, _, _ := net.SplitHostPort(hostPort)

	certPath := c.GlobalString(FlagTLSCertPath)
	keyPath := c.GlobalString(FlagTLSKeyPath)
	caPath := c.GlobalString(FlagTLSCaPath)
	hostNameVerification := c.GlobalBool(FlagTLSEnableHostVerification)

	grpcSecurityOptions := grpc.WithInsecure()
	var cert *tls.Certificate
	var caPool *x509.CertPool

	if caPath != "" {
		caCertPool, err := fetchCACert(caPath)
		if err != nil {
			b.logger.Fatal("Failed to load server CA certificate", zap.Error(err))
			return nil, err
		}
		caPool = caCertPool
	}
	if certPath != "" {
		myCert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			b.logger.Fatal("Failed to load client certificate", zap.Error(err))
			return nil, err
		}
		cert = &myCert
	}
	// If we are given arguments to verify either server or client, configure TLS
	if caPool != nil || cert != nil {
		tlsConfig := auth.NewTLSConfigForServer(host, hostNameVerification)
		if caPool != nil {
			tlsConfig.RootCAs = caPool
		}
		if cert != nil {
			tlsConfig.Certificates = []tls.Certificate{*cert}
		}
		grpcSecurityOptions = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	connection, err := grpc.Dial(hostPort, grpcSecurityOptions)
	if err != nil {
		b.logger.Fatal("Failed to create connection", zap.Error(err))
		return nil, err
	}
	return connection, nil
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
