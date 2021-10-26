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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/cli/headersprovider"
	"go.temporal.io/server/tools/cli/plugin"
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
	logger := log.NewCLILogger()

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
		Identity:  getCliIdentity(),
		ConnectionOptions: sdkclient.ConnectionOptions{
			DisableHealthCheck: true,
			TLS:                tlsConfig,
		},
		HeadersProvider: headersprovider.GetCurrent(),
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

func headersProviderInterceptor(headersProvider plugin.HeadersProvider) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		headers, err := headersProvider.GetHeaders(ctx)
		if err != nil {
			return err
		}
		for k, v := range headers {
			ctx = metadata.AppendToOutgoingContext(ctx, k, v)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
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

	dialOpts := []grpc.DialOption{
		grpcSecurityOptions,
	}
	headersProvider := headersprovider.GetCurrent()
	if headersProvider != nil {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(headersProviderInterceptor(headersProvider)))
	}

	connection, err := grpc.Dial(hostPort, dialOpts...)
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
	disableHostNameVerification := c.GlobalBool(FlagTLSDisableHostVerification)
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
		} else {
			hostPort := c.GlobalString(FlagAddress)
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
