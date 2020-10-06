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
	"log"

	"github.com/urfave/cli"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	l "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/service/config"
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
	connection := b.createGRPCConnection(c)

	return workflowservice.NewWorkflowServiceClient(connection)
}

// AdminClient builds an admin client (based on server side thrift interface)
func (b *clientFactory) AdminClient(c *cli.Context) adminservice.AdminServiceClient {
	connection := b.createGRPCConnection(c)

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
		Logger:    l.NewZapAdapter(b.logger),
		ConnectionOptions: sdkclient.ConnectionOptions{
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		b.logger.Fatal("Failed to create SDK client", zap.Error(err))
	}

	return sdkClient
}

func (b *clientFactory) createGRPCConnection(c *cli.Context) *grpc.ClientConn {
	hostPort := c.GlobalString(FlagAddress)
	if hostPort == "" {
		hostPort = localHostPort
	}

	var tlsClientConfig *tls.Config

	certPath := c.GlobalString(FlagTLSCertPath)
	keyPath := c.GlobalString(FlagTLSKeyPath)
	caPath := c.GlobalString(FlagTLSCaPath)

	if caPath != "" {
		tlsConfig := config.RootTLS{
			Frontend: config.GroupTLS{
				Server: config.ServerTLS{
					CertFile:          certPath,
					KeyFile:           keyPath,
					ClientCAFiles:     []string{caPath},
					RequireClientAuth: false,
				},
				Client: config.ClientTLS{
					RootCAFiles: []string{caPath},
					ServerName:  "localhost",
				},
			},
		}
		provider, err := encryption.NewLocalStoreTlsProvider(&tlsConfig)
		if err != nil {
			log.Fatalf("error initializing TLS provider: %v", err)
		}
		// TODO: should be GetFrontendClientConfig(), but it's currently not working
		tlsClientConfig, err = provider.GetFrontendServerConfig()
		if err != nil {
			log.Fatalf("Failed to create tls config for grpc connection, err=%v", err)
		}
	}
	connection, err := rpc.Dial(hostPort, tlsClientConfig)
	if err != nil {
		b.logger.Fatal("Failed to create connection", zap.Error(err))
		return nil
	}
	return connection
}
