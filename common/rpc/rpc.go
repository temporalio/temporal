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

package rpc

import (
	"crypto/tls"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/environment"
)

var _ common.RPCFactory = (*RPCFactory)(nil)

// RPCFactory is an implementation of common.RPCFactory interface
type RPCFactory struct {
	config      *config.RPC
	serviceName primitives.ServiceName
	logger      log.Logger

	frontendURL       string
	frontendTLSConfig *tls.Config

	initListener       sync.Once
	grpcListener       net.Listener
	tlsFactory         encryption.TLSConfigProvider
	clientInterceptors []grpc.UnaryClientInterceptor
}

// NewFactory builds a new RPCFactory
// conforming to the underlying configuration
func NewFactory(
	cfg *config.RPC,
	sName primitives.ServiceName,
	logger log.Logger,
	tlsProvider encryption.TLSConfigProvider,
	frontendURL string,
	frontendTLSConfig *tls.Config,
	clientInterceptors []grpc.UnaryClientInterceptor,
) *RPCFactory {
	return &RPCFactory{
		config:             cfg,
		serviceName:        sName,
		logger:             logger,
		frontendURL:        frontendURL,
		frontendTLSConfig:  frontendTLSConfig,
		tlsFactory:         tlsProvider,
		clientInterceptors: clientInterceptors,
	}
}

func (d *RPCFactory) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	var opts []grpc.ServerOption

	if d.tlsFactory != nil {
		serverConfig, err := d.tlsFactory.GetFrontendServerConfig()
		if err != nil {
			return nil, err
		}
		if serverConfig == nil {
			return opts, nil
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(serverConfig)))
	}

	return opts, nil
}

func (d *RPCFactory) GetFrontendClientTlsConfig() (*tls.Config, error) {
	if d.tlsFactory != nil {
		return d.tlsFactory.GetFrontendClientConfig()
	}

	return nil, nil
}

func (d *RPCFactory) GetRemoteClusterClientConfig(hostname string) (*tls.Config, error) {
	if d.tlsFactory != nil {
		return d.tlsFactory.GetRemoteClusterClientConfig(hostname)
	}

	return nil, nil
}

func (d *RPCFactory) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	var opts []grpc.ServerOption

	if d.tlsFactory != nil {
		serverConfig, err := d.tlsFactory.GetInternodeServerConfig()
		if err != nil {
			return nil, err
		}
		if serverConfig == nil {
			return opts, nil
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(serverConfig)))
	}

	return opts, nil
}

func (d *RPCFactory) GetInternodeClientTlsConfig() (*tls.Config, error) {
	if d.tlsFactory != nil {
		return d.tlsFactory.GetInternodeClientConfig()
	}

	return nil, nil
}

// GetGRPCListener returns cached dispatcher for gRPC inbound or creates one
func (d *RPCFactory) GetGRPCListener() net.Listener {
	d.initListener.Do(func() {
		hostAddress := net.JoinHostPort(getListenIP(d.config, d.logger).String(), convert.IntToString(d.config.GRPCPort))
		var err error
		d.grpcListener, err = net.Listen("tcp", hostAddress)

		if err != nil {
			d.logger.Fatal("Failed to start gRPC listener", tag.Error(err), tag.Service(d.serviceName), tag.Address(hostAddress))
		}

		d.logger.Info("Created gRPC listener", tag.Service(d.serviceName), tag.Address(hostAddress))
	})

	return d.grpcListener
}

func getListenIP(cfg *config.RPC, logger log.Logger) net.IP {
	if cfg.BindOnLocalHost && len(cfg.BindOnIP) > 0 {
		logger.Fatal("ListenIP failed, bindOnLocalHost and bindOnIP are mutually exclusive")
		return nil
	}

	if cfg.BindOnLocalHost {
		return net.ParseIP(environment.GetLocalhostIP())
	}

	if len(cfg.BindOnIP) > 0 {
		ip := net.ParseIP(cfg.BindOnIP)
		if ip != nil {
			return ip
		}
		logger.Fatal("ListenIP failed, unable to parse bindOnIP value", tag.Address(cfg.BindOnIP))
		return nil
	}
	ip, err := config.ListenIP()
	if err != nil {
		logger.Fatal("ListenIP failed", tag.Error(err))
		return nil
	}
	return ip
}

// CreateRemoteFrontendGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) CreateRemoteFrontendGRPCConnection(rpcAddress string) *grpc.ClientConn {
	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		hostname, _, err2 := net.SplitHostPort(rpcAddress)
		if err2 != nil {
			d.logger.Fatal("Invalid rpcAddress for remote cluster", tag.Error(err2))
		}
		tlsClientConfig, err = d.tlsFactory.GetRemoteClusterClientConfig(hostname)

		if err != nil {
			d.logger.Fatal("Failed to create tls config for gRPC connection", tag.Error(err))
			return nil
		}
	}

	return d.dial(rpcAddress, tlsClientConfig)
}

// CreateLocalFrontendGRPCConnection creates connection for internal frontend calls
func (d *RPCFactory) CreateLocalFrontendGRPCConnection() *grpc.ClientConn {
	return d.dial(d.frontendURL, d.frontendTLSConfig)
}

// CreateInternodeGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) CreateInternodeGRPCConnection(hostName string) *grpc.ClientConn {
	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		tlsClientConfig, err = d.tlsFactory.GetInternodeClientConfig()
		if err != nil {
			d.logger.Fatal("Failed to create tls config for gRPC connection", tag.Error(err))
			return nil
		}
	}

	return d.dial(hostName, tlsClientConfig)
}

func (d *RPCFactory) dial(hostName string, tlsClientConfig *tls.Config) *grpc.ClientConn {
	connection, err := Dial(hostName, tlsClientConfig, d.logger, d.clientInterceptors...)
	if err != nil {
		d.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
		return nil
	}

	return connection
}

func (d *RPCFactory) GetTLSConfigProvider() encryption.TLSConfigProvider {
	return d.tlsFactory
}
