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
	"fmt"
	"net"
	"sync"

	"github.com/uber/tchannel-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/encryption"
)

// RPCFactory is an implementation of service.RPCFactory interface
type RPCFactory struct {
	config      *config.RPC
	serviceName string
	logger      log.Logger
	dc          *dynamicconfig.Collection

	sync.Mutex
	grpcListener   net.Listener
	ringpopChannel *tchannel.Channel
	tlsFactory     encryption.TLSConfigProvider
}

// NewFactory builds a new RPCFactory
// conforming to the underlying configuration
func NewFactory(cfg *config.RPC, sName string, logger log.Logger, tlsProvider encryption.TLSConfigProvider, dc *dynamicconfig.Collection) *RPCFactory {
	return &RPCFactory{
		config:      cfg,
		serviceName: sName,
		logger:      logger,
		dc:          dc,
		tlsFactory:  tlsProvider,
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
	if d.grpcListener != nil {
		return d.grpcListener
	}

	d.Lock()
	defer d.Unlock()

	if d.grpcListener == nil {
		hostAddress := net.JoinHostPort(getListenIP(d.config, d.logger).String(), convert.IntToString(d.config.GRPCPort))
		var err error
		d.grpcListener, err = net.Listen("tcp", hostAddress)

		if err != nil {
			d.logger.Fatal("Failed to start gRPC listener", tag.Error(err), tag.Service(d.serviceName), tag.Address(hostAddress))
			return nil
		}

		d.logger.Info("Created gRPC listener", tag.Service(d.serviceName), tag.Address(hostAddress))
	}

	return d.grpcListener
}

// GetRingpopChannel return a cached ringpop dispatcher
func (d *RPCFactory) GetRingpopChannel() *tchannel.Channel {
	if d.ringpopChannel != nil {
		return d.ringpopChannel
	}

	d.Lock()
	defer d.Unlock()

	if d.ringpopChannel == nil {
		ringpopServiceName := fmt.Sprintf("%v-ringpop", d.serviceName)
		ringpopHostAddress := net.JoinHostPort(getListenIP(d.config, d.logger).String(), convert.IntToString(d.config.MembershipPort))
		enableTLS := d.dc.GetBoolProperty(dynamicconfig.EnableRingpopTLS, false)()

		opts := &tchannel.ChannelOptions{}
		if enableTLS {
			clientTLSConfig, err := d.GetInternodeClientTlsConfig()
			if err != nil {
				d.logger.Fatal("Failed to get internode TLS client config", tag.Error(err))
				return nil
			}
			if clientTLSConfig != nil {
				opts.Dialer = (&tls.Dialer{Config: clientTLSConfig}).DialContext
			}
		}
		var err error

		d.ringpopChannel, err = tchannel.NewChannel(ringpopServiceName, opts)
		if err != nil {
			d.logger.Fatal("Failed to create ringpop TChannel", tag.Error(err))
			return nil
		}

		var listener net.Listener
		if enableTLS {
			serverTLSConfig, err := d.tlsFactory.GetInternodeServerConfig()
			if err != nil {
				d.logger.Fatal("Failed to get internode TLS server config", tag.Error(err))
				return nil
			}
			if serverTLSConfig != nil {
				listener, err = tls.Listen("tcp", ringpopHostAddress, serverTLSConfig)
				if err != nil {
					d.logger.Fatal("Failed to start ringpop TLS listener", tag.Error(err), tag.Address(ringpopHostAddress))
					return nil
				}
			}
		}
		if listener == nil {
			if listener, err = net.Listen("tcp", ringpopHostAddress); err != nil {
				d.logger.Fatal("Failed to start ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
				return nil
			}
		}

		if err := d.ringpopChannel.Serve(listener); err != nil {
			d.logger.Fatal("Failed to serve ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
			return nil
		}
	}

	return d.ringpopChannel
}

func (d *RPCFactory) getTLSFactory() encryption.TLSConfigProvider {
	return d.tlsFactory
}

func getListenIP(cfg *config.RPC, logger log.Logger) net.IP {
	if cfg.BindOnLocalHost && len(cfg.BindOnIP) > 0 {
		logger.Fatal("ListenIP failed, bindOnLocalHost and bindOnIP are mutually exclusive")
		return nil
	}

	if cfg.BindOnLocalHost {
		return net.IPv4(127, 0, 0, 1)
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

// CreateFrontendGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) CreateFrontendGRPCConnection(hostName string) *grpc.ClientConn {
	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		tlsClientConfig, err = d.tlsFactory.GetFrontendClientConfig()
		if err != nil {
			d.logger.Fatal("Failed to create tls config for gRPC connection", tag.Error(err))
			return nil
		}
	}

	return d.dial(hostName, tlsClientConfig)
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
	connection, err := Dial(hostName, tlsClientConfig, d.logger)
	if err != nil {
		d.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
		return nil
	}

	return connection
}

func (d *RPCFactory) GetTLSConfigProvider() encryption.TLSConfigProvider {
	return d.tlsFactory
}
