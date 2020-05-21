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

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/rpc/encryption"
	"github.com/temporalio/temporal/common/service/config"
)

// RPCFactory is an implementation of service.RPCFactory interface
type RPCFactory struct {
	config      *config.RPC
	serviceName string
	logger      log.Logger

	sync.Mutex
	grpcListener   net.Listener
	ringpopChannel *tchannel.Channel
	tlsFactory     encryption.TLSConfigProvider
}

// NewFactory builds a new RPCFactory
// conforming to the underlying configuration
func NewFactory(cfg *config.RPC, sName string, logger log.Logger, tlsProvider encryption.TLSConfigProvider) (*RPCFactory) {
	return newFactory(cfg, sName, logger, tlsProvider)
}

func newFactory(cfg *config.RPC, sName string, logger log.Logger, tlsProvider encryption.TLSConfigProvider) (*RPCFactory) {
	factory := &RPCFactory{config: cfg, serviceName: sName, logger: logger, tlsFactory: tlsProvider}
	return factory
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
		hostAddress := fmt.Sprintf("%v:%v", getListenIP(d.config, d.logger), d.config.GRPCPort)
		var err error
		d.grpcListener, err = net.Listen("tcp", hostAddress)

		if err != nil {
			d.logger.Fatal("Failed to start gRPC listener", tag.Error(err), tag.Service(d.serviceName), tag.Address(hostAddress))
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
		ringpopHostAddress := fmt.Sprintf("%v:%v", getListenIP(d.config, d.logger), d.config.MembershipPort)

		var err error
		d.ringpopChannel, err = tchannel.NewChannel(ringpopServiceName, nil)
		if err != nil {
			d.logger.Fatal("Failed to create ringpop TChannel", tag.Error(err))
		}

		err = d.ringpopChannel.ListenAndServe(ringpopHostAddress)
		if err != nil {
			d.logger.Fatal("Failed to start ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
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
	}

	if cfg.BindOnLocalHost {
		return net.IPv4(127, 0, 0, 1)
	}

	if len(cfg.BindOnIP) > 0 {
		ip := net.ParseIP(cfg.BindOnIP)
		if ip != nil && ip.To4() != nil {
			return ip.To4()
		}
		logger.Fatal("ListenIP failed, unable to parse bindOnIP value %q or it is not IPv4 address", tag.Address(cfg.BindOnIP))
	}
	ip, err := config.ListenIP()
	if err != nil {
		logger.Fatal("ListenIP failed, err=%v", tag.Error(err))
	}
	return ip
}

// CreateGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) CreateFrontendGRPCConnection(hostName string) *grpc.ClientConn {
	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		tlsClientConfig, err = d.tlsFactory.GetFrontendClientConfig()
		if err != nil {
			d.logger.Fatal("Failed to create tls config for grpc connection", tag.Error(err))
		}
	}

	return d.dial(hostName, tlsClientConfig)
}

// CreateGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) CreateInternodeGRPCConnection(hostName string) *grpc.ClientConn {
	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		tlsClientConfig, err = d.tlsFactory.GetInternodeClientConfig()
		if err != nil {
			d.logger.Fatal("Failed to create tls config for grpc connection", tag.Error(err))
		}
	}

	return d.dial(hostName, tlsClientConfig)
}

func (d *RPCFactory) dial(hostName string, tlsClientConfig *tls.Config) *grpc.ClientConn {
	connection, err := Dial(hostName, tlsClientConfig)
	if err != nil {
		d.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
	}

	return connection
}

func getBroadcastAddressFromConfig(serverCfg *config.Global, cfg *config.RPC, logger log.Logger) string {
	if serverCfg.Membership.BroadcastAddress != "" {
		return serverCfg.Membership.BroadcastAddress
	} else {
		return getListenIP(cfg, logger).String()
	}
}
