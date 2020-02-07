// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"fmt"
	"net"
	"sync"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/tchannel"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

// RPCFactory is an implementation of service.RPCFactory interface
type RPCFactory struct {
	config      *RPC
	serviceName string
	ch          *tchannel.ChannelTransport
	logger      log.Logger

	sync.Mutex
	dispatcher        *yarpc.Dispatcher
	grpcListiner      net.Listener
	ringpopDispatcher *yarpc.Dispatcher
}

// NewFactory builds a new RPCFactory
// conforming to the underlying configuration
func (cfg *RPC) NewFactory(sName string, logger log.Logger) *RPCFactory {
	return newRPCFactory(cfg, sName, logger)
}

func newRPCFactory(cfg *RPC, sName string, logger log.Logger) *RPCFactory {
	factory := &RPCFactory{config: cfg, serviceName: sName, logger: logger}
	return factory
}

// GetGRPCListener returns cached dispatcher for gRPC inbound or creates one
func (d *RPCFactory) GetGRPCListener() net.Listener {
	if d.grpcListiner != nil {
		return d.grpcListiner
	}

	d.Lock()
	defer d.Unlock()

	if d.grpcListiner == nil {
		hostAddress := fmt.Sprintf("%v:%v", d.getListenIP(), d.config.GRPCPort)
		var err error
		d.grpcListiner, err = net.Listen("tcp", hostAddress)
		if err != nil {
			d.logger.Fatal("Failed create gRPC listener", tag.Error(err), tag.Service(d.serviceName), tag.Address(hostAddress))
		}

		d.logger.Info("Created gRPC listener", tag.Service(d.serviceName), tag.Address(hostAddress))
	}

	return d.grpcListiner
}

// GetTChannelDispatcher return a cached dispatcher
func (d *RPCFactory) GetTChannelDispatcher() *yarpc.Dispatcher {
	d.Lock()
	defer d.Unlock()

	if d.dispatcher != nil {
		return d.dispatcher
	}

	d.dispatcher = d.createInboundTChannelDispatcher(d.serviceName, d.config.Port)
	return d.dispatcher
}

// GetRingpopDispatcher return a cached ringpop dispatcher
func (d *RPCFactory) GetRingpopDispatcher() *yarpc.Dispatcher {
	d.Lock()
	defer d.Unlock()

	if d.ringpopDispatcher != nil {
		return d.ringpopDispatcher
	}

	ringpopServiceName := fmt.Sprintf("%v-ringpop", d.serviceName)
	d.ringpopDispatcher = d.createInboundTChannelDispatcher(ringpopServiceName, d.config.RingpopPort)
	return d.ringpopDispatcher
}

func (d *RPCFactory) createInboundTChannelDispatcher(serviceName string, port int) *yarpc.Dispatcher {
	var err error
	hostAddress := fmt.Sprintf("%v:%v", d.getListenIP(), port)
	d.ch, err = tchannel.NewChannelTransport(
		tchannel.ServiceName(serviceName),
		tchannel.ListenAddr(hostAddress))
	if err != nil {
		d.logger.Fatal("Failed to create transport channel", tag.Error(err), tag.Address(hostAddress))
	}
	d.logger.Info("Created RPC dispatcher and listening", tag.Service(serviceName), tag.Address(hostAddress))
	return yarpc.NewDispatcher(yarpc.Config{
		Name:     serviceName,
		Inbounds: yarpc.Inbounds{d.ch.NewInbound()},
	})
}

// CreateTChannelDispatcherForOutbound creates a dispatcher for outbound connection
func (d *RPCFactory) CreateTChannelDispatcherForOutbound(callerName, serviceName, hostName string) *yarpc.Dispatcher {
	return d.createDispatcherForOutbound(d.ch.NewSingleOutbound(hostName), callerName, serviceName, hostName, "TChannel")
}

func (d *RPCFactory) createDispatcherForOutbound(unaryOutbound transport.UnaryOutbound, callerName, serviceName, hostName, transportType string) *yarpc.Dispatcher {
	d.logger.Info("Created RPC dispatcher outbound", tag.Service(d.serviceName))
	dsp := yarpc.NewDispatcher(yarpc.Config{
		Name: callerName,
		Outbounds: yarpc.Outbounds{
			serviceName: {Unary: unaryOutbound},
		},
	})

	d.logger.Info("Created RPC dispatcher outbound", tag.Service(d.serviceName), tag.Address(hostName), tag.TransportType(transportType))

	if err := dsp.Start(); err != nil {
		d.logger.Fatal("Failed to start outbound dispatcher", tag.Error(err), tag.TransportType(transportType))
	}
	return dsp
}

func (d *RPCFactory) getListenIP() net.IP {
	if d.config.BindOnLocalHost && len(d.config.BindOnIP) > 0 {
		d.logger.Fatal("ListenIP failed, bindOnLocalHost and bindOnIP are mutually exclusive")
	}

	if d.config.BindOnLocalHost {
		return net.IPv4(127, 0, 0, 1)
	}

	if len(d.config.BindOnIP) > 0 {
		ip := net.ParseIP(d.config.BindOnIP)
		if ip != nil && ip.To4() != nil {
			return ip.To4()
		}
		d.logger.Fatal("ListenIP failed, unable to parse bindOnIP value %q or it is not IPv4 address", tag.Address(d.config.BindOnIP))
	}
	ip, err := ListenIP()
	if err != nil {
		d.logger.Fatal("ListenIP failed, err=%v", tag.Error(err))
	}
	return ip
}

// CreateGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) CreateGRPCConnection(hostName string) *grpc.ClientConn {
	connection, err := grpc.Dial(hostName, grpc.WithInsecure())
	if err != nil {
		d.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
	}

	return connection
}
