package config

import (
	"fmt"
	"net"
	"sync"

	"github.com/uber/tchannel-go"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/rpc"
)

// RPCFactory is an implementation of service.RPCFactory interface
type RPCFactory struct {
	config      *RPC
	serviceName string
	logger      log.Logger

	sync.Mutex
	grpcListener   net.Listener
	ringpopChannel *tchannel.Channel
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
	if d.grpcListener != nil {
		return d.grpcListener
	}

	d.Lock()
	defer d.Unlock()

	if d.grpcListener == nil {
		hostAddress := fmt.Sprintf("%v:%v", d.getListenIP(), d.config.GRPCPort)
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
		ringpopHostAddress := fmt.Sprintf("%v:%v", d.getListenIP(), d.config.MembershipPort)

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
	connection, err := rpc.Dial(hostName)
	if err != nil {
		d.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
	}

	return connection
}
