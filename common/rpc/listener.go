package rpc

import (
	"net"
	"sync"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
)

type (
	// ConfigListenerProvider is a common.ListenerProvider that uses configuration settings to create gRPC listeners.
	//
	// The listener creation is wrapped with a sync.Once and will always return the same result.
	ConfigListenerProvider struct {
		config      *config.Config
		serviceName primitives.ServiceName
		logger      log.Logger

		grpc       memoizedListener
		membership memoizedListener
	}

	memoizedListener struct {
		sync.Once
		net.Listener
		error
	}
)

var _ common.ListenerProvider = (*ConfigListenerProvider)(nil)

func NewConfigListenerProvider(cfg *config.Config, serviceName primitives.ServiceName, logger log.Logger) *ConfigListenerProvider {
	return &ConfigListenerProvider{
		config:      cfg,
		serviceName: serviceName,
		logger:      logger,
	}
}

func (p *ConfigListenerProvider) GetGRPCListener() net.Listener {
	cfg := p.rpcConfig()
	hostAddr := p.hostAddr(&cfg, cfg.GRPCPort)
	listener, err := p.grpc.getOrCreate(hostAddr)
	if err != nil {
		p.logger.Fatal("Failed to start gRPC listener", tag.Error(err), tag.Service(p.serviceName))
	}
	if listener == nil || listener.Addr() == nil {
		p.logger.Fatal("gRPC listener is nil or address is nil", tag.Error(err), tag.Service(p.serviceName))
	}
	p.logger.Info("Created gRPC listener", tag.Service(p.serviceName), tag.Address(listener.Addr().String()))
	return listener
}

func (p *ConfigListenerProvider) GetMembershipListener() net.Listener {
	cfg := p.rpcConfig()
	hostAddr := p.hostAddr(&cfg, cfg.MembershipPort)
	listener, err := p.membership.getOrCreate(hostAddr)
	if err != nil {
		p.logger.Fatal("Failed to start membership listener", tag.Error(err), tag.Service(p.serviceName))
	}
	if listener == nil || listener.Addr() == nil {
		p.logger.Fatal("Membership listener is nil or address is nil", tag.Error(err), tag.Service(p.serviceName))
	}
	p.logger.Info("Created membership listener", tag.Service(p.serviceName), tag.Address(listener.Addr().String()))
	return listener
}

func (p *ConfigListenerProvider) hostAddr(cfg *config.RPC, port int) string {
	return net.JoinHostPort(getListenIP(cfg, p.logger).String(), convert.IntToString(port))
}

func (p *ConfigListenerProvider) rpcConfig() config.RPC {
	return p.config.Services[string(p.serviceName)].RPC
}

func (ml *memoizedListener) getOrCreate(hostAddr string) (net.Listener, error) {
	ml.Do(func() { ml.Listener, ml.error = net.Listen("tcp", hostAddr) })
	return ml.Listener, ml.error
}
