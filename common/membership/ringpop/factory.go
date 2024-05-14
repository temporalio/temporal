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

package ringpop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/tchannel-go"
	"go.uber.org/fx"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/environment"
)

const (
	defaultMaxJoinDuration      = 10 * time.Second
	persistenceOperationTimeout = 10 * time.Second
)

type factoryParams struct {
	fx.In

	Config          *config.Membership
	ServiceName     primitives.ServiceName
	ServicePortMap  config.ServicePortMap
	Logger          log.Logger
	MetadataManager persistence.ClusterMetadataManager
	RPCConfig       *config.RPC
	TLSFactory      encryption.TLSConfigProvider
	DC              *dynamicconfig.Collection
}

// factory provides ringpop based membership objects
type factory struct {
	Config          *config.Membership
	ServiceName     primitives.ServiceName
	ServicePortMap  config.ServicePortMap
	Logger          log.Logger
	MetadataManager persistence.ClusterMetadataManager
	RPCConfig       *config.RPC
	TLSFactory      encryption.TLSConfigProvider
	DC              *dynamicconfig.Collection

	channel *tchannel.Channel
	monitor *monitor
	chOnce  sync.Once
	monOnce sync.Once
}

var errMalformedBroadcastAddress = errors.New("ringpop config malformed `broadcastAddress` param")

// newFactory builds a ringpop factory
func newFactory(params factoryParams) (*factory, error) {
	cfg := params.Config
	if cfg.BroadcastAddress != "" && net.ParseIP(cfg.BroadcastAddress) == nil {
		return nil, fmt.Errorf("%w: %s", errMalformedBroadcastAddress, cfg.BroadcastAddress)
	}

	if cfg.MaxJoinDuration == 0 {
		cfg.MaxJoinDuration = defaultMaxJoinDuration
	}

	return &factory{
		Config:          params.Config,
		ServiceName:     params.ServiceName,
		ServicePortMap:  params.ServicePortMap,
		Logger:          params.Logger,
		MetadataManager: params.MetadataManager,
		RPCConfig:       params.RPCConfig,
		TLSFactory:      params.TLSFactory,
		DC:              params.DC,
	}, nil
}

// getMonitor returns a membership monitor
func (factory *factory) getMonitor() *monitor {
	factory.monOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), persistenceOperationTimeout)
		defer cancel()

		ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)
		currentClusterMetadata, err := factory.MetadataManager.GetCurrentClusterMetadata(ctx)
		if err != nil {
			factory.Logger.Fatal("Failed to get current cluster ID", tag.Error(err))
		}

		appName := "temporal"
		if currentClusterMetadata.UseClusterIdMembership {
			appName = fmt.Sprintf("temporal-%s", currentClusterMetadata.GetClusterId())
		}
		rp, err := ringpop.New(appName, ringpop.Channel(factory.getTChannel()), ringpop.AddressResolverFunc(factory.broadcastAddressResolver))
		if err != nil {
			factory.Logger.Fatal("Failed to get new ringpop", tag.Error(err))
		}

		// Empirically, ringpop updates usually propagate in under a second even in relatively large clusters.
		// 3 seconds is an over-estimate to be safer.
		maxPropagationTime := dynamicconfig.RingpopApproximateMaxPropagationTime.Get(factory.DC)()

		factory.monitor = newMonitor(
			factory.ServiceName,
			factory.ServicePortMap,
			rp,
			factory.Logger,
			factory.MetadataManager,
			factory.broadcastAddressResolver,
			factory.Config.MaxJoinDuration,
			maxPropagationTime,
			factory.getJoinTime(maxPropagationTime),
		)
	})

	return factory.monitor
}

func (factory *factory) getJoinTime(maxPropagationTime time.Duration) time.Time {
	var alignTime time.Duration
	switch factory.ServiceName {
	case primitives.MatchingService:
		alignTime = dynamicconfig.MatchingAlignMembershipChange.Get(factory.DC)()
	}
	if alignTime == 0 {
		return time.Time{}
	}
	return util.NextAlignedTime(time.Now().Add(maxPropagationTime), alignTime)
}

func (factory *factory) broadcastAddressResolver() (string, error) {
	return buildBroadcastHostPort(factory.getTChannel().PeerInfo(), factory.Config.BroadcastAddress)
}

func (factory *factory) getTChannel() *tchannel.Channel {
	factory.chOnce.Do(func() {
		ringpopServiceName := fmt.Sprintf("%v-ringpop", factory.ServiceName)
		ringpopHostAddress := net.JoinHostPort(factory.getListenIP().String(), convert.IntToString(factory.RPCConfig.MembershipPort))
		enableTLS := dynamicconfig.EnableRingpopTLS.Get(factory.DC)()

		var tChannel *tchannel.Channel
		if enableTLS {
			tChannel = factory.getTLSChannel(ringpopHostAddress, ringpopServiceName)
		} else {
			tChannel = factory.getTCPChannel(ringpopHostAddress, ringpopServiceName)
		}
		factory.channel = tChannel
	})

	return factory.channel
}

func (factory *factory) getTCPChannel(ringpopHostAddress string, ringpopServiceName string) *tchannel.Channel {
	listener, err := net.Listen("tcp", ringpopHostAddress)
	if err != nil {
		factory.Logger.Fatal("Failed to start ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}

	tChannel, err := tchannel.NewChannel(ringpopServiceName, &tchannel.ChannelOptions{})
	if err != nil {
		factory.Logger.Fatal("Failed to create ringpop TChannel", tag.Error(err))
	}

	if err := tChannel.Serve(listener); err != nil {
		factory.Logger.Fatal("Failed to serve ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}
	return tChannel
}

func (factory *factory) getTLSChannel(ringpopHostAddress string, ringpopServiceName string) *tchannel.Channel {
	clientTLSConfig, err := factory.TLSFactory.GetInternodeClientConfig()
	if err != nil {
		factory.Logger.Fatal("Failed to get internode TLS client config", tag.Error(err))
	}

	serverTLSConfig, err := factory.TLSFactory.GetInternodeServerConfig()
	if err != nil {
		factory.Logger.Fatal("Failed to get internode TLS server config", tag.Error(err))
	}

	listener, err := tls.Listen("tcp", ringpopHostAddress, serverTLSConfig)
	if err != nil {
		factory.Logger.Fatal("Failed to start ringpop TLS listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}

	dialer := tls.Dialer{Config: clientTLSConfig}
	tChannel, err := tchannel.NewChannel(ringpopServiceName, &tchannel.ChannelOptions{Dialer: dialer.DialContext})
	if err != nil {
		factory.Logger.Fatal("Failed to create ringpop TChannel", tag.Error(err))
	}

	if err := tChannel.Serve(listener); err != nil {
		factory.Logger.Fatal("Failed to serve ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}
	return tChannel
}

func (factory *factory) getListenIP() net.IP {
	if factory.RPCConfig.BindOnLocalHost && len(factory.RPCConfig.BindOnIP) > 0 {
		factory.Logger.Fatal("ListenIP failed, bindOnLocalHost and bindOnIP are mutually exclusive")
		return nil
	}

	if factory.RPCConfig.BindOnLocalHost {
		return net.ParseIP(environment.GetLocalhostIP())
	}

	if len(factory.RPCConfig.BindOnIP) > 0 {
		ip := net.ParseIP(factory.RPCConfig.BindOnIP)
		if ip != nil {
			return ip
		}

		factory.Logger.Fatal("ListenIP failed, unable to parse bindOnIP value", tag.Address(factory.RPCConfig.BindOnIP))
		return nil
	}

	ip, err := config.ListenIP()
	if err != nil {
		factory.Logger.Fatal("ListenIP failed", tag.Error(err))
		return nil
	}
	return ip
}

// closeTChannel allows fx Stop hook to close channel
func (factory *factory) closeTChannel() {
	if factory.channel != nil {
		factory.getTChannel().Close()
		factory.channel = nil
	}
}

func (factory *factory) getHostInfoProvider() (membership.HostInfoProvider, error) {
	address, err := factory.broadcastAddressResolver()
	if err != nil {
		return nil, err
	}

	servicePort, ok := factory.ServicePortMap[factory.ServiceName]
	if !ok {
		return nil, membership.ErrUnknownService
	}

	// The broadcastAddressResolver returns the host:port used to listen for
	// ringpop messages. We use a different port for the service, so we
	// replace that portion.
	serviceAddress, err := replaceServicePort(address, servicePort)
	if err != nil {
		return nil, err
	}

	hostInfo := membership.NewHostInfoFromAddress(serviceAddress)
	return membership.NewHostInfoProvider(hostInfo), nil
}
