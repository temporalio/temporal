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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/tchannel-go"

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
)

const (
	defaultMaxJoinDuration      = 10 * time.Second
	persistenceOperationTimeout = 10 * time.Second
)

var (
	IPV4Localhost = net.IPv4(127, 0, 0, 1)
)

// factory provides a Monitor
type factory struct {
	config         *config.Membership
	channel        *tchannel.Channel
	serviceName    primitives.ServiceName
	servicePortMap map[primitives.ServiceName]int
	logger         log.Logger

	membershipMonitor membership.Monitor
	metadataManager   persistence.ClusterMetadataManager
	rpcConfig         *config.RPC
	tlsFactory        encryption.TLSConfigProvider
	dc                *dynamicconfig.Collection

	chOnce  sync.Once
	monOnce sync.Once
}

// newFactory builds a ringpop factory conforming
// to the underlying configuration
func newFactory(
	rpConfig *config.Membership,
	serviceName primitives.ServiceName,
	servicePortMap map[primitives.ServiceName]int,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
	rpcConfig *config.RPC,
	tlsProvider encryption.TLSConfigProvider,
	dc *dynamicconfig.Collection,
) (*factory, error) {
	if err := ValidateConfig(rpConfig); err != nil {
		return nil, err
	}
	if rpConfig.MaxJoinDuration == 0 {
		rpConfig.MaxJoinDuration = defaultMaxJoinDuration
	}
	return &factory{
		config:          rpConfig,
		serviceName:     serviceName,
		servicePortMap:  servicePortMap,
		logger:          logger,
		metadataManager: metadataManager,
		rpcConfig:       rpcConfig,
		tlsFactory:      tlsProvider,
		dc:              dc,
	}, nil
}

// ValidateConfig validates that ringpop config is parseable and valid
func ValidateConfig(cfg *config.Membership) error {
	if cfg.BroadcastAddress != "" && net.ParseIP(cfg.BroadcastAddress) == nil {
		return fmt.Errorf("ringpop config malformed `broadcastAddress` param")
	}
	return nil
}

// getMembershipMonitor return a membership monitor
func (factory *factory) getMembershipMonitor() (membership.Monitor, error) {
	return factory.getMembership()
}

func (factory *factory) getMembership() (membership.Monitor, error) {
	var err error
	factory.monOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), persistenceOperationTimeout)
		defer cancel()
		ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)

		currentClusterMetadata, err := factory.metadataManager.GetCurrentClusterMetadata(ctx)
		if err != nil {
			factory.logger.Fatal("Failed to get current cluster ID", tag.Error(err))
		}
		appName := "temporal"
		if currentClusterMetadata.UseClusterIdMembership {
			appName = fmt.Sprintf("temporal-%s", currentClusterMetadata.GetClusterId())
		}
		if rp, err := ringpop.New(appName, ringpop.Channel(factory.getTChannel()), ringpop.AddressResolverFunc(factory.broadcastAddressResolver)); err != nil {
			factory.logger.Fatal("Failed to get new ringpop", tag.Error(err))
		} else {
			mrp := newService(rp, factory.config.MaxJoinDuration, factory.logger)

			factory.membershipMonitor = newMonitor(
				factory.serviceName,
				factory.servicePortMap,
				mrp,
				factory.logger,
				factory.metadataManager,
				factory.broadcastAddressResolver,
			)
		}
	})

	return factory.membershipMonitor, err
}

func (factory *factory) broadcastAddressResolver() (string, error) {
	return buildBroadcastHostPort(factory.getTChannel().PeerInfo(), factory.config.BroadcastAddress)
}

func (factory *factory) getTChannel() *tchannel.Channel {
	factory.chOnce.Do(func() {
		ringpopServiceName := fmt.Sprintf("%v-ringpop", factory.serviceName)
		ringpopHostAddress := net.JoinHostPort(factory.getListenIP().String(), convert.IntToString(factory.rpcConfig.MembershipPort))
		enableTLS := factory.dc.GetBoolProperty(dynamicconfig.EnableRingpopTLS, false)()

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
		factory.logger.Fatal("Failed to start ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}

	tChannel, err := tchannel.NewChannel(ringpopServiceName, &tchannel.ChannelOptions{})
	if err != nil {
		factory.logger.Fatal("Failed to create ringpop TChannel", tag.Error(err))
	}

	if err := tChannel.Serve(listener); err != nil {
		factory.logger.Fatal("Failed to serve ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}
	return tChannel
}

func (factory *factory) getTLSChannel(ringpopHostAddress string, ringpopServiceName string) *tchannel.Channel {
	clientTLSConfig, err := factory.tlsFactory.GetInternodeClientConfig()
	if err != nil {
		factory.logger.Fatal("Failed to get internode TLS client config", tag.Error(err))
	}

	serverTLSConfig, err := factory.tlsFactory.GetInternodeServerConfig()
	if err != nil {
		factory.logger.Fatal("Failed to get internode TLS server config", tag.Error(err))
	}

	listener, err := tls.Listen("tcp", ringpopHostAddress, serverTLSConfig)
	if err != nil {
		factory.logger.Fatal("Failed to start ringpop TLS listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}

	dialer := tls.Dialer{Config: clientTLSConfig}
	tChannel, err := tchannel.NewChannel(ringpopServiceName, &tchannel.ChannelOptions{Dialer: dialer.DialContext})
	if err != nil {
		factory.logger.Fatal("Failed to create ringpop TChannel", tag.Error(err))
	}

	if err := tChannel.Serve(listener); err != nil {
		factory.logger.Fatal("Failed to serve ringpop listener", tag.Error(err), tag.Address(ringpopHostAddress))
	}
	return tChannel
}

func (factory *factory) getListenIP() net.IP {
	if factory.rpcConfig.BindOnLocalHost && len(factory.rpcConfig.BindOnIP) > 0 {
		factory.logger.Fatal("ListenIP failed, bindOnLocalHost and bindOnIP are mutually exclusive")
		return nil
	}

	if factory.rpcConfig.BindOnLocalHost {
		return IPV4Localhost
	}

	if len(factory.rpcConfig.BindOnIP) > 0 {
		ip := net.ParseIP(factory.rpcConfig.BindOnIP)
		if ip != nil {
			return ip
		}
		factory.logger.Fatal("ListenIP failed, unable to parse bindOnIP value", tag.Address(factory.rpcConfig.BindOnIP))
		return nil
	}
	ip, err := config.ListenIP()
	if err != nil {
		factory.logger.Fatal("ListenIP failed", tag.Error(err))
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
