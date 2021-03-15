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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/temporalio/ringpop-go"
	"github.com/uber/tchannel-go"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
)

const (
	defaultMaxJoinDuration = 10 * time.Second
)

// RingpopFactory implements the RingpopFactory interface
type RingpopFactory struct {
	config         *config.Membership
	channel        *tchannel.Channel
	serviceName    string
	servicePortMap map[string]int
	logger         log.Logger

	sync.Mutex
	ringPop           *membership.RingPop
	membershipMonitor membership.Monitor
	metadataManager   persistence.ClusterMetadataManager
}

// NewRingpopFactory builds a ringpop factory conforming
// to the underlying configuration
func NewRingpopFactory(
	rpConfig *config.Membership,
	channel *tchannel.Channel,
	serviceName string,
	servicePortMap map[string]int,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
) (*RingpopFactory, error) {
	return newRingpopFactory(rpConfig, channel, serviceName, servicePortMap, logger, metadataManager)
}

// ValidateRingpopConfig validates that ringpop config is parseable and valid
func ValidateRingpopConfig(rpConfig *config.Membership) error {
	if rpConfig.BroadcastAddress != "" && net.ParseIP(rpConfig.BroadcastAddress) == nil {
		return fmt.Errorf("ringpop config malformed `broadcastAddress` param")
	}
	return nil
}

func newRingpopFactory(
	rpConfig *config.Membership,
	channel *tchannel.Channel,
	serviceName string,
	servicePortMap map[string]int,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
) (*RingpopFactory, error) {

	if err := ValidateRingpopConfig(rpConfig); err != nil {
		return nil, err
	}
	if rpConfig.MaxJoinDuration == 0 {
		rpConfig.MaxJoinDuration = defaultMaxJoinDuration
	}
	return &RingpopFactory{
		config:          rpConfig,
		channel:         channel,
		serviceName:     serviceName,
		servicePortMap:  servicePortMap,
		logger:          logger,
		metadataManager: metadataManager,
	}, nil
}

// GetMembershipMonitor return a membership monitor
func (factory *RingpopFactory) GetMembershipMonitor() (membership.Monitor, error) {
	factory.Lock()
	defer factory.Unlock()

	return factory.getMembership()
}

func (factory *RingpopFactory) getMembership() (membership.Monitor, error) {
	if factory.membershipMonitor != nil {
		return factory.membershipMonitor, nil
	}

	membershipMonitor, err := factory.createMembership()
	if err != nil {
		return nil, err
	}
	factory.membershipMonitor = membershipMonitor
	return membershipMonitor, nil
}

func (factory *RingpopFactory) createMembership() (membership.Monitor, error) {
	// use actual listen port (in case service is bound to :0 or 0.0.0.0:0)
	rp, err := factory.getRingpop()
	if err != nil {
		return nil, fmt.Errorf("ringpop creation failed: %v", err)
	}

	membershipMonitor := membership.NewRingpopMonitor(factory.serviceName,
		factory.servicePortMap, rp, factory.logger, factory.metadataManager, factory.broadcastAddressResolver)

	return membershipMonitor, nil
}

func (factory *RingpopFactory) getRingpop() (*membership.RingPop, error) {
	if factory.ringPop != nil {
		return factory.ringPop, nil
	}

	ringPop, err := factory.createRingpop()
	if err != nil {
		return nil, err
	}
	factory.ringPop = ringPop
	return ringPop, nil
}

func (factory *RingpopFactory) broadcastAddressResolver() (string, error) {
	return membership.BuildBroadcastHostPort(factory.channel.PeerInfo(), factory.config.BroadcastAddress)
}

func (factory *RingpopFactory) createRingpop() (*membership.RingPop, error) {
	rp, err := ringpop.New("temporal", ringpop.Channel(factory.channel), ringpop.AddressResolverFunc(factory.broadcastAddressResolver))
	if err != nil {
		return nil, err
	}

	return membership.NewRingPop(rp, factory.config.MaxJoinDuration, factory.logger), nil
}
