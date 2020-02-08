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

package ringpop

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/temporalio/temporal/common/service/config"

	"github.com/temporalio/temporal/common/persistence"

	"github.com/uber/ringpop-go"
	tcg "github.com/uber/tchannel-go"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/membership"
)

const (
	defaultMaxJoinDuration = 10 * time.Second
)

// RingpopFactory implements the RingpopFactory interface
type RingpopFactory struct {
	config         *config.Ringpop
	dispatcher     *yarpc.Dispatcher
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
	rpConfig *config.Ringpop,
	dispatcher *yarpc.Dispatcher,
	serviceName string,
	servicePortMap map[string]int,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
) (*RingpopFactory, error) {
	return newRingpopFactory(rpConfig, dispatcher, serviceName, servicePortMap, logger, metadataManager)
}

func validateRingpopConfig(rpConfig *config.Ringpop) error {
	if len(rpConfig.Name) == 0 {
		return fmt.Errorf("ringpop config missing `name` param")
	}
	if rpConfig.BroadcastAddress != "" && net.ParseIP(rpConfig.BroadcastAddress) == nil {
		return fmt.Errorf("ringpop config malformed `broadcastAddress` param")
	}
	return nil
}

func newRingpopFactory(
	rpConfig *config.Ringpop,
	dispatcher *yarpc.Dispatcher,
	serviceName string,
	servicePortMap map[string]int,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
) (*RingpopFactory, error) {

	if err := validateRingpopConfig(rpConfig); err != nil {
		return nil, err
	}
	if rpConfig.MaxJoinDuration == 0 {
		rpConfig.MaxJoinDuration = defaultMaxJoinDuration
	}
	return &RingpopFactory{
		config:          rpConfig,
		dispatcher:      dispatcher,
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
	// This initial piece is copied from ringpop-go/ringpop.go/channelAddressResolver
	var ch *tcg.Channel
	var err error
	if ch, err = factory.getChannel(factory.dispatcher); err != nil {
		return "", err
	}

	return membership.BuildBroadcastHostPort(ch.PeerInfo(), factory.config.BroadcastAddress)
}

func (factory *RingpopFactory) createRingpop() (*membership.RingPop, error) {

	var ch *tcg.Channel
	var err error
	if ch, err = factory.getChannel(factory.dispatcher); err != nil {
		return nil, err
	}

	rp, err := ringpop.New(factory.config.Name, ringpop.Channel(ch), ringpop.AddressResolverFunc(factory.broadcastAddressResolver))
	if err != nil {
		return nil, err
	}

	return membership.NewRingPop(rp, factory.config.MaxJoinDuration, factory.logger), nil
}

func (factory *RingpopFactory) getChannel(
	dispatcher *yarpc.Dispatcher,
) (*tcg.Channel, error) {

	t := dispatcher.Inbounds()[0].Transports()[0].(*tchannel.ChannelTransport)
	ty := reflect.ValueOf(t.Channel())
	var ch *tcg.Channel
	var ok bool
	if ch, ok = ty.Interface().(*tcg.Channel); !ok {
		return nil, errors.New("unable to get tchannel out of the dispatcher")
	}
	return ch, nil
}
