package ringpop

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/uber/ringpop-go"
	"github.com/uber/tchannel-go"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/config"
)

const (
	defaultMaxJoinDuration = 10 * time.Second
)

// RingpopFactory implements the RingpopFactory interface
type RingpopFactory struct {
	config         *config.Ringpop
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
	rpConfig *config.Ringpop,
	channel *tchannel.Channel,
	serviceName string,
	servicePortMap map[string]int,
	logger log.Logger,
	metadataManager persistence.ClusterMetadataManager,
) (*RingpopFactory, error) {
	return newRingpopFactory(rpConfig, channel, serviceName, servicePortMap, logger, metadataManager)
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
	channel *tchannel.Channel,
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
	rp, err := ringpop.New(factory.config.Name, ringpop.Channel(factory.channel), ringpop.AddressResolverFunc(factory.broadcastAddressResolver))
	if err != nil {
		return nil, err
	}

	return membership.NewRingPop(rp, factory.config.MaxJoinDuration, factory.logger), nil
}
