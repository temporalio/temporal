package common

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"code.uber.internal/devexp/minions/common/logging"
	"code.uber.internal/devexp/minions/common/membership"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	ringpop "github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

var cadenceServices = []string{"cadence-frontend", "cadence-history", "cadence-matching"}

const rpAppNamePrefix string = "cadence"
const maxRpJoinTimeout = 30 * time.Second

// TChannelFactory creates a TChannel and Thrift server
type TChannelFactory func(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server)

// Service contains the objects specific to this service
type serviceImpl struct {
	sName             string
	hostName          string
	hostPort          string
	server            *thrift.Server
	ch                *tchannel.Channel
	rp                *ringpop.Ringpop
	rpSeedHosts       []string
	membershipMonitor membership.Monitor
	tchannelFactory   TChannelFactory
	clientFactory     ClientFactory
	logger            bark.Logger
}

// NewService instantiates a ServiceInstance
// TODO: have a better name for Service.
// this is the object which holds all the common stuff
// shared by all the services.
func NewService(serviceName string, logger bark.Logger, tchanFactory TChannelFactory, rpHosts []string) Service {
	sVice := &serviceImpl{
		sName:           serviceName,
		logger:          logger.WithField("Service", serviceName),
		tchannelFactory: tchanFactory,
		rpSeedHosts:     rpHosts,
	}

	// Get the host name and set it on the service.  This is used for emitting metric with a tag for hostname
	if hostName, e := os.Hostname(); e != nil {
		log.Fatal("Error getting hostname")
	} else {
		sVice.hostName = hostName
	}
	return sVice
}

// GetTChannel returns the tchannel for this service
func (h *serviceImpl) GetTChannel() *tchannel.Channel {
	return h.ch
}

// GetHostPort returns the host port for this service
func (h *serviceImpl) GetHostPort() string {
	return h.hostPort
}

// GetHostName returns the name of host running the service
func (h *serviceImpl) GetHostName() string {
	return h.hostName
}

// Start starts a TChannel-Thrift service
func (h *serviceImpl) Start(thriftServices []thrift.TChanServer) {
	var err error
	h.ch, h.server = h.tchannelFactory(h.sName, thriftServices)

	// use actual listen port (in case service is bound to :0 or 0.0.0.0:0)
	h.hostPort = h.ch.PeerInfo().HostPort
	h.rp, err = h.createRingpop(h.sName, h.ch)
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Ringpop creation failed")
	}

	err = h.bootstrapRingpop(h.rp, h.rpSeedHosts)
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Ringpop bootstrap failed")
	}

	labels, err := h.rp.Labels()
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Ringpop get node labels failed")
	}
	err = labels.Set(membership.RoleKey, h.sName)
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Ringpop setting role label failed")
	}

	h.membershipMonitor = membership.NewRingpopMonitor(cadenceServices, h.rp, h.logger)
	err = h.membershipMonitor.Start()
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("starting membership monitor failed")
	}

	h.clientFactory = newTChannelClientFactory(h.ch, h.membershipMonitor)

	// The service is now started up
	log.Info("service started")

	// seed the random generator once for this service
	rand.Seed(time.Now().UTC().UnixNano())
}

// Stop closes the associated listening tchannel
func (h *serviceImpl) Stop() {
	if h.membershipMonitor != nil {
		h.membershipMonitor.Stop()
	}

	if h.rp != nil {
		h.rp.Destroy()
	}

	if h.ch != nil {
		h.ch.Close()
	}
}

// GetLogger returns the service logger
func (h *serviceImpl) GetLogger() bark.Logger {
	return h.logger
}

func (h *serviceImpl) GetClientFactory() ClientFactory {
	return h.clientFactory
}

// createRingpop instantiates the ringpop for the provided channel and host,
func (h *serviceImpl) createRingpop(service string, ch *tchannel.Channel) (*ringpop.Ringpop, error) {
	rp, err := ringpop.New(fmt.Sprintf("%s", rpAppNamePrefix), ringpop.Channel(ch))

	return rp, err
}

// bootstrapRingpop tries to bootstrap the given ringpop instance using the hosts list
func (h *serviceImpl) bootstrapRingpop(rp *ringpop.Ringpop, rpHosts []string) error {
	// TODO: log ring hosts

	bOptions := new(swim.BootstrapOptions)
	bOptions.DiscoverProvider = statichosts.New(rpHosts...)
	bOptions.MaxJoinDuration = maxRpJoinTimeout
	bOptions.JoinSize = 1 // this ensures the first guy comes up quickly

	_, err := rp.Bootstrap(bOptions)
	return err
}
