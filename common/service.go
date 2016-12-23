package common

import (
	"math/rand"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

// TChannelFactory creates a TChannel and Thrift server
type TChannelFactory func(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server)

// Service contains the objects specific to this service
type serviceImpl struct {
	sName           string
	hostName        string
	hostPort        string
	server          *thrift.Server
	ch              *tchannel.Channel
	tchannelFactory TChannelFactory
	logger          bark.Logger
}

// NewService instantiates a ServiceInstance
// TODO: have a better name for Service.
// this is the object which holds all the common stuff
// shared by all the services.
func NewService(serviceName string, uuid string, logger bark.Logger, tchanFactory TChannelFactory) Service {
	sVice := &serviceImpl{
		sName:           serviceName,
		logger:          logger,
		tchannelFactory: tchanFactory,
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
	h.ch, h.server = h.tchannelFactory(h.sName, thriftServices)

	// use actual listen port (in case service is bound to :0 or 0.0.0.0:0)
	h.hostPort = h.ch.PeerInfo().HostPort

	// The service is now started up and registered with hyperbahn
	log.Info("service started")

	// seed the random generator once for this service
	rand.Seed(time.Now().UTC().UnixNano())
}

// Stop closes the associated listening tchannel
func (h *serviceImpl) Stop() {
	if h.ch != nil {
		h.ch.Close()
	}
}

// GetLogger returns the service logger
func (h *serviceImpl) GetLogger() bark.Logger {
	return h.logger
}
