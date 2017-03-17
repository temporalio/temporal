package service

import (
	"math/rand"
	"os"
	"time"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"

	log "github.com/Sirupsen/logrus"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	ringpop "github.com/uber/ringpop-go"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

var cadenceServices = []string{common.FrontendServiceName, common.HistoryServiceName, common.MatchingServiceName}

// TChannelFactory creates a TChannel and Thrift server
type TChannelFactory func(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server)

// RingpopFactory provides a bootstrapped ringpop
type RingpopFactory interface {
	CreateRingpop(ch *tchannel.Channel) (*ringpop.Ringpop, error)
}

// Service contains the objects specific to this service
type serviceImpl struct {
	sName                  string
	hostName               string
	hostPort               string
	hostInfo               *membership.HostInfo
	server                 *thrift.Server
	ch                     *tchannel.Channel
	rp                     *ringpop.Ringpop
	rpFactory              RingpopFactory
	membershipMonitor      membership.Monitor
	tchannelFactory        TChannelFactory
	clientFactory          client.Factory
	numberOfHistoryShards  int
	logger                 bark.Logger
	metricsScope           tally.Scope
	runtimeMetricsReporter *metrics.RuntimeMetricsReporter
	metricsClient          metrics.Client
}

// New instantiates a Service Instance
// TODO: have a better name for Service.
// TODO: consider passing a config object if the parameter list gets too big
// this is the object which holds all the common stuff
// shared by all the services.
func New(serviceName string, logger bark.Logger, scope tally.Scope, tchanFactory TChannelFactory,
	rpFactory RingpopFactory, numberOfHistoryShards int) Service {
	sVice := &serviceImpl{
		sName:                 serviceName,
		logger:                logger.WithField("Service", serviceName),
		tchannelFactory:       tchanFactory,
		rpFactory:             rpFactory,
		metricsScope:          scope,
		numberOfHistoryShards: numberOfHistoryShards,
	}
	sVice.runtimeMetricsReporter = metrics.NewRuntimeMetricsReporter(scope, time.Minute, sVice.logger)
	sVice.metricsClient = metrics.NewClient(scope, getMetricsServiceIdx(serviceName, logger))

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

	h.metricsScope.Counter(metrics.RestartCount).Inc(1)
	h.runtimeMetricsReporter.Start()

	h.ch, h.server = h.tchannelFactory(h.sName, thriftServices)

	// use actual listen port (in case service is bound to :0 or 0.0.0.0:0)
	h.hostPort = h.ch.PeerInfo().HostPort
	h.rp, err = h.rpFactory.CreateRingpop(h.ch)
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Ringpop creation failed")
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

	hostInfo, err := h.membershipMonitor.WhoAmI()
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("failed to get host info from membership monitor")
	}
	h.hostInfo = hostInfo

	h.clientFactory = client.NewTChannelClientFactory(h.ch, h.membershipMonitor, h.metricsClient,
		h.numberOfHistoryShards)

	// The service is now started up
	h.logger.Info("service started")

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

	h.runtimeMetricsReporter.Stop()
}

// GetLogger returns the service logger
func (h *serviceImpl) GetLogger() bark.Logger {
	return h.logger
}

func (h *serviceImpl) GetMetricsClient() metrics.Client {
	return h.metricsClient
}

func (h *serviceImpl) GetClientFactory() client.Factory {
	return h.clientFactory
}

func (h *serviceImpl) GetMembershipMonitor() membership.Monitor {
	return h.membershipMonitor
}

func (h *serviceImpl) GetHostInfo() *membership.HostInfo {
	return h.hostInfo
}

func getMetricsServiceIdx(serviceName string, logger bark.Logger) metrics.ServiceIdx {
	switch serviceName {
	case common.FrontendServiceName:
		return metrics.Frontend
	case common.HistoryServiceName:
		return metrics.History
	case common.MatchingServiceName:
		return metrics.Matching
	default:
		logger.Fatalf("Unknown service name '%v' for metrics!", serviceName)
	}

	// this should never happen!
	return metrics.NumServices
}
