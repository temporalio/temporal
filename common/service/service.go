package service

import (
	"fmt"
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
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

var cadenceServices = []string{common.FrontendServiceName, common.HistoryServiceName, common.MatchingServiceName}

const rpAppNamePrefix string = "cadence"
const maxRpJoinTimeout = 30 * time.Second

// TChannelFactory creates a TChannel and Thrift server
type TChannelFactory func(sName string, thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server)

// Service contains the objects specific to this service
type serviceImpl struct {
	sName                  string
	hostName               string
	hostPort               string
	hostInfo               *membership.HostInfo
	server                 *thrift.Server
	ch                     *tchannel.Channel
	rp                     *ringpop.Ringpop
	rpSeedHosts            []string
	membershipMonitor      membership.Monitor
	tchannelFactory        TChannelFactory
	clientFactory          client.Factory
	numberOfHistoryShards  int
	logger                 bark.Logger
	metricsScope           tally.Scope
	runtimeMetricsReporter *metrics.RuntimeMetricsReporter
}

// New instantiates a Service Instance
// TODO: have a better name for Service.
// TODO: consider passing a config object if the parameter list gets too big
// this is the object which holds all the common stuff
// shared by all the services.
func New(serviceName string, logger bark.Logger,
	scope tally.Scope, tchanFactory TChannelFactory, rpHosts []string, numberOfHistoryShards int) Service {
	sVice := &serviceImpl{
		sName:                 serviceName,
		logger:                logger.WithField("Service", serviceName),
		tchannelFactory:       tchanFactory,
		rpSeedHosts:           rpHosts,
		metricsScope:          scope,
		numberOfHistoryShards: numberOfHistoryShards,
	}
	sVice.runtimeMetricsReporter = metrics.NewRuntimeMetricsReporter(scope, time.Minute, sVice.logger)

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

	hostInfo, err := h.membershipMonitor.WhoAmI()
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("failed to get host info from membership monitor")
	}
	h.hostInfo = hostInfo

	metricsClient := metrics.NewClient(h.metricsScope, h.getMetricsServiceIdx(h.sName))
	h.clientFactory = client.NewTChannelClientFactory(h.ch, h.membershipMonitor, metricsClient, h.numberOfHistoryShards)

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

func (h *serviceImpl) GetMetricsScope() tally.Scope {
	return h.metricsScope
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

func (h *serviceImpl) getMetricsServiceIdx(serviceName string) metrics.ServiceIdx {
	switch serviceName {
	case common.FrontendServiceName:
		return metrics.Frontend
	case common.HistoryServiceName:
		return metrics.History
	case common.MatchingServiceName:
		return metrics.Matching
	default:
		h.logger.WithField("name", serviceName).Fatal("Unknown service name for metrics")
	}
	panic("Fatal!") // this should never happen!
}
