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

package service

import (
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common/blobstore"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"

	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	ringpop "github.com/uber/ringpop-go"
	"go.uber.org/yarpc"
)

var cadenceServices = []string{
	common.FrontendServiceName,
	common.HistoryServiceName,
	common.MatchingServiceName,
	common.WorkerServiceName,
}

type (
	// BootstrapParams holds the set of parameters
	// needed to bootstrap a service
	BootstrapParams struct {
		Name               string
		Logger             bark.Logger
		MetricScope        tally.Scope
		RingpopFactory     RingpopFactory
		RPCFactory         common.RPCFactory
		PProfInitializer   common.PProfInitializer
		PersistenceConfig  config.Persistence
		ClusterMetadata    cluster.Metadata
		ReplicatorConfig   config.Replicator
		MetricsClient      metrics.Client
		MessagingClient    messaging.Client
		DynamicConfig      dynamicconfig.Client
		DispatcherProvider client.DispatcherProvider
		BlobstoreClient    blobstore.Client
	}

	// RingpopFactory provides a bootstrapped ringpop
	RingpopFactory interface {
		// CreateRingpop vends a bootstrapped ringpop object
		CreateRingpop(d *yarpc.Dispatcher) (*ringpop.Ringpop, error)
	}

	// Service contains the objects specific to this service
	serviceImpl struct {
		status                 int32
		sName                  string
		hostName               string
		hostInfo               *membership.HostInfo
		dispatcher             *yarpc.Dispatcher
		rp                     *ringpop.Ringpop
		rpFactory              RingpopFactory
		membershipMonitor      membership.Monitor
		rpcFactory             common.RPCFactory
		pprofInitializer       common.PProfInitializer
		clientBean             client.Bean
		numberOfHistoryShards  int
		logger                 bark.Logger
		metricsScope           tally.Scope
		runtimeMetricsReporter *metrics.RuntimeMetricsReporter
		metricsClient          metrics.Client
		clusterMetadata        cluster.Metadata
		messagingClient        messaging.Client
		dynamicCollection      *dynamicconfig.Collection
		dispatcherProvider     client.DispatcherProvider
	}
)

// New instantiates a Service Instance
// TODO: have a better name for Service.
func New(params *BootstrapParams) Service {
	sVice := &serviceImpl{
		status:                common.DaemonStatusInitialized,
		sName:                 params.Name,
		logger:                params.Logger,
		rpcFactory:            params.RPCFactory,
		rpFactory:             params.RingpopFactory,
		pprofInitializer:      params.PProfInitializer,
		metricsScope:          params.MetricScope,
		numberOfHistoryShards: params.PersistenceConfig.NumHistoryShards,
		clusterMetadata:       params.ClusterMetadata,
		metricsClient:         params.MetricsClient,
		messagingClient:       params.MessagingClient,
		dispatcherProvider:    params.DispatcherProvider,
		dynamicCollection:     dynamicconfig.NewCollection(params.DynamicConfig, params.Logger),
	}
	sVice.runtimeMetricsReporter = metrics.NewRuntimeMetricsReporter(params.MetricScope, time.Minute, sVice.logger)
	sVice.dispatcher = sVice.rpcFactory.CreateDispatcher()
	if sVice.dispatcher == nil {
		sVice.logger.Fatal("Unable to create yarpc dispatcher")
	}

	// Get the host name and set it on the service.  This is used for emitting metric with a tag for hostname
	if hostName, err := os.Hostname(); err != nil {
		sVice.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Error getting hostname")
	} else {
		sVice.hostName = hostName
	}
	return sVice
}

// UpdateLoggerWithServiceName tag logging with service name from the top level
func (params *BootstrapParams) UpdateLoggerWithServiceName(name string) {
	params.Logger = params.Logger.WithField("Service", name)
}

// GetHostName returns the name of host running the service
func (h *serviceImpl) GetHostName() string {
	return h.hostName
}

// Start starts a yarpc service
func (h *serviceImpl) Start() {
	if !atomic.CompareAndSwapInt32(&h.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	var err error

	h.metricsScope.Counter(metrics.RestartCount).Inc(1)
	h.runtimeMetricsReporter.Start()

	if err := h.pprofInitializer.Start(); err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Failed to start pprof")
	}

	if err := h.dispatcher.Start(); err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("Failed to start yarpc dispatcher")
	}

	// use actual listen port (in case service is bound to :0 or 0.0.0.0:0)
	h.rp, err = h.rpFactory.CreateRingpop(h.dispatcher)
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

	h.clientBean, err = client.NewClientBean(
		client.NewRPCClientFactory(h.rpcFactory, h.membershipMonitor, h.metricsClient, h.numberOfHistoryShards),
		h.dispatcherProvider,
		h.clusterMetadata,
	)
	if err != nil {
		h.logger.WithFields(bark.Fields{logging.TagErr: err}).Fatal("fail to initialize client bean")
	}

	// The service is now started up
	h.logger.Info("service started")

	// seed the random generator once for this service
	rand.Seed(time.Now().UTC().UnixNano())
}

// Stop closes the associated transport
func (h *serviceImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&h.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	if h.membershipMonitor != nil {
		h.membershipMonitor.Stop()
	}

	if h.rp != nil {
		h.rp.Destroy()
	}

	if h.dispatcher != nil {
		h.dispatcher.Stop()
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

func (h *serviceImpl) GetClientBean() client.Bean {
	return h.clientBean
}

func (h *serviceImpl) GetMembershipMonitor() membership.Monitor {
	return h.membershipMonitor
}

func (h *serviceImpl) GetHostInfo() *membership.HostInfo {
	return h.hostInfo
}

func (h *serviceImpl) GetDispatcher() *yarpc.Dispatcher {
	return h.dispatcher
}

// GetClusterMetadata returns the service cluster metadata
func (h *serviceImpl) GetClusterMetadata() cluster.Metadata {
	return h.clusterMetadata
}

// GetMessagingClient returns the messaging client against Kafka
func (h *serviceImpl) GetMessagingClient() messaging.Client {
	return h.messagingClient
}

// GetMetricsServiceIdx returns the metrics name
func GetMetricsServiceIdx(serviceName string, logger bark.Logger) metrics.ServiceIdx {
	switch serviceName {
	case common.FrontendServiceName:
		return metrics.Frontend
	case common.HistoryServiceName:
		return metrics.History
	case common.MatchingServiceName:
		return metrics.Matching
	case common.WorkerServiceName:
		return metrics.Worker
	default:
		logger.Fatalf("Unknown service name '%v' for metrics!", serviceName)
	}

	// this should never happen!
	return metrics.NumServices
}
