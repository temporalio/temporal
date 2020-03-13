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

	persistenceClient "github.com/uber/cadence/common/persistence/client"

	"github.com/uber/cadence/common/authorization"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/yarpc"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	es "github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// BootstrapParams holds the set of parameters
	// needed to bootstrap a service
	BootstrapParams struct {
		Name            string
		InstanceID      string
		Logger          log.Logger
		ThrottledLogger log.Logger

		MetricScope              tally.Scope
		MembershipFactory        MembershipMonitorFactory
		RPCFactory               common.RPCFactory
		AbstractDatastoreFactory persistenceClient.AbstractDataStoreFactory
		PProfInitializer         common.PProfInitializer
		PersistenceConfig        config.Persistence
		ClusterMetadata          cluster.Metadata
		ReplicatorConfig         config.Replicator
		MetricsClient            metrics.Client
		MessagingClient          messaging.Client
		ESClient                 es.Client
		ESConfig                 *es.Config
		DynamicConfig            dynamicconfig.Client
		DispatcherProvider       client.DispatcherProvider
		DCRedirectionPolicy      config.DCRedirectionPolicy
		PublicClient             workflowserviceclient.Interface
		ArchivalMetadata         archiver.ArchivalMetadata
		ArchiverProvider         provider.ArchiverProvider
		Authorizer               authorization.Authorizer
	}

	// MembershipMonitorFactory provides a bootstrapped membership monitor
	MembershipMonitorFactory interface {
		// GetMembershipMonitor return a membership monitor
		GetMembershipMonitor() (membership.Monitor, error)
	}

	// Service contains the objects specific to this service
	serviceImpl struct {
		status                int32
		sName                 string
		hostName              string
		hostInfo              *membership.HostInfo
		dispatcher            *yarpc.Dispatcher
		membershipFactory     MembershipMonitorFactory
		membershipMonitor     membership.Monitor
		rpcFactory            common.RPCFactory
		pprofInitializer      common.PProfInitializer
		clientBean            client.Bean
		timeSource            clock.TimeSource
		numberOfHistoryShards int

		logger          log.Logger
		throttledLogger log.Logger

		metricsScope           tally.Scope
		runtimeMetricsReporter *metrics.RuntimeMetricsReporter
		metricsClient          metrics.Client
		clusterMetadata        cluster.Metadata
		messagingClient        messaging.Client
		dynamicCollection      *dynamicconfig.Collection
		dispatcherProvider     client.DispatcherProvider
		archivalMetadata       archiver.ArchivalMetadata
		archiverProvider       provider.ArchiverProvider
		serializer             persistence.PayloadSerializer
	}
)

var _ Service = (*serviceImpl)(nil)

// New instantiates a Service Instance
// TODO: have a better name for Service.
func New(params *BootstrapParams) Service {
	sVice := &serviceImpl{
		status:                common.DaemonStatusInitialized,
		sName:                 params.Name,
		logger:                params.Logger,
		throttledLogger:       params.ThrottledLogger,
		rpcFactory:            params.RPCFactory,
		membershipFactory:     params.MembershipFactory,
		pprofInitializer:      params.PProfInitializer,
		timeSource:            clock.NewRealTimeSource(),
		metricsScope:          params.MetricScope,
		numberOfHistoryShards: params.PersistenceConfig.NumHistoryShards,
		clusterMetadata:       params.ClusterMetadata,
		metricsClient:         params.MetricsClient,
		messagingClient:       params.MessagingClient,
		dispatcherProvider:    params.DispatcherProvider,
		dynamicCollection:     dynamicconfig.NewCollection(params.DynamicConfig, params.Logger),
		archivalMetadata:      params.ArchivalMetadata,
		archiverProvider:      params.ArchiverProvider,
		serializer:            persistence.NewPayloadSerializer(),
	}

	sVice.runtimeMetricsReporter = metrics.NewRuntimeMetricsReporter(params.MetricScope, time.Minute, sVice.GetLogger(), params.InstanceID)
	sVice.dispatcher = sVice.rpcFactory.GetDispatcher()
	if sVice.dispatcher == nil {
		sVice.logger.Fatal("Unable to create yarpc dispatcher")
	}

	// Get the host name and set it on the service.  This is used for emitting metric with a tag for hostname
	if hostName, err := os.Hostname(); err != nil {
		sVice.logger.WithTags(tag.Error(err)).Fatal("Error getting hostname")
	} else {
		sVice.hostName = hostName
	}
	return sVice
}

// UpdateLoggerWithServiceName tag logging with service name from the top level
func (params *BootstrapParams) UpdateLoggerWithServiceName(name string) {
	params.Logger = params.Logger.WithTags(tag.Service(name))
	params.ThrottledLogger = params.ThrottledLogger.WithTags(tag.Service(name))
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
		h.logger.WithTags(tag.Error(err)).Fatal("Failed to start pprof")
	}

	if err := h.dispatcher.Start(); err != nil {
		h.logger.WithTags(tag.Error(err)).Fatal("Failed to start yarpc dispatcher")
	}

	h.membershipMonitor, err = h.membershipFactory.GetMembershipMonitor()
	if err != nil {
		h.logger.WithTags(tag.Error(err)).Fatal("Membership monitor creation failed")
	}

	h.membershipMonitor.Start()

	hostInfo, err := h.membershipMonitor.WhoAmI()
	if err != nil {
		h.logger.WithTags(tag.Error(err)).Fatal("failed to get host info from membership monitor")
	}
	h.hostInfo = hostInfo

	h.clientBean, err = client.NewClientBean(
		client.NewRPCClientFactory(h.rpcFactory, h.membershipMonitor, h.metricsClient, h.dynamicCollection, h.numberOfHistoryShards, h.logger),
		h.dispatcherProvider,
		h.clusterMetadata,
	)
	if err != nil {
		h.logger.WithTags(tag.Error(err)).Fatal("fail to initialize client bean")
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

	if h.dispatcher != nil {
		h.dispatcher.Stop() //nolint:errcheck
	}

	h.runtimeMetricsReporter.Stop()
}

func (h *serviceImpl) GetLogger() log.Logger {
	return h.logger
}

func (h *serviceImpl) GetThrottledLogger() log.Logger {
	return h.throttledLogger
}

func (h *serviceImpl) GetMetricsClient() metrics.Client {
	return h.metricsClient
}

func (h *serviceImpl) GetClientBean() client.Bean {
	return h.clientBean
}

func (h *serviceImpl) GetTimeSource() clock.TimeSource {
	return h.timeSource
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

func (h *serviceImpl) GetArchivalMetadata() archiver.ArchivalMetadata {
	return h.archivalMetadata
}

func (h *serviceImpl) GetArchiverProvider() provider.ArchiverProvider {
	return h.archiverProvider
}

func (h *serviceImpl) GetPayloadSerializer() persistence.PayloadSerializer {
	return h.serializer
}

// GetMetricsServiceIdx returns the metrics name
func GetMetricsServiceIdx(serviceName string, logger log.Logger) metrics.ServiceIdx {
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
		logger.Fatal("Unknown service name '%v' for metrics!", tag.Service(serviceName))
	}

	// this should never happen!
	return metrics.NumServices
}
