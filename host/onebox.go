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

package host

import (
	"flag"
	"fmt"
	"reflect"
	"sync"
	"time"

	"errors"

	"github.com/stretchr/testify/mock"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	fecli "github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/frontend"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
	"github.com/uber/cadence/service/worker"
	ringpop "github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	tcg "github.com/uber/tchannel-go"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

const rpAppNamePrefix string = "cadence"
const maxRpJoinTimeout = 30 * time.Second

var (
	integration = flag.Bool("integration", true, "run integration tests")
	topicName   = []string{"active", "standby"}
)

const (
	testNumberOfHistoryShards = 4
	testNumberOfHistoryHosts  = 1
)

// Cadence hosts all of cadence services in one process
type Cadence interface {
	Start() error
	Stop()
	GetFrontendClient() workflowserviceclient.Interface
	FrontendAddress() string
	GetFrontendService() service.Service
}

type (
	cadenceImpl struct {
		frontendHandler       *frontend.WorkflowHandler
		matchingHandler       *matching.Handler
		historyHandlers       []*history.Handler
		numberOfHistoryShards int
		numberOfHistoryHosts  int
		logger                bark.Logger
		clusterMetadata       cluster.Metadata
		messagingClient       messaging.Client
		metadataMgr           persistence.MetadataManager
		metadataMgrV2         persistence.MetadataManager
		shardMgr              persistence.ShardManager
		historyMgr            persistence.HistoryManager
		taskMgr               persistence.TaskManager
		visibilityMgr         persistence.VisibilityManager
		executionMgrFactory   persistence.ExecutionManagerFactory
		shutdownCh            chan struct{}
		shutdownWG            sync.WaitGroup
		frontEndService       service.Service
		clusterNo             int // cluster number
		replicator            *worker.Replicator
		enableWorkerService   bool // tmp flag used to tell if onbox should create worker service
	}

	ringpopFactoryImpl struct {
		rpHosts []string
	}
)

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(clusterMetadata cluster.Metadata, messagingClient messaging.Client, metadataMgr persistence.MetadataManager,
	metadataMgrV2 persistence.MetadataManager, shardMgr persistence.ShardManager, historyMgr persistence.HistoryManager,
	executionMgrFactory persistence.ExecutionManagerFactory, taskMgr persistence.TaskManager,
	visibilityMgr persistence.VisibilityManager, numberOfHistoryShards, numberOfHistoryHosts int,
	logger bark.Logger, clusterNo int, enableWorker bool) Cadence {

	return &cadenceImpl{
		numberOfHistoryShards: numberOfHistoryShards,
		numberOfHistoryHosts:  numberOfHistoryHosts,
		logger:                logger,
		clusterMetadata:       clusterMetadata,
		messagingClient:       messagingClient,
		metadataMgr:           metadataMgr,
		metadataMgrV2:         metadataMgrV2,
		visibilityMgr:         visibilityMgr,
		shardMgr:              shardMgr,
		historyMgr:            historyMgr,
		taskMgr:               taskMgr,
		executionMgrFactory:   executionMgrFactory,
		shutdownCh:            make(chan struct{}),
		clusterNo:             clusterNo,
		enableWorkerService:   enableWorker,
	}
}

func (c *cadenceImpl) enableWorker() bool {
	return c.enableWorkerService
}

func (c *cadenceImpl) Start() error {
	var rpHosts []string
	rpHosts = append(rpHosts, c.FrontendAddress())
	rpHosts = append(rpHosts, c.MatchingServiceAddress())
	rpHosts = append(rpHosts, c.HistoryServiceAddress()...)
	if c.enableWorker() {
		rpHosts = append(rpHosts, c.WorkerServiceAddress())
	}

	var startWG sync.WaitGroup
	startWG.Add(2)
	go c.startHistory(rpHosts, &startWG)
	go c.startMatching(rpHosts, &startWG)
	startWG.Wait()

	if c.enableWorker() {
		startWG.Add(1)
		go c.startWorker(rpHosts, &startWG)
		startWG.Wait()
	}

	startWG.Add(1)
	go c.startFrontend(rpHosts, &startWG)
	startWG.Wait()
	return nil
}

func (c *cadenceImpl) Stop() {
	if c.enableWorker() {
		c.shutdownWG.Add(4)
	} else {
		c.shutdownWG.Add(3)
	}
	c.frontendHandler.Stop()
	for _, historyHandler := range c.historyHandlers {
		historyHandler.Stop()
	}
	c.matchingHandler.Stop()
	if c.enableWorker() {
		c.replicator.Stop()
	}
	close(c.shutdownCh)
	c.shutdownWG.Wait()
}

func (c *cadenceImpl) FrontendAddress() string {
	if c.clusterNo != 0 {
		return "127.0.0.1:8104"
	}
	return "127.0.0.1:7104"
}

func (c *cadenceImpl) FrontendPProfPort() int {
	if c.clusterNo != 0 {
		return 8105
	}
	return 7105
}

func (c *cadenceImpl) HistoryServiceAddress() []string {
	hosts := []string{}
	startPort := 7200
	if c.clusterNo != 0 {
		startPort = 8200
	}
	for i := 0; i < c.numberOfHistoryHosts; i++ {
		port := startPort + i
		hosts = append(hosts, fmt.Sprintf("127.0.0.1:%v", port))
	}

	c.logger.Infof("History hosts: %v", hosts)
	return hosts
}

func (c *cadenceImpl) HistoryPProfPort() []int {
	ports := []int{}
	startPort := 7300
	if c.clusterNo != 0 {
		startPort = 8300
	}
	for i := 0; i < c.numberOfHistoryHosts; i++ {
		port := startPort + i
		ports = append(ports, port)
	}

	c.logger.Infof("History pprof ports: %v", ports)
	return ports
}

func (c *cadenceImpl) MatchingServiceAddress() string {
	if c.clusterNo != 0 {
		return "127.0.0.1:8106"
	}
	return "127.0.0.1:7106"
}

func (c *cadenceImpl) MatchingPProfPort() int {
	if c.clusterNo != 0 {
		return 8107
	}
	return 7107
}

func (c *cadenceImpl) WorkerServiceAddress() string {
	if c.clusterNo != 0 {
		return "127.0.0.1:8108"
	}
	return "127.0.0.1:7108"
}

func (c *cadenceImpl) WorkerPProfPort() int {
	if c.clusterNo != 0 {
		return 8109
	}
	return 7109
}

func (c *cadenceImpl) GetFrontendClient() workflowserviceclient.Interface {
	return fecli.New(c.frontEndService.GetDispatcher())
}

// For integration tests to get hold of FE instance.
func (c *cadenceImpl) GetFrontendService() service.Service {
	return c.frontEndService
}

func (c *cadenceImpl) startFrontend(rpHosts []string, startWG *sync.WaitGroup) {
	params := new(service.BootstrapParams)
	params.Name = common.FrontendServiceName
	params.Logger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.FrontendPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.FrontendServiceName, c.FrontendAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.RingpopFactory = newRingpopFactory(rpHosts)
	params.ClusterMetadata = c.clusterMetadata
	params.MessagingClient = c.messagingClient
	params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
	params.CassandraConfig.Hosts = "127.0.0.1"
	params.DynamicConfig = dynamicconfig.NewNopClient()

	// TODO when cross DC is public, remove this temporary override
	var kafkaProducer messaging.Producer
	var err error
	if c.enableWorker() {
		kafkaProducer, err = c.messagingClient.NewProducer(topicName[c.clusterNo])
		if err != nil {
			c.logger.WithField("error", err).Fatal("Failed to create kafka producer when start frontend")
		}
	} else {
		kafkaProducer = &mocks.KafkaProducer{}
		kafkaProducer.(*mocks.KafkaProducer).On("Publish", mock.Anything).Return(nil)
	}

	c.frontEndService = service.New(params)
	c.frontendHandler = frontend.NewWorkflowHandler(
		c.frontEndService, frontend.NewConfig(dynamicconfig.NewNopCollection()), c.metadataMgr, c.historyMgr, c.visibilityMgr, kafkaProducer)
	err = c.frontendHandler.Start()
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to start frontend")
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startHistory(rpHosts []string, startWG *sync.WaitGroup) {

	pprofPorts := c.HistoryPProfPort()
	for i, hostport := range c.HistoryServiceAddress() {
		params := new(service.BootstrapParams)
		params.Name = common.HistoryServiceName
		params.Logger = c.logger
		params.PProfInitializer = newPProfInitializerImpl(c.logger, pprofPorts[i])
		params.RPCFactory = newRPCFactoryImpl(common.HistoryServiceName, hostport, c.logger)
		params.MetricScope = tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.RingpopFactory = newRingpopFactory(rpHosts)
		params.ClusterMetadata = c.clusterMetadata
		params.MessagingClient = c.messagingClient
		params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
		service := service.New(params)
		historyConfig := history.NewConfig(dynamicconfig.NewNopCollection(), c.numberOfHistoryShards)
		historyConfig.HistoryMgrNumConns = dynamicconfig.GetIntPropertyFn(c.numberOfHistoryShards)
		historyConfig.ExecutionMgrNumConns = dynamicconfig.GetIntPropertyFn(c.numberOfHistoryShards)
		handler := history.NewHandler(service, historyConfig, c.shardMgr, c.metadataMgr,
			c.visibilityMgr, c.historyMgr, c.executionMgrFactory)
		handler.Start()
		c.historyHandlers = append(c.historyHandlers, handler)
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startMatching(rpHosts []string, startWG *sync.WaitGroup) {

	params := new(service.BootstrapParams)
	params.Name = common.MatchingServiceName
	params.Logger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.MatchingPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.MatchingServiceName, c.MatchingServiceAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.RingpopFactory = newRingpopFactory(rpHosts)
	params.ClusterMetadata = c.clusterMetadata
	params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
	service := service.New(params)
	c.matchingHandler = matching.NewHandler(
		service, matching.NewConfig(dynamicconfig.NewNopCollection()), c.taskMgr, c.metadataMgr,
	)
	c.matchingHandler.Start()
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startWorker(rpHosts []string, startWG *sync.WaitGroup) {
	params := new(service.BootstrapParams)
	params.Name = common.WorkerServiceName
	params.Logger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.WorkerPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.WorkerServiceName, c.WorkerServiceAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.WorkerServiceName, make(map[string]string))
	params.RingpopFactory = newRingpopFactory(rpHosts)
	params.ClusterMetadata = c.clusterMetadata
	params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
	service := service.New(params)
	service.Start()

	historyClient, err := service.GetClientFactory().NewHistoryClient()
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to create history service client when start worker")
	}
	metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgrV2, service.GetMetricsClient(), c.logger)

	workerConfig := worker.NewConfig(dynamicconfig.NewNopCollection())
	workerConfig.ReplicatorConcurrency = 10
	c.replicator = worker.NewReplicator(c.clusterMetadata, metadataManager, historyClient,
		workerConfig, c.messagingClient, c.logger, service.GetMetricsClient())
	if err := c.replicator.Start(); err != nil {
		c.replicator.Stop()
		c.logger.WithField("error", err).Fatal("Fail to start replicator when start worker")
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func newRingpopFactory(rpHosts []string) service.RingpopFactory {
	return &ringpopFactoryImpl{
		rpHosts: rpHosts,
	}
}

func (p *ringpopFactoryImpl) CreateRingpop(dispatcher *yarpc.Dispatcher) (*ringpop.Ringpop, error) {
	var ch *tcg.Channel
	var err error
	if ch, err = p.getChannel(dispatcher); err != nil {
		return nil, err
	}

	rp, err := ringpop.New(fmt.Sprintf("%s", rpAppNamePrefix), ringpop.Channel(ch))
	if err != nil {
		return nil, err
	}
	err = p.bootstrapRingpop(rp, p.rpHosts)
	if err != nil {
		return nil, err
	}
	return rp, nil
}

func (p *ringpopFactoryImpl) getChannel(dispatcher *yarpc.Dispatcher) (*tcg.Channel, error) {
	t := dispatcher.Inbounds()[0].Transports()[0].(*tchannel.ChannelTransport)
	ty := reflect.ValueOf(t.Channel())
	var ch *tcg.Channel
	var ok bool
	if ch, ok = ty.Interface().(*tcg.Channel); !ok {
		return nil, errors.New("Unable to get tchannel out of the dispatcher")
	}
	return ch, nil
}

// bootstrapRingpop tries to bootstrap the given ringpop instance using the hosts list
func (p *ringpopFactoryImpl) bootstrapRingpop(rp *ringpop.Ringpop, rpHosts []string) error {
	// TODO: log ring hosts

	bOptions := new(swim.BootstrapOptions)
	bOptions.DiscoverProvider = statichosts.New(rpHosts...)
	bOptions.MaxJoinDuration = maxRpJoinTimeout
	bOptions.JoinSize = 1 // this ensures the first guy comes up quickly

	_, err := rp.Bootstrap(bOptions)
	return err
}

type rpcFactoryImpl struct {
	ch          *tchannel.ChannelTransport
	serviceName string
	hostPort    string
	logger      bark.Logger
}

func newPProfInitializerImpl(logger bark.Logger, port int) common.PProfInitializer {
	return &config.PProfInitializerImpl{
		PProf: &config.PProf{
			Port: port,
		},
		Logger: logger,
	}
}

func newRPCFactoryImpl(sName string, hostPort string, logger bark.Logger) common.RPCFactory {
	return &rpcFactoryImpl{
		serviceName: sName,
		hostPort:    hostPort,
		logger:      logger,
	}
}

func (c *rpcFactoryImpl) CreateDispatcher() *yarpc.Dispatcher {
	// Setup dispatcher for onebox
	var err error
	c.ch, err = tchannel.NewChannelTransport(
		tchannel.ServiceName(c.serviceName), tchannel.ListenAddr(c.hostPort))
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to create transport channel")
	}
	return yarpc.NewDispatcher(yarpc.Config{
		Name:     c.serviceName,
		Inbounds: yarpc.Inbounds{c.ch.NewInbound()},
		// For integration tests to generate client out of the same outbound.
		Outbounds: yarpc.Outbounds{
			c.serviceName: {Unary: c.ch.NewSingleOutbound(c.hostPort)},
		},
	})
}

func (c *rpcFactoryImpl) CreateDispatcherForOutbound(
	callerName, serviceName, hostName string) *yarpc.Dispatcher {
	// Setup dispatcher(outbound) for onebox
	d := yarpc.NewDispatcher(yarpc.Config{
		Name: callerName,
		Outbounds: yarpc.Outbounds{
			serviceName: {Unary: c.ch.NewSingleOutbound(hostName)},
		},
	})
	if err := d.Start(); err != nil {
		c.logger.WithField("error", err).Fatal("Failed to create outbound transport channel")
	}
	return d
}
