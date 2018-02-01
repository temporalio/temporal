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
	"fmt"
	"reflect"
	"sync"
	"time"

	"errors"

	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	fecli "github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/service/frontend"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
	ringpop "github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	tcg "github.com/uber/tchannel-go"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

const rpAppNamePrefix string = "cadence"
const maxRpJoinTimeout = 30 * time.Second

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
		metadataMgr           persistence.MetadataManager
		shardMgr              persistence.ShardManager
		historyMgr            persistence.HistoryManager
		taskMgr               persistence.TaskManager
		visibilityMgr         persistence.VisibilityManager
		executionMgrFactory   persistence.ExecutionManagerFactory
		shutdownCh            chan struct{}
		shutdownWG            sync.WaitGroup
		frontEndService       service.Service
	}

	ringpopFactoryImpl struct {
		service string
		rpHosts []string
	}
)

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(clusterMetadata cluster.Metadata, metadataMgr persistence.MetadataManager, shardMgr persistence.ShardManager,
	historyMgr persistence.HistoryManager, executionMgrFactory persistence.ExecutionManagerFactory,
	taskMgr persistence.TaskManager, visibilityMgr persistence.VisibilityManager,
	numberOfHistoryShards, numberOfHistoryHosts int, logger bark.Logger) Cadence {
	return &cadenceImpl{
		numberOfHistoryShards: numberOfHistoryShards,
		numberOfHistoryHosts:  numberOfHistoryHosts,
		logger:                logger,
		clusterMetadata:       clusterMetadata,
		metadataMgr:           metadataMgr,
		visibilityMgr:         visibilityMgr,
		shardMgr:              shardMgr,
		historyMgr:            historyMgr,
		taskMgr:               taskMgr,
		executionMgrFactory:   executionMgrFactory,
		shutdownCh:            make(chan struct{}),
	}
}

func (c *cadenceImpl) Start() error {
	var rpHosts []string
	rpHosts = append(rpHosts, c.FrontendAddress())
	rpHosts = append(rpHosts, c.MatchingServiceAddress())
	rpHosts = append(rpHosts, c.HistoryServiceAddress()...)

	var startWG sync.WaitGroup
	startWG.Add(2)
	go c.startHistory(c.logger, c.shardMgr, c.metadataMgr, c.visibilityMgr, c.historyMgr, c.executionMgrFactory, rpHosts, &startWG)
	go c.startMatching(c.logger, c.taskMgr, rpHosts, &startWG)
	startWG.Wait()

	startWG.Add(1)
	go c.startFrontend(c.logger, rpHosts, &startWG)
	startWG.Wait()
	return nil
}

func (c *cadenceImpl) Stop() {
	c.shutdownWG.Add(3)
	c.frontendHandler.Stop()
	for _, historyHandler := range c.historyHandlers {
		historyHandler.Stop()
	}
	c.matchingHandler.Stop()
	close(c.shutdownCh)
	c.shutdownWG.Wait()
}

func (c *cadenceImpl) FrontendAddress() string {
	return "127.0.0.1:7104"
}

func (c *cadenceImpl) FrontendPProfPort() int {
	return 7105
}

func (c *cadenceImpl) HistoryServiceAddress() []string {
	hosts := []string{}
	startPort := 7200
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
	for i := 0; i < c.numberOfHistoryHosts; i++ {
		port := startPort + i
		ports = append(ports, port)
	}

	c.logger.Infof("History pprof ports: %v", ports)
	return ports
}

func (c *cadenceImpl) MatchingServiceAddress() string {
	return "127.0.0.1:7106"
}

func (c *cadenceImpl) MatchingPProfPort() int {
	return 7107
}

func (c *cadenceImpl) GetFrontendClient() workflowserviceclient.Interface {
	return fecli.New(c.frontEndService.GetDispatcher())
}

// For integration tests to get hold of FE instance.
func (c *cadenceImpl) GetFrontendService() service.Service {
	return c.frontEndService
}

func (c *cadenceImpl) startFrontend(logger bark.Logger, rpHosts []string, startWG *sync.WaitGroup) {
	params := new(service.BootstrapParams)
	params.Name = common.FrontendServiceName
	params.Logger = logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.FrontendPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.FrontendServiceName, c.FrontendAddress(), logger)
	params.MetricScope = tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.RingpopFactory = newRingpopFactory(common.FrontendServiceName, rpHosts)
	params.ClusterMetadata = c.clusterMetadata
	params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
	params.CassandraConfig.Hosts = "127.0.0.1"

	c.frontEndService = service.New(params)
	c.frontendHandler = frontend.NewWorkflowHandler(
		c.frontEndService, frontend.NewConfig(), c.metadataMgr, c.historyMgr, c.visibilityMgr)
	err := c.frontendHandler.Start()
	if err != nil {
		c.logger.WithField("error", err).Fatal("Failed to start frontend")
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startHistory(logger bark.Logger, shardMgr persistence.ShardManager,
	metadataMgr persistence.MetadataManager, visibilityMgr persistence.VisibilityManager, historyMgr persistence.HistoryManager,
	executionMgrFactory persistence.ExecutionManagerFactory, rpHosts []string, startWG *sync.WaitGroup) {

	pprofPorts := c.HistoryPProfPort()
	for i, hostport := range c.HistoryServiceAddress() {
		params := new(service.BootstrapParams)
		params.Name = common.HistoryServiceName
		params.Logger = logger
		params.PProfInitializer = newPProfInitializerImpl(c.logger, pprofPorts[i])
		params.RPCFactory = newRPCFactoryImpl(common.HistoryServiceName, hostport, logger)
		params.MetricScope = tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.RingpopFactory = newRingpopFactory(common.FrontendServiceName, rpHosts)
		params.ClusterMetadata = c.clusterMetadata
		params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
		service := service.New(params)
		historyConfig := history.NewConfig(c.numberOfHistoryShards)
		historyConfig.HistoryMgrNumConns = c.numberOfHistoryShards
		historyConfig.ExecutionMgrNumConns = c.numberOfHistoryShards
		handler := history.NewHandler(service, historyConfig, shardMgr, metadataMgr,
			visibilityMgr, historyMgr, executionMgrFactory)
		handler.Start()
		c.historyHandlers = append(c.historyHandlers, handler)
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startMatching(logger bark.Logger, taskMgr persistence.TaskManager,
	rpHosts []string, startWG *sync.WaitGroup) {

	params := new(service.BootstrapParams)
	params.Name = common.MatchingServiceName
	params.Logger = logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.MatchingPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.MatchingServiceName, c.MatchingServiceAddress(), logger)
	params.MetricScope = tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.RingpopFactory = newRingpopFactory(common.FrontendServiceName, rpHosts)
	params.ClusterMetadata = c.clusterMetadata
	params.CassandraConfig.NumHistoryShards = c.numberOfHistoryShards
	service := service.New(params)
	c.matchingHandler = matching.NewHandler(service, matching.NewConfig(), taskMgr)
	c.matchingHandler.Start()
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func newRingpopFactory(service string, rpHosts []string) service.RingpopFactory {
	return &ringpopFactoryImpl{
		service: service,
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
